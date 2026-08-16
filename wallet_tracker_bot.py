"""
Telegram Cuzdan Takip Botu
- 2x BTC cuzdani (mempool + confirmed)
- 1x Ethereum mainnet USDT (ERC-20) - 30sn polling
- 1x Solana USDT / SPL - 30sn polling (RPC ile confirmed islemler)
- Inline butonlar
- Gunluk ozet raporu
- /komutlar destegi

RAM OPTIMIZASYONLARI (islev kaybi olmadan):
  1) Tum HTTP istekleri icin tek, paylasimli aiohttp.ClientSession kullanilir.
     Eskiden her kontrol donguusnde / her komutta yeni bir session acilip
     kapatiliyordu (yeni connector havuzu = fazladan bellek + GC yuku).
  2) daily_txs artik ham (raw) API cevabini degil, sadece rapor icin
     gereken minimal alanlari (miktar, yon, tip) saklar. BTC/EVM/Solana
     ham JSON'lari (vin/vout, gas bilgisi, log index, pre/postTokenBalances vs.)
     gun icinde onlarca islem biriktiginde gereksiz yere bellek tuketiyordu.

EVM (ETH / POLYGON) NOTU:
  Ethereum ve Polygon USDT takibi ayni Etherscan v2 API'si uzerinden
  yapilir (https://api.etherscan.io/v2/api), sadece "chainid" parametresi
  degisir (Ethereum = 1, Polygon = 137). Ayni API anahtari her iki zincir
  icin de gecerlidir.

SOLANA NOTU:
  Varsayilan olarak public Solana RPC (api.mainnet-beta.solana.com) kullanilir.
  Bu endpoint sik sik rate-limit (429) doner. Kararli calisma icin ucretsiz/
  ucretli ozel bir RPC saglayicisi (Helius, QuickNode, Alchemy vb.) onerilir;
  SOLANA_RPC_URL ortam degiskeniyle degistirilebilir.
"""

import asyncio
import logging
import os
import json
import re
import time
import aiohttp
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot, Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

# ──────────────────────────────────────────────────────
# AYARLAR
# ──────────────────────────────────────────────────────
# NOT: Bu degerler ortam degiskeni (env var) olarak verilmezse kod icindeki
# varsayilanlar kullanilir. Bu anahtarlari bir yerde paylastiysaniz
# (ornegin bu sohbette) guvenlik icin yenilemeniz (rotate) onerilir.
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "8649558470:AAHCRXTKxCiVi2MaAp88trJVe7McE8v7j9k")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "492272237")

# Takip edilen cuzdanlar. BTC, EVM USDT (Ethereum/Polygon) ve Solana USDT (SPL)
# karisik olarak eklenebilir; her girdi kendi "network" alanina gore islenir.
WALLETS = {
    "bc1q4uxrzj5kday3xtfz28ju6k4re8npa8jeaddmug": {
        "address": "bc1q4uxrzj5kday3xtfz28ju6k4re8npa8jeaddmug",
        "network": "btc",
        "symbol":  "BTC",
    },
    "bc1qyz28dm0vxt6jj7lu9f837570az4ewutzdrcq3q": {
        "address": "bc1qyz28dm0vxt6jj7lu9f837570az4ewutzdrcq3q",
        "network": "btc",
        "symbol":  "BTC",
    },
    "Solana Cuzdan": {
        "address": "6ZusgXdQDNvRiqzqJ1mj7xsRCcAnLGzNgyB7weWVUb2F",
        "network": "solana",
        "symbol":  "USDT",
    },
    "ETH Cuzdan": {
        "address": "0xeE261990aaFFbe4d018B7ED71655b2A6B56C6770",
        "network": "eth",
        "symbol":  "USDT",
    },
}

# EVM (Ethereum / Polygon) USDT kontrat adresleri
USDT_CONTRACT_ETH      = "0xdAC17F958D2ee523a2206206994597C13D831ec7"   # Ethereum mainnet USDT
USDT_CONTRACT_POLYGON  = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"   # Polygon USDT (referans - su an takip edilmiyor)
ETHERSCAN_API_KEY      = os.getenv("ETHERSCAN_API_KEY", os.getenv("POLYGONSCAN_API_KEY", "RGSD69N6JG2KM9IIMJME2G8W8Y9N6FX6JY"))

# network adi -> (chainid, kontrat adresi, explorer adi, explorer tx URL taban)
EVM_NETWORKS = {
    "eth": {
        "chainid":       1,
        "contract":      USDT_CONTRACT_ETH,
        "explorer_name": "Etherscan",
        "explorer_url":  "https://etherscan.io/tx/",
    },
    "polygon": {
        "chainid":       137,
        "contract":      USDT_CONTRACT_POLYGON,
        "explorer_name": "Polygonscan",
        "explorer_url":  "https://polygonscan.com/tx/",
    },
}

# Solana USDT (SPL Token) mint adresi
USDT_SOLANA_MINT   = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
SOLANA_RPC_URL     = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
SOLANA_TX_LIMIT    = 15   # her kontrol donguusnde token hesabi icin cekilecek son islem sayisi

DAILY_REPORT_HOUR      = 20   # UTC
DAILY_REPORT_MINUTE    = 0
CHECK_INTERVAL_SECONDS = 30

# ── nonlogs.io/reserves takibi ──
# Bu sayfa icin resmi bir JSON API yok (nonlogs.io/api sadece markets/orderbook/order
# endpointlerini sunuyor), o yuzden sayfa HTML'i cekilip parse edilir. Sayfaya asiri
# yuk bindirmemek icin ayri ve daha seyrek bir kontrol araligi kullanilir.
NONLOGS_RESERVES_URL          = "https://nonlogs.io/reserves"
NONLOGS_CHECK_INTERVAL_SECONDS = 60
NONLOGS_STATE_FILE            = "nonlogs_reserves.json"
# Takip edilen varliklar: coin -> (sayfadaki baslik metni, sembol satiri, ondalik hassasiyet)
NONLOGS_TRACKED_ASSETS = {
    "BTC":  {"heading": "Bitcoin",    "symbol_line": "BTC BTC",    "unit": "BTC",  "decimals": 8},
    "GRIN": {"heading": "Grin",       "symbol_line": "GRIN Grin",  "unit": "GRIN", "decimals": 8},
    "USDT": {"heading": "Tether USD", "symbol_line": "USDT ETH",   "unit": "USDT", "decimals": 6},
}

BOT_START_TIME = time.time()

# ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

STATE_FILE   = "seen_txs.json"
PENDING_FILE = "pending_txs.json"

def load_json(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

seen_txs    = load_json(STATE_FILE)
pending_txs = load_json(PENDING_FILE)
# Artik ham tx degil, minimal ozet dict tutuluyor: {"type":..,"amount":..,"is_in":..}
daily_txs   = {name: [] for name in WALLETS}

# nonlogs.io/reserves icin son bilinen bakiyeler: {"BTC": 0.63005365, "GRIN": ..., "USDT": ...}
nonlogs_reserves = load_json(NONLOGS_STATE_FILE)

# Solana icin: sahibin (owner) USDT associated token account (ATA) adresini
# her seferinde RPC'den sormamak icin basit bir cache. {owner_address: token_account | None}
solana_token_account_cache: dict[str, str | None] = {}

# Tum HTTP cagrilari icin tek paylasimli session (main() icinde olusturulur)
HTTP_SESSION: aiohttp.ClientSession | None = None

# ──────────────────────────────────────────────────────
# YARDIMCI
# ──────────────────────────────────────────────────────
def e(text):
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def ts_to_str(ts):
    if not ts or int(ts) == 0:
        return "Bilinmiyor"
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc) + timedelta(hours=3)
    return dt.strftime("%d.%m.%Y %H:%M (TR)")

def now_str():
    return (datetime.now(tz=timezone.utc) + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M (TR)")

# ──────────────────────────────────────────────────────
# INLINE KLAVYELER
# ──────────────────────────────────────────────────────
def btc_tx_keyboard(txid):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔍 Blockstream", url=f"https://blockstream.info/tx/{txid}"),
            InlineKeyboardButton("🌐 Mempool.space", url=f"https://mempool.space/tx/{txid}"),
        ],
        [
            InlineKeyboardButton("💼 Bakiyeler", callback_data="bakiye"),
            InlineKeyboardButton("📊 Rapor", callback_data="rapor"),
        ],
    ])

def evm_tx_keyboard(txhash, network):
    cfg = EVM_NETWORKS[network]
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"🔍 {cfg['explorer_name']}'da Gör", url=f"{cfg['explorer_url']}{txhash}"),
        ],
        [
            InlineKeyboardButton("💼 Bakiyeler", callback_data="bakiye"),
            InlineKeyboardButton("📊 Rapor", callback_data="rapor"),
        ],
    ])

def solana_tx_keyboard(signature):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔍 Solscan'da Gör", url=f"https://solscan.io/tx/{signature}"),
            InlineKeyboardButton("🌐 Solana FM", url=f"https://solana.fm/tx/{signature}"),
        ],
        [
            InlineKeyboardButton("💼 Bakiyeler", callback_data="bakiye"),
            InlineKeyboardButton("📊 Rapor", callback_data="rapor"),
        ],
    ])

def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💼 Bakiyeler", callback_data="bakiye"),
            InlineKeyboardButton("📊 Rapor", callback_data="rapor"),
        ],
        [
            InlineKeyboardButton("🔎 Son İşlemler", callback_data="sonislem"),
            InlineKeyboardButton("⏳ Bekleyenler", callback_data="bekleyenler"),
        ],
        [
            InlineKeyboardButton("🖥️ Sistem Kontrol", callback_data="sistemkontrol"),
        ],
    ])

# ──────────────────────────────────────────────────────
# API  (hepsi artik parametre olarak gelen paylasimli session'i kullanir)
# ──────────────────────────────────────────────────────
async def fetch_btc_txs(address, session):
    try:
        async with session.get(
            f"https://blockstream.info/api/address/{address}/txs",
            timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            if r.status == 200:
                return await r.json()
    except Exception as ex:
        log.warning(f"BTC confirmed API hatasi: {ex}")
    return []

async def fetch_btc_mempool_txs(address, session):
    try:
        async with session.get(
            f"https://blockstream.info/api/address/{address}/txs/mempool",
            timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            if r.status == 200:
                return await r.json()
    except Exception as ex:
        log.warning(f"BTC mempool API hatasi: {ex}")
    return []

async def fetch_btc_address_info(address, session):
    try:
        async with session.get(
            f"https://blockstream.info/api/address/{address}",
            timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            if r.status == 200:
                return await r.json()
    except Exception as ex:
        log.warning(f"BTC address info hatasi: {ex}")
    return {}

# ── EVM (Ethereum / Polygon) - Etherscan v2 unified API ──
async def fetch_evm_confirmed(address, session, network, offset=10):
    cfg = EVM_NETWORKS[network]
    try:
        async with session.get(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": cfg["chainid"],
                "module": "account", "action": "tokentx",
                "contractaddress": cfg["contract"],
                "address": address,
                "sort": "desc", "page": 1, "offset": offset,
                "apikey": ETHERSCAN_API_KEY,
            },
            timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            data = await r.json()
            if data.get("status") == "1":
                return data.get("result", [])
    except Exception as ex:
        log.warning(f"EVM ({network}) confirmed API hatasi: {ex}")
    return []

async def fetch_evm_usdt_balance(address, session, network):
    cfg = EVM_NETWORKS[network]
    try:
        async with session.get(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": cfg["chainid"],
                "module": "account", "action": "tokenbalance",
                "contractaddress": cfg["contract"],
                "address": address,
                "tag": "latest",
                "apikey": ETHERSCAN_API_KEY,
            },
            timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            data = await r.json()
            if data.get("status") == "1":
                return int(data.get("result", 0)) / 1e6
    except Exception as ex:
        log.warning(f"EVM ({network}) balance API hatasi: {ex}")
    return 0.0

# ── SOLANA ──
async def _solana_rpc(method, params, session):
    """Solana JSON-RPC istegi atar, 'result' alanini dondurur (hata olursa None)."""
    try:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        async with session.post(
            SOLANA_RPC_URL, json=payload,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            data = await r.json()
            if "error" in data:
                log.warning(f"Solana RPC hatasi ({method}): {data['error']}")
                return None
            return data.get("result")
    except Exception as ex:
        log.warning(f"Solana RPC istek hatasi ({method}): {ex}")
        return None

async def fetch_solana_usdt_account(owner, session, use_cache=True):
    """
    Owner'in USDT (SPL) token hesabini bulur.
    Donus: (token_account_pubkey_or_None, ui_balance_float)
    """
    if use_cache and owner in solana_token_account_cache and solana_token_account_cache[owner]:
        token_account = solana_token_account_cache[owner]
        # Cache'de adres var, sadece guncel bakiyeyi cekmek icin getTokenAccountBalance kullan
        result = await _solana_rpc("getTokenAccountBalance", [token_account], session)
        if result:
            balance = float(result.get("value", {}).get("uiAmount") or 0)
            return token_account, balance
        # Cache gecersiz kaldiysa asagida yeniden arayacagiz

    result = await _solana_rpc(
        "getTokenAccountsByOwner",
        [owner, {"mint": USDT_SOLANA_MINT}, {"encoding": "jsonParsed"}],
        session,
    )
    if not result:
        return None, 0.0
    accounts = result.get("value", [])
    if not accounts:
        solana_token_account_cache[owner] = None
        return None, 0.0

    acc = accounts[0]
    token_account = acc.get("pubkey")
    try:
        amount_info = acc["account"]["data"]["parsed"]["info"]["tokenAmount"]
        balance = float(amount_info.get("uiAmount") or 0)
    except (KeyError, TypeError):
        balance = 0.0

    solana_token_account_cache[owner] = token_account
    return token_account, balance

async def fetch_solana_signatures(token_account, session, limit=SOLANA_TX_LIMIT):
    result = await _solana_rpc(
        "getSignaturesForAddress",
        [token_account, {"limit": limit}],
        session,
    )
    return result or []

async def fetch_solana_tx(signature, session):
    return await _solana_rpc(
        "getTransaction",
        [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
        session,
    )

def _solana_tx_delta(tx, owner):
    """
    Verilen tx'in meta.pre/postTokenBalances alanlarindan, 'owner' sahibinin
    USDT bakiyesindeki degisimi hesaplar.
    Donus: (amount, is_in) veya alakasiz/parse edilemezse None.
    """
    if not tx:
        return None
    meta = tx.get("meta") or {}
    pre  = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    pre_map  = {b["accountIndex"]: b for b in pre  if b.get("mint") == USDT_SOLANA_MINT}
    post_map = {b["accountIndex"]: b for b in post if b.get("mint") == USDT_SOLANA_MINT}

    # Hesap hala aciksa (postTokenBalances icinde var)
    for idx, pb in post_map.items():
        if pb.get("owner") != owner:
            continue
        pre_amt  = float((pre_map.get(idx, {}).get("uiTokenAmount") or {}).get("uiAmount") or 0)
        post_amt = float((pb.get("uiTokenAmount") or {}).get("uiAmount") or 0)
        delta = post_amt - pre_amt
        if delta != 0:
            return abs(delta), delta > 0

    # Hesap bu islemde kapatildiysa (sadece preTokenBalances icinde var)
    for idx, pb in pre_map.items():
        if pb.get("owner") != owner or idx in post_map:
            continue
        pre_amt = float((pb.get("uiTokenAmount") or {}).get("uiAmount") or 0)
        if pre_amt != 0:
            return pre_amt, False

    return None

# ── NONLOGS.IO/RESERVES ──
def _strip_html_to_text(html):
    """HTML'i duz metne cevirir (script/style haric), sayfa yapisi degisse bile
    metin akisi ayni kaldigi surece parse edilebilir olsun diye."""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    text = re.sub(r"\s+", " ", text)
    return text.strip()

async def fetch_nonlogs_reserves(session):
    """
    https://nonlogs.io/reserves sayfasindan BTC / GRIN / USDT (Tether) toplam
    kullanici bakiyelerini ceker. Sayfada resmi bir JSON API olmadigi icin HTML
    parse edilir. Sayfa yapisi degisirse ilgili varlik icin log uyarisi basar
    ve o varligi atlar (crash etmez).
    Donus: {"BTC": 0.63005365, "GRIN": 3162454.76677309, "USDT": 521.196633}
    """
    try:
        async with session.get(
            NONLOGS_RESERVES_URL,
            timeout=aiohttp.ClientTimeout(total=20),
            headers={"User-Agent": "Mozilla/5.0 (WalletTrackerBot)"},
        ) as r:
            if r.status != 200:
                log.warning(f"Nonlogs reserves HTTP {r.status}")
                return {}
            html = await r.text()
    except Exception as ex:
        log.warning(f"Nonlogs reserves fetch hatasi: {ex}")
        return {}

    text = _strip_html_to_text(html)
    result = {}
    for coin, cfg in NONLOGS_TRACKED_ASSETS.items():
        pattern = (
            re.escape(cfg["heading"]) + r"\s*" + re.escape(cfg["symbol_line"]) +
            r"\s*Total user balance\s*([\d,]+\.\d+)\s*" + re.escape(cfg["unit"])
        )
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            try:
                result[coin] = float(m.group(1).replace(",", ""))
            except ValueError:
                log.warning(f"Nonlogs reserves: {coin} degeri sayisal olarak parse edilemedi.")
        else:
            log.warning(f"Nonlogs reserves: {coin} degeri sayfada bulunamadi (site yapisi degismis olabilir).")
    return result

async def initialize_nonlogs_snapshot():
    """Bot baslarken nonlogs.io/reserves bakiyelerini baz deger olarak kaydeder,
    bildirim gondermeden (ilk deger referans noktasidir)."""
    global nonlogs_reserves
    current = await fetch_nonlogs_reserves(HTTP_SESSION)
    if not current:
        log.warning("Nonlogs reserves baslangic verisi alinamadi, ilk kontrolde tekrar denenecek.")
        return
    for coin, value in current.items():
        if coin not in nonlogs_reserves:
            nonlogs_reserves[coin] = value
    save_json(NONLOGS_STATE_FILE, nonlogs_reserves)
    log.info(f"Nonlogs reserves snapshot alindi: {nonlogs_reserves}")

async def check_nonlogs_reserves(bot: Bot):
    """nonlogs.io/reserves sayfasini yeniden ceker, onceki degerle karsilastirir,
    degisiklik varsa Telegram'a bildirim gonderir (ornek: '-0.12000000 BTC')."""
    global nonlogs_reserves
    current = await fetch_nonlogs_reserves(HTTP_SESSION)
    if not current:
        return

    changed = False
    for coin, value in current.items():
        cfg      = NONLOGS_TRACKED_ASSETS[coin]
        decimals = cfg["decimals"]
        prev     = nonlogs_reserves.get(coin)

        if prev is None:
            nonlogs_reserves[coin] = value
            changed = True
            continue

        delta = value - prev
        # Kayan nokta yuvarlama gurultusunu degil, gercek degisimi yakala
        if abs(delta) < (10 ** -decimals) / 2:
            continue

        icon = "📈" if delta > 0 else "📉"
        sign = "+" if delta > 0 else ""
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=(
                f"{icon} <b>Nonlogs Rezerv Degisikligi - {coin}</b>\n"
                "────────────────────────────\n"
                f"🔄 <b>Degisim:</b> <code>{sign}{delta:.{decimals}f} {coin}</code>\n"
                f"💰 <b>Onceki:</b> <code>{prev:.{decimals}f} {coin}</code>\n"
                f"💰 <b>Guncel:</b> <code>{value:.{decimals}f} {coin}</code>\n"
                f"🕐 <b>Zaman:</b> {e(now_str())}\n"
                f'🔗 <a href="{NONLOGS_RESERVES_URL}">Nonlogs Reserves</a>'
            ),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        nonlogs_reserves[coin] = value
        changed = True

    if changed:
        save_json(NONLOGS_STATE_FILE, nonlogs_reserves)

# ──────────────────────────────────────────────────────
# MESAJ FORMATLAMA
# ──────────────────────────────────────────────────────
def format_btc_tx(wallet_name, address, tx, is_pending=False):
    txid        = tx["txid"]
    vout        = tx.get("vout", [])
    status      = tx.get("status", {})
    is_incoming = any(o.get("scriptpubkey_address") == address for o in vout)
    dir_icon    = "📥" if is_incoming else "📤"
    direction   = "GIRIS" if is_incoming else "CIKIS"
    amount_sat  = sum(o.get("value", 0) for o in vout if o.get("scriptpubkey_address") == address)
    amount_str  = f"{amount_sat / 1e8:.8f} BTC" if amount_sat else "?"
    fee_sat     = tx.get("fee", 0)
    fee_str     = f"{fee_sat / 1e8:.8f} BTC" if fee_sat else "?"

    if is_pending:
        header     = "⚡ <b>Yeni BTC Islemi - PENDING!</b>"
        status_str = "⏳ PENDING (Mempool)"
        time_str   = now_str()
    else:
        header     = "🔔 <b>Yeni BTC Islemi!</b>"
        status_str = "Onaylandi ✅" if status.get("confirmed") else "Bekliyor ⏳"
        time_str   = ts_to_str(status.get("block_time"))

    return (
        f"{header}\n"
        f"👛 <b>Cuzdan:</b> {e(wallet_name)}\n"
        "────────────────────────────\n"
        f"{dir_icon} <b>Yon:</b> {direction}\n"
        f"💰 <b>Miktar:</b> <code>{e(amount_str)}</code>\n"
        f"⛽ <b>Ucret:</b> <code>{e(fee_str)}</code>\n"
        f"📋 <b>Durum:</b> {status_str}\n"
        f"🕐 <b>Zaman:</b> {e(time_str)}\n"
        f"🔑 <b>TX:</b> <code>{e(txid)}</code>"
    )

def format_evm_tx(wallet_name, address, tx, network, is_pending=False):
    net_label   = "Ethereum" if network == "eth" else "Polygon"
    txhash      = tx.get("hash", "")
    value       = int(tx.get("value", 0)) / 1e6
    from_addr   = tx.get("from", "")
    to_addr     = tx.get("to", "")
    is_incoming = to_addr.lower() == address.lower()
    dir_icon    = "📥" if is_incoming else "📤"
    direction   = "GIRIS" if is_incoming else "CIKIS"
    label       = "Gonderen" if is_incoming else "Alici"
    counterpart = from_addr if is_incoming else to_addr
    confs       = int(tx.get("confirmations", 0))
    gas_gwei    = int(tx.get("gasPrice", 0)) / 1e9

    if is_pending:
        header     = f"⚡ <b>Yeni {net_label} USDT Islemi - PENDING!</b>"
        status_str = "⏳ PENDING"
        time_str   = now_str()
    else:
        header     = f"🔔 <b>Yeni {net_label} USDT Islemi!</b>"
        status_str = f"Onaylandi ✅ ({confs} onay)"
        time_str   = ts_to_str(tx.get("timeStamp"))

    return (
        f"{header}\n"
        f"👛 <b>Cuzdan:</b> {e(wallet_name)}\n"
        "────────────────────────────\n"
        f"{dir_icon} <b>Yon:</b> {direction}\n"
        f"💰 <b>Miktar:</b> <code>{value:.2f} USDT</code>\n"
        f"👤 <b>{label}:</b> <code>{e(counterpart)}</code>\n"
        f"⛽ <b>Gas:</b> <code>{gas_gwei:.1f} Gwei</code>\n"
        f"📋 <b>Durum:</b> {status_str}\n"
        f"🕐 <b>Zaman:</b> {e(time_str)}\n"
        f"🔑 <b>TX:</b> <code>{e(txhash)}</code>"
    )

def format_solana_tx(wallet_name, signature, amount, is_in, block_time):
    dir_icon  = "📥" if is_in else "📤"
    direction = "GIRIS" if is_in else "CIKIS"
    return (
        "🔔 <b>Yeni Solana USDT Islemi!</b>\n"
        f"👛 <b>Cuzdan:</b> {e(wallet_name)}\n"
        "────────────────────────────\n"
        f"{dir_icon} <b>Yon:</b> {direction}\n"
        f"💰 <b>Miktar:</b> <code>{amount:.6f} USDT</code>\n"
        f"📋 <b>Durum:</b> Onaylandi ✅\n"
        f"🕐 <b>Zaman:</b> {e(ts_to_str(block_time))}\n"
        f"🔑 <b>TX:</b> <code>{e(signature[:20])}...</code>"
    )

def format_confirmed_update(wallet_name, txid, extra=""):
    return (
        f"✅ <b>Islem Onaylandi!</b>\n"
        f"👛 <b>Cuzdan:</b> {e(wallet_name)}\n"
        f"🔑 <b>TX:</b> <code>{e(txid)}</code>\n"
        f"{extra}"
    )

# ──────────────────────────────────────────────────────
# SNAPSHOT
# ──────────────────────────────────────────────────────
async def initialize_snapshots():
    global seen_txs
    log.info("Snapshot aliniyor...")
    for name, cfg in WALLETS.items():
        address = cfg["address"]
        ids = []
        if cfg["network"] == "btc":
            txs     = await fetch_btc_txs(address, HTTP_SESSION)
            mempool = await fetch_btc_mempool_txs(address, HTTP_SESSION)
            ids     = [tx["txid"] for tx in txs[:20]] + [tx["txid"] for tx in mempool]
        elif cfg["network"] in EVM_NETWORKS:
            txs = await fetch_evm_confirmed(address, HTTP_SESSION, cfg["network"])
            ids = [tx["hash"] for tx in txs[:20]]
        elif cfg["network"] == "solana":
            token_account, _ = await fetch_solana_usdt_account(address, HTTP_SESSION, use_cache=False)
            if token_account:
                sigs = await fetch_solana_signatures(token_account, HTTP_SESSION, limit=25)
                ids  = [s["signature"] for s in sigs]

        if name not in seen_txs:
            seen_txs[name] = ids
        else:
            existing = set(seen_txs[name])
            seen_txs[name].extend(i for i in ids if i not in existing)
        log.info(f"  {name}: {len(ids)} eski islem isaretlendi")

    save_json(STATE_FILE, seen_txs)
    log.info("Snapshot tamamlandi.")

# ──────────────────────────────────────────────────────
# DAILY_TXS ICIN MINIMAL OZET CIKARIMI (RAM optimizasyonu)
# ──────────────────────────────────────────────────────
def _summarize_btc(tx, address):
    vout   = tx.get("vout", [])
    amount = sum(o.get("value", 0) for o in vout if o.get("scriptpubkey_address") == address) / 1e8
    is_in  = any(o.get("scriptpubkey_address") == address for o in vout)
    return {"type": "btc", "amount": amount, "is_in": is_in}

def _summarize_evm(tx, address, network):
    amount = int(tx.get("value", 0)) / 1e6
    is_in  = tx.get("to", "").lower() == address.lower()
    return {"type": f"{network}_usdt", "amount": amount, "is_in": is_in}

def _summarize_solana(amount, is_in):
    return {"type": "solana_usdt", "amount": amount, "is_in": is_in}

# ──────────────────────────────────────────────────────
# ANA KONTROL DÖNGÜSÜ
# ──────────────────────────────────────────────────────
async def check_wallets(bot: Bot):
    global seen_txs, pending_txs
    session = HTTP_SESSION
    for name, cfg in WALLETS.items():
        address = cfg["address"]
        network = cfg["network"]

        # ── BTC ──
        if network == "btc":
            # Mempool (pending)
            for tx in await fetch_btc_mempool_txs(address, session):
                txid = tx["txid"]
                if txid not in seen_txs.get(name, []):
                    await bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=format_btc_tx(name, address, tx, is_pending=True),
                        parse_mode=ParseMode.HTML,
                        reply_markup=btc_tx_keyboard(txid),
                        disable_web_page_preview=True,
                    )
                    seen_txs.setdefault(name, []).append(txid)
                    pending_txs[txid] = {"wallet": name, "type": "btc"}
                    daily_txs[name].append(_summarize_btc(tx, address))

            # Confirmed
            for tx in (await fetch_btc_txs(address, session))[:10]:
                txid = tx["txid"]
                if txid in pending_txs:
                    await bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=format_confirmed_update(name, txid),
                        parse_mode=ParseMode.HTML,
                        reply_markup=btc_tx_keyboard(txid),
                        disable_web_page_preview=True,
                    )
                    del pending_txs[txid]
                elif txid not in seen_txs.get(name, []):
                    await bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=format_btc_tx(name, address, tx, is_pending=False),
                        parse_mode=ParseMode.HTML,
                        reply_markup=btc_tx_keyboard(txid),
                        disable_web_page_preview=True,
                    )
                    seen_txs.setdefault(name, []).append(txid)
                    daily_txs[name].append(_summarize_btc(tx, address))

            seen_txs[name] = seen_txs.get(name, [])[-100:]

        # ── EVM (Ethereum / Polygon) ──
        elif network in EVM_NETWORKS:
            for tx in await fetch_evm_confirmed(address, session, network):
                txhash = tx.get("hash", "")
                if not txhash:
                    continue
                if txhash in pending_txs:
                    await bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=format_confirmed_update(name, txhash,
                            f"📋 <b>Onay:</b> {tx.get('confirmations','?')}\n"),
                        parse_mode=ParseMode.HTML,
                        reply_markup=evm_tx_keyboard(txhash, network),
                        disable_web_page_preview=True,
                    )
                    del pending_txs[txhash]
                elif txhash not in seen_txs.get(name, []):
                    await bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=format_evm_tx(name, address, tx, network),
                        parse_mode=ParseMode.HTML,
                        reply_markup=evm_tx_keyboard(txhash, network),
                        disable_web_page_preview=True,
                    )
                    seen_txs.setdefault(name, []).append(txhash)
                    daily_txs[name].append(_summarize_evm(tx, address, network))

            seen_txs[name] = seen_txs.get(name, [])[-100:]

        # ── SOLANA ──
        elif network == "solana":
            token_account, _bal = await fetch_solana_usdt_account(address, session)
            if not token_account:
                continue  # bu owner'in henuz USDT token hesabi yok

            sigs = await fetch_solana_signatures(token_account, session, limit=SOLANA_TX_LIMIT)
            # En eskiden en yeniye dogru isle, boylece chat'e dogru sirayla dusuyor
            for s in reversed(sigs):
                sig = s.get("signature")
                if not sig or sig in seen_txs.get(name, []):
                    continue

                seen_txs.setdefault(name, []).append(sig)

                if s.get("err"):
                    # Basarisiz islem, bildirim gonderme ama gorulmus say
                    continue

                tx = await fetch_solana_tx(sig, session)
                delta = _solana_tx_delta(tx, address)
                if not delta:
                    continue  # USDT ile ilgisiz veya parse edilemedi

                amount, is_in = delta
                block_time = (tx.get("blockTime") if tx else None) or s.get("blockTime")

                await bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=format_solana_tx(name, sig, amount, is_in, block_time),
                    parse_mode=ParseMode.HTML,
                    reply_markup=solana_tx_keyboard(sig),
                    disable_web_page_preview=True,
                )
                daily_txs[name].append(_summarize_solana(amount, is_in))

            seen_txs[name] = seen_txs.get(name, [])[-100:]

    save_json(STATE_FILE, seen_txs)
    save_json(PENDING_FILE, pending_txs)

# ──────────────────────────────────────────────────────
# VERİ FONKSİYONLARI (komutlar + callback için ortak)
# ──────────────────────────────────────────────────────
async def _bakiye_data():
    lines = [f"💼 <b>Cuzdan Bakiyeleri</b>\n🕐 {now_str()}\n══════════════════════════════"]
    session = HTTP_SESSION
    for name, cfg in WALLETS.items():
        address = cfg["address"]
        if cfg["network"] == "btc":
            info = await fetch_btc_address_info(address, session)
            if info:
                funded  = info.get("chain_stats", {}).get("funded_txo_sum", 0)
                spent   = info.get("chain_stats", {}).get("spent_txo_sum", 0)
                balance = (funded - spent) / 1e8
                mem     = info.get("mempool_stats", {})
                unconf  = (mem.get("funded_txo_sum", 0) - mem.get("spent_txo_sum", 0)) / 1e8
                lines.append(
                    f"\n👛 <b>{e(name)}</b>\n"
                    f"  💰 Bakiye: <code>{balance:.8f} BTC</code>\n"
                    f"  ⏳ Bekleyen: <code>{unconf:+.8f} BTC</code>\n"
                    f"  📍 <code>{e(address[:20])}...</code>"
                )
            else:
                lines.append(f"\n👛 <b>{e(name)}</b>\n  ❌ Bakiye alinamadi.")
        elif cfg["network"] in EVM_NETWORKS:
            balance = await fetch_evm_usdt_balance(address, session, cfg["network"])
            lines.append(
                f"\n👛 <b>{e(name)}</b>\n"
                f"  💰 Bakiye: <code>{balance:.2f} USDT</code>\n"
                f"  📍 <code>{e(address[:20])}...</code>"
            )
        elif cfg["network"] == "solana":
            _, balance = await fetch_solana_usdt_account(address, session)
            lines.append(
                f"\n👛 <b>{e(name)}</b>\n"
                f"  💰 Bakiye: <code>{balance:.6f} USDT</code>\n"
                f"  📍 <code>{e(address[:20])}...</code>"
            )
    lines.append("\n══════════════════════════════")
    return "\n".join(lines)

async def _rapor_text():
    lines = [
        "📊 <b>Gunluk Ozet</b>",
        f"🕐 {now_str()}",
        "══════════════════════════════",
    ]
    has_data = False
    for name, cfg in WALLETS.items():
        entries = daily_txs.get(name, [])
        total_in = total_out = 0.0
        for entry in entries:
            if entry["is_in"]:
                total_in  += entry["amount"]
            else:
                total_out += entry["amount"]
        if entries:
            has_data = True
        lines.append(
            f"\n👛 <b>{e(name)}</b>\n"
            f"  📥 Giris: <code>{total_in:.6f} {cfg['symbol']}</code>\n"
            f"  📤 Cikis: <code>{total_out:.6f} {cfg['symbol']}</code>\n"
            f"  🔢 Islem: <code>{len(entries)} adet</code>"
        )
    if not has_data:
        lines.append("\n✨ Bugun hic islem gerceklesmedi.")
    lines += ["", "══════════════════════════════"]
    return "\n".join(lines)

async def _sonislem_data():
    lines = [f"🔎 <b>Son Islemler</b>\n🕐 {now_str()}\n══════════════════════════════"]
    last_txhash = last_txid = last_signature = last_network = None
    session = HTTP_SESSION

    for name, cfg in WALLETS.items():
        address = cfg["address"]
        lines.append(f"\n👛 <b>{e(name)}</b>")

        if cfg["network"] == "btc":
            txs = await fetch_btc_txs(address, session)
            if txs:
                tx        = txs[0]
                txid      = tx["txid"]
                vout      = tx.get("vout", [])
                status    = tx.get("status", {})
                is_in     = any(o.get("scriptpubkey_address") == address for o in vout)
                amount    = sum(o.get("value", 0) for o in vout if o.get("scriptpubkey_address") == address) / 1e8
                icon      = "📥" if is_in else "📤"
                conf_str  = "✅ Onayli" if status.get("confirmed") else "⏳ Pending"
                lines.append(
                    f"  {icon} <code>{amount:.8f} BTC</code>\n"
                    f"  📋 {conf_str}\n"
                    f"  🕐 {ts_to_str(status.get('block_time'))}\n"
                    f'  <a href="https://blockstream.info/tx/{txid}">TX Goruntule</a>'
                )
                last_txid    = txid
                last_network = "btc"
            else:
                lines.append("  Hic islem bulunamadi.")

        elif cfg["network"] in EVM_NETWORKS:
            txs = await fetch_evm_confirmed(address, session, cfg["network"], offset=1)
            if txs:
                tx     = txs[0]
                txhash = tx.get("hash", "")
                value  = int(tx.get("value", 0)) / 1e6
                is_in  = tx.get("to", "").lower() == address.lower()
                icon   = "📥" if is_in else "📤"
                explorer_url = EVM_NETWORKS[cfg["network"]]["explorer_url"]
                lines.append(
                    f"  {icon} <code>{value:.2f} USDT</code>\n"
                    f"  📋 {tx.get('confirmations','?')} onay ✅\n"
                    f"  🕐 {ts_to_str(tx.get('timeStamp'))}\n"
                    f'  <a href="{explorer_url}{txhash}">TX Goruntule</a>'
                )
                last_txhash  = txhash
                last_network = cfg["network"]
            else:
                lines.append("  Hic islem bulunamadi.")

        elif cfg["network"] == "solana":
            token_account, _bal = await fetch_solana_usdt_account(address, session)
            sig = None
            if token_account:
                sigs = await fetch_solana_signatures(token_account, session, limit=1)
                if sigs:
                    sig_info = sigs[0]
                    sig = sig_info.get("signature")
                    tx  = await fetch_solana_tx(sig, session)
                    delta = _solana_tx_delta(tx, address)
                    block_time = (tx.get("blockTime") if tx else None) or sig_info.get("blockTime")
                    if delta:
                        amount, is_in = delta
                        icon = "📥" if is_in else "📤"
                        lines.append(
                            f"  {icon} <code>{amount:.6f} USDT</code>\n"
                            f"  📋 Onayli ✅\n"
                            f"  🕐 {ts_to_str(block_time)}\n"
                            f'  <a href="https://solscan.io/tx/{sig}">TX Goruntule</a>'
                        )
                    else:
                        lines.append("  ℹ️ Son islem USDT transferi degil / parse edilemedi.")
            if sig:
                last_signature = sig
                last_network   = "solana"
            elif not token_account:
                lines.append("  Bu adres icin henuz USDT token hesabi yok.")
            elif not sig:
                lines.append("  Hic islem bulunamadi.")

    lines.append("\n══════════════════════════════")
    if last_network == "btc" and last_txid:
        keyboard = btc_tx_keyboard(last_txid)
    elif last_network in EVM_NETWORKS and last_txhash:
        keyboard = evm_tx_keyboard(last_txhash, last_network)
    elif last_network == "solana" and last_signature:
        keyboard = solana_tx_keyboard(last_signature)
    else:
        keyboard = main_menu_keyboard()
    return "\n".join(lines), keyboard

def _bekleyenler_data():
    if not pending_txs:
        return "✅ <b>Bekleyen islem yok.</b>\nTum islemler onaylandi.", main_menu_keyboard()
    lines = [f"⏳ <b>Bekleyen Islemler</b>\n🕐 {now_str()}\n══════════════════════════════"]
    for txid, info in pending_txs.items():
        typ  = info.get("type", "?")
        if typ == "btc":
            link = f'<a href="https://mempool.space/tx/{txid}">Mempool.space</a>'
        elif typ in EVM_NETWORKS:
            link = f'<a href="{EVM_NETWORKS[typ]["explorer_url"]}{txid}">{EVM_NETWORKS[typ]["explorer_name"]}</a>'
        else:
            link = ""
        lines.append(
            f"\n👛 <b>{e(info.get('wallet','?'))}</b>\n"
            f"  🔑 <code>{e(txid[:30])}...</code>\n"
            f"  {link}"
        )
    lines.append("\n══════════════════════════════")
    return "\n".join(lines), main_menu_keyboard()

def _sistemkontrol_text():
    uptime_sec = int(time.time() - BOT_START_TIME)
    h, rem = divmod(uptime_sec, 3600)
    m, s   = divmod(rem, 60)
    lines = [
        "🖥️ <b>Sistem Kontrol</b>",
        f"🕐 {now_str()}",
        "══════════════════════════════",
        f"⏱ <b>Uptime:</b> <code>{h}s {m}dk {s}sn</code>",
        f"🔄 <b>Kontrol araligi:</b> <code>{CHECK_INTERVAL_SECONDS} saniye</code>",
        f"📊 <b>Gunluk ozet:</b> <code>{DAILY_REPORT_HOUR:02d}:{DAILY_REPORT_MINUTE:02d} UTC</code>",
        "",
        f"👛 <b>Takip edilen:</b> <code>{len(WALLETS)}</code>",
        f"📝 <b>Gorulmus TX:</b> <code>{sum(len(v) for v in seen_txs.values())}</code>",
        f"⏳ <b>Bekleyen TX:</b> <code>{len(pending_txs)}</code>",
        f"📈 <b>Bugunun islemi:</b> <code>{sum(len(v) for v in daily_txs.values())}</code>",
        "",
    ]
    net_label = {"btc": "BTC", "eth": "Ethereum", "polygon": "Polygon", "solana": "Solana"}
    for name, cfg in WALLETS.items():
        net = net_label.get(cfg["network"], cfg["network"])
        lines.append(f"  ✅ {e(name)} ({net})")

    lines.append("")
    lines.append(f"🌐 <b>Nonlogs Reserves</b> (her {NONLOGS_CHECK_INTERVAL_SECONDS}sn):")
    if nonlogs_reserves:
        for coin, cfg in NONLOGS_TRACKED_ASSETS.items():
            val = nonlogs_reserves.get(coin)
            if val is not None:
                lines.append(f"  ✅ {coin}: <code>{val:.{cfg['decimals']}f}</code>")
    else:
        lines.append("  ⏳ Henuz snapshot alinmadi.")

    lines += ["══════════════════════════════", "<i>Tum sistemler calisiyor.</i>"]
    return "\n".join(lines)

# ──────────────────────────────────────────────────────
# KOMUT HANDLERLARI
# ──────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 <b>Cuzdan Takip Botu</b>\n\n"
        "Asagidaki butonlari veya komutlari kullanabilirsin:\n\n"
        "/rapor — Bugunun ozet raporu\n"
        "/sonislem — Her cüzdanın son islemi\n"
        "/bakiye — Tum cüzdan bakiyeleri\n"
        "/saat — Simdi saat kac (TR)\n"
        "/bekleyenler — Pending islemler\n"
        "/sistemkontrol — Bot durumu\n"
        "/yardim — Bu mesaj",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(),
    )

async def cmd_yardim(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, ctx)

async def cmd_saat(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    utc = datetime.now(tz=timezone.utc)
    tr  = utc + timedelta(hours=3)
    await update.message.reply_text(
        f"🕐 <b>Simdiki Saat</b>\n\n"
        f"🇹🇷 <b>Turkiye:</b> <code>{tr.strftime('%d.%m.%Y %H:%M:%S')}</code>\n"
        f"🌍 <b>UTC:</b> <code>{utc.strftime('%d.%m.%Y %H:%M:%S')}</code>",
        parse_mode=ParseMode.HTML,
    )

async def cmd_rapor(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        await _rapor_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard()
    )

async def cmd_sonislem(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔄 Veriler cekiliyor...", parse_mode=ParseMode.HTML)
    text, kb = await _sonislem_data()
    await msg.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

async def cmd_bakiye(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔄 Bakiyeler cekiliyor...", parse_mode=ParseMode.HTML)
    await msg.edit_text(await _bakiye_data(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def cmd_bekleyenler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text, kb = _bekleyenler_data()
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

async def cmd_sistemkontrol(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        _sistemkontrol_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard()
    )

# ──────────────────────────────────────────────────────
# CALLBACK HANDLER
# ──────────────────────────────────────────────────────
async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data

    if data == "bakiye":
        await query.edit_message_text("🔄 Bakiyeler cekiliyor...", parse_mode=ParseMode.HTML)
        await query.edit_message_text(await _bakiye_data(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
    elif data == "rapor":
        await query.edit_message_text(await _rapor_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
    elif data == "sonislem":
        await query.edit_message_text("🔄 Veriler cekiliyor...", parse_mode=ParseMode.HTML)
        text, kb = await _sonislem_data()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
    elif data == "bekleyenler":
        text, kb = _bekleyenler_data()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
    elif data == "sistemkontrol":
        await query.edit_message_text(_sistemkontrol_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

# ──────────────────────────────────────────────────────
# GÜNLÜK ÖZET
# ──────────────────────────────────────────────────────
async def send_daily_report(bot: Bot):
    text = await _rapor_text()
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=text.replace("Talep Uzerine", "Otomatik").replace("Gunluk Ozet", "Gunluk Ozet Raporu"),
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(),
    )
    for name in daily_txs:
        daily_txs[name] = []
    log.info("Gunluk ozet gonderildi.")

# ──────────────────────────────────────────────────────
# ANA FONKSİYON
# ──────────────────────────────────────────────────────
async def main():
    global HTTP_SESSION

    # Tek paylasimli session - baglanti sayisi sinirli tutularak bellek
    # kullanimini dusuruyoruz (islev/veri kaybi yok, sadece havuz kucultuluyor).
    connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
    HTTP_SESSION = aiohttp.ClientSession(connector=connector)

    try:
        await initialize_snapshots()
        await initialize_nonlogs_snapshot()

        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        app.add_handler(CommandHandler("start",         cmd_start))
        app.add_handler(CommandHandler("yardim",        cmd_yardim))
        app.add_handler(CommandHandler("saat",          cmd_saat))
        app.add_handler(CommandHandler("rapor",         cmd_rapor))
        app.add_handler(CommandHandler("sonislem",      cmd_sonislem))
        app.add_handler(CommandHandler("bakiye",        cmd_bakiye))
        app.add_handler(CommandHandler("bekleyenler",   cmd_bekleyenler))
        app.add_handler(CommandHandler("sistemkontrol", cmd_sistemkontrol))
        app.add_handler(CallbackQueryHandler(callback_handler))

        await app.bot.set_my_commands([
            BotCommand("rapor",         "Bugunun ozet raporu"),
            BotCommand("sonislem",      "Her cuzdanin son islemi"),
            BotCommand("bakiye",        "Tum cuzdan bakiyeleri"),
            BotCommand("saat",          "Simdi saat kac (TR)"),
            BotCommand("bekleyenler",   "Pending islemler"),
            BotCommand("sistemkontrol", "Bot durumu ve istatistik"),
            BotCommand("yardim",        "Komut listesi"),
        ])

        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(check_wallets, "interval", seconds=CHECK_INTERVAL_SECONDS,
                          args=[app.bot], id="check_wallets", max_instances=1)
        scheduler.add_job(check_nonlogs_reserves, "interval", seconds=NONLOGS_CHECK_INTERVAL_SECONDS,
                          args=[app.bot], id="check_nonlogs_reserves", max_instances=1)
        scheduler.add_job(send_daily_report, "cron",
                          hour=DAILY_REPORT_HOUR, minute=DAILY_REPORT_MINUTE,
                          args=[app.bot], id="daily_report")
        scheduler.start()

        await app.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=(
                "✅ <b>Cuzdan Takip Botu Basladi!</b>\n\n"
                f"🔍 Takip: {len(WALLETS)} cuzdan\n"
                f"⏱ Kontrol: {CHECK_INTERVAL_SECONDS} saniye\n"
                f"🌐 Nonlogs Reserves (BTC/GRIN/USDT): {NONLOGS_CHECK_INTERVAL_SECONDS} saniyede bir\n"
                f"📊 Gunluk ozet: {DAILY_REPORT_HOUR:02d}:{DAILY_REPORT_MINUTE:02d} UTC\n\n"
                "Komutlar icin /yardim yaz."
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
        )

        log.info("Bot calisiyor...")
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)

        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            log.info("Bot durduruldu.")
            scheduler.shutdown(wait=False)
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
    finally:
        await HTTP_SESSION.close()


if __name__ == "__main__":
    asyncio.run(main())
