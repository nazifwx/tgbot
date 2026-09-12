"""
Telegram Cuzdan Takip Botu
- BTC / Ethereum (USDT) / Polygon (USDT) / Solana (USDT) destegi
- Cuzdanlar artik SABIT KOD DEGIL: /cuzdanekle ve /cuzdansil komutlariyla
  (ya da menudeki butonlarla) Telegram uzerinden interaktif olarak
  eklenip cikartilabilir. Liste "wallets.json" dosyasinda saklanir.
- Inline butonlar
- Gunluk ozet raporu
- nonlogs.io/reserves takibi (degismedi)
- /komutlar destegi

RAM OPTIMIZASYONLARI (islev kaybi olmadan):
  1) Tum HTTP istekleri icin tek, paylasimli aiohttp.ClientSession kullanilir.
  2) daily_txs artik ham (raw) API cevabini degil, sadece rapor icin
     gereken minimal alanlari (miktar, yon, tip) saklar.

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

CUZDAN YONETIMI (YENI):
  /cuzdanekle  -> Ag secimi (BTC / ETH / Polygon / Solana) -> adres -> isim
                  sorulur, dogrulanir ve anlik olarak takibe eklenir.
  /cuzdansil   -> Mevcut cuzdanlardan biri secilip onay ile silinir.
  /cuzdanlar   -> Su an takip edilen tum cuzdanlarin listesi.
  Ana menudeki "➕ Cuzdan Ekle" / "➖ Cuzdan Sil" / "📋 Cuzdanlar" butonlari
  ile de ayni islemler yapilabilir.
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
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode

# ──────────────────────────────────────────────────────
# AYARLAR
# ──────────────────────────────────────────────────────
# NOT: Bu degerler ortam degiskeni (env var) olarak verilmezse kod icindeki
# varsayilanlar kullanilir. Bu anahtarlari bir yerde paylastiysaniz
# (ornegin bu sohbette) guvenlik icin yenilemeniz (rotate) onerilir.
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "8649558470:AAHCRXTKxCiVi2MaAp88trJVe7McE8v7j9k")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "492272237")

# Cuzdanlar artik kod icinde sabit degil, wallets.json dosyasindan okunur.
# Dosya yoksa asagidaki varsayilan liste ile olusturulur (ilk kurulum).
WALLETS_FILE = "wallets.json"

DEFAULT_WALLETS = {
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
USDT_CONTRACT_POLYGON  = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"   # Polygon USDT
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

# Desteklenen aglar - /cuzdanekle akisinda kullanicidan secmesi istenir.
NETWORK_LABELS = {
    "btc":     "🟠 Bitcoin (BTC)",
    "eth":     "🔷 Ethereum (USDT)",
    "polygon": "🟣 Polygon (USDT)",
    "solana":  "🟢 Solana (USDT)",
}
NETWORK_EMOJI = {"btc": "🟠", "eth": "🔷", "polygon": "🟣", "solana": "🟢"}
NETWORK_SYMBOL = {"btc": "BTC", "eth": "USDT", "polygon": "USDT", "solana": "USDT"}

# Adres format dogrulamasi (kaba ama pratik bir kontrol - kesin garanti degildir)
ADDRESS_PATTERNS = {
    "btc":     re.compile(r"^(bc1[a-z0-9]{25,90}|[13][a-km-zA-HJ-NP-Z1-9]{25,34})$"),
    "eth":     re.compile(r"^0x[a-fA-F0-9]{40}$"),
    "polygon": re.compile(r"^0x[a-fA-F0-9]{40}$"),
    "solana":  re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$"),
}

# Cuzdan ekleme akisi (ConversationHandler) durumlari
ADDING_NETWORK, ADDING_ADDRESS, ADDING_NAME = range(3)

# ── nonlogs.io/reserves takibi (degismedi) ──
NONLOGS_RESERVES_URL          = "https://nonlogs.io/reserves"
NONLOGS_CHECK_INTERVAL_SECONDS = 60
NONLOGS_STATE_FILE            = "nonlogs_reserves.json"
NONLOGS_TRACKED_ASSETS = {
    "BTC":  {"heading": "Bitcoin",    "symbol_line": "BTC BTC",    "unit": "BTC",  "decimals": 8},
    "GRIN": {"heading": "Grin",       "symbol_line": "GRIN Grin",  "unit": "GRIN", "decimals": 8},
    "USDT": {"heading": "Tether USD", "symbol_line": "USDT ETH",   "unit": "USDT", "decimals": 6},
    "XMR":  {"heading": "Monero",     "symbol_line": "XMR XMR",    "unit": "XMR",  "decimals": 8},
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
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_wallets():
    data = load_json(WALLETS_FILE)
    if not data:
        data = dict(DEFAULT_WALLETS)
        save_json(WALLETS_FILE, data)
    return data

seen_txs    = load_json(STATE_FILE)
pending_txs = load_json(PENDING_FILE)
WALLETS     = load_wallets()
# Artik ham tx degil, minimal ozet dict tutuluyor: {"type":..,"amount":..,"is_in":..}
daily_txs   = {name: [] for name in WALLETS}

# nonlogs.io/reserves icin son bilinen bakiyeler
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

def net_label(network):
    return NETWORK_LABELS.get(network, network)

def net_emoji(network):
    return NETWORK_EMOJI.get(network, "🔘")

DIVIDER = "━━━━━━━━━━━━━━━━━━━━"

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
            InlineKeyboardButton("📋 Cüzdanlar", callback_data="cuzdanlar"),
            InlineKeyboardButton("🖥️ Sistem", callback_data="sistemkontrol"),
        ],
        [
            InlineKeyboardButton("➕ Cüzdan Ekle", callback_data="cuzdanekle_baslat"),
            InlineKeyboardButton("➖ Cüzdan Sil", callback_data="cuzdansil_baslat"),
        ],
    ])

def network_choice_keyboard():
    rows = []
    row = []
    for net, label in NETWORK_LABELS.items():
        row.append(InlineKeyboardButton(label, callback_data=f"addnet_{net}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("❌ İptal", callback_data="addnet_iptal")])
    return InlineKeyboardMarkup(rows)

def wallet_delete_keyboard():
    rows = []
    for name, cfg in WALLETS.items():
        label = f"{net_emoji(cfg['network'])} {name}"
        rows.append([InlineKeyboardButton(label, callback_data=f"delwallet_{name}")])
    rows.append([InlineKeyboardButton("❌ İptal", callback_data="delwallet_iptal")])
    return InlineKeyboardMarkup(rows)

def wallet_delete_confirm_keyboard(name):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Evet, Sil", callback_data=f"delconfirm_{name}"),
            InlineKeyboardButton("↩️ Vazgeç", callback_data="delcancel"),
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
        result = await _solana_rpc("getTokenAccountBalance", [token_account], session)
        if result:
            balance = float(result.get("value", {}).get("uiAmount") or 0)
            return token_account, balance

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

    for idx, pb in post_map.items():
        if pb.get("owner") != owner:
            continue
        pre_amt  = float((pre_map.get(idx, {}).get("uiTokenAmount") or {}).get("uiAmount") or 0)
        post_amt = float((pb.get("uiTokenAmount") or {}).get("uiAmount") or 0)
        delta = post_amt - pre_amt
        if delta != 0:
            return abs(delta), delta > 0

    for idx, pb in pre_map.items():
        if pb.get("owner") != owner or idx in post_map:
            continue
        pre_amt = float((pb.get("uiTokenAmount") or {}).get("uiAmount") or 0)
        if pre_amt != 0:
            return pre_amt, False

    return None

# ── NONLOGS.IO/RESERVES ── (degismedi)
def _strip_html_to_text(html):
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    text = re.sub(r"\s+", " ", text)
    return text.strip()

async def fetch_nonlogs_reserves(session):
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
        if abs(delta) < (10 ** -decimals) / 2:
            continue

        icon = "📈" if delta > 0 else "📉"
        sign = "+" if delta > 0 else ""
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=(
                f"{icon} <b>Nonlogs Rezerv Değişikliği · {coin}</b>\n"
                f"{DIVIDER}\n"
                f"🔄 <b>Değişim:</b> <code>{sign}{delta:.{decimals}f} {coin}</code>\n"
                f"💰 <b>Önceki:</b> <code>{prev:.{decimals}f} {coin}</code>\n"
                f"💰 <b>Güncel:</b> <code>{value:.{decimals}f} {coin}</code>\n"
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
    direction   = "GİRİŞ" if is_incoming else "ÇIKIŞ"
    amount_sat  = sum(o.get("value", 0) for o in vout if o.get("scriptpubkey_address") == address)
    amount_str  = f"{amount_sat / 1e8:.8f} BTC" if amount_sat else "?"
    fee_sat     = tx.get("fee", 0)
    fee_str     = f"{fee_sat / 1e8:.8f} BTC" if fee_sat else "?"

    if is_pending:
        header     = "⚡ <b>Yeni BTC İşlemi · PENDING</b>"
        status_str = "⏳ Bekliyor (Mempool)"
        time_str   = now_str()
    else:
        header     = "🔔 <b>Yeni BTC İşlemi</b>"
        status_str = "✅ Onaylandı" if status.get("confirmed") else "⏳ Bekliyor"
        time_str   = ts_to_str(status.get("block_time"))

    return (
        f"{header}\n"
        f"{net_emoji('btc')} <b>Cüzdan:</b> {e(wallet_name)}\n"
        f"{DIVIDER}\n"
        f"{dir_icon} <b>Yön:</b> {direction}\n"
        f"💰 <b>Miktar:</b> <code>{e(amount_str)}</code>\n"
        f"⛽ <b>Ücret:</b> <code>{e(fee_str)}</code>\n"
        f"📋 <b>Durum:</b> {status_str}\n"
        f"🕐 <b>Zaman:</b> {e(time_str)}\n"
        f"{DIVIDER}\n"
        f"🔑 <code>{e(txid)}</code>"
    )

def format_evm_tx(wallet_name, address, tx, network, is_pending=False):
    net_name    = "Ethereum" if network == "eth" else "Polygon"
    txhash      = tx.get("hash", "")
    value       = int(tx.get("value", 0)) / 1e6
    from_addr   = tx.get("from", "")
    to_addr     = tx.get("to", "")
    is_incoming = to_addr.lower() == address.lower()
    dir_icon    = "📥" if is_incoming else "📤"
    direction   = "GİRİŞ" if is_incoming else "ÇIKIŞ"
    label       = "Gönderen" if is_incoming else "Alıcı"
    counterpart = from_addr if is_incoming else to_addr
    confs       = int(tx.get("confirmations", 0))
    gas_gwei    = int(tx.get("gasPrice", 0)) / 1e9

    if is_pending:
        header     = f"⚡ <b>Yeni {net_name} USDT İşlemi · PENDING</b>"
        status_str = "⏳ Bekliyor"
        time_str   = now_str()
    else:
        header     = f"🔔 <b>Yeni {net_name} USDT İşlemi</b>"
        status_str = f"✅ Onaylandı ({confs} onay)"
        time_str   = ts_to_str(tx.get("timeStamp"))

    return (
        f"{header}\n"
        f"{net_emoji(network)} <b>Cüzdan:</b> {e(wallet_name)}\n"
        f"{DIVIDER}\n"
        f"{dir_icon} <b>Yön:</b> {direction}\n"
        f"💰 <b>Miktar:</b> <code>{value:.2f} USDT</code>\n"
        f"👤 <b>{label}:</b> <code>{e(counterpart)}</code>\n"
        f"⛽ <b>Gas:</b> <code>{gas_gwei:.1f} Gwei</code>\n"
        f"📋 <b>Durum:</b> {status_str}\n"
        f"🕐 <b>Zaman:</b> {e(time_str)}\n"
        f"{DIVIDER}\n"
        f"🔑 <code>{e(txhash)}</code>"
    )

def format_solana_tx(wallet_name, signature, amount, is_in, block_time):
    dir_icon  = "📥" if is_in else "📤"
    direction = "GİRİŞ" if is_in else "ÇIKIŞ"
    return (
        "🔔 <b>Yeni Solana USDT İşlemi</b>\n"
        f"{net_emoji('solana')} <b>Cüzdan:</b> {e(wallet_name)}\n"
        f"{DIVIDER}\n"
        f"{dir_icon} <b>Yön:</b> {direction}\n"
        f"💰 <b>Miktar:</b> <code>{amount:.6f} USDT</code>\n"
        f"📋 <b>Durum:</b> ✅ Onaylandı\n"
        f"🕐 <b>Zaman:</b> {e(ts_to_str(block_time))}\n"
        f"{DIVIDER}\n"
        f"🔑 <code>{e(signature[:24])}...</code>"
    )

def format_confirmed_update(wallet_name, txid, network, extra=""):
    return (
        f"✅ <b>İşlem Onaylandı</b>\n"
        f"{net_emoji(network)} <b>Cüzdan:</b> {e(wallet_name)}\n"
        f"{DIVIDER}\n"
        f"🔑 <code>{e(txid)}</code>\n"
        f"{extra}"
    )

# ──────────────────────────────────────────────────────
# CUZDAN YARDIMCI FONKSIYONLARI
# ──────────────────────────────────────────────────────
async def collect_snapshot_ids(cfg):
    """Verilen tek bir cuzdan icin, o ana kadarki islem id'lerini toplar
    (yeni eklenen bir cuzdanin eski islemlerinin bildirim olarak
    dusmemesi icin baslangic referansi olusturur)."""
    address = cfg["address"]
    network = cfg["network"]
    ids = []
    if network == "btc":
        txs     = await fetch_btc_txs(address, HTTP_SESSION)
        mempool = await fetch_btc_mempool_txs(address, HTTP_SESSION)
        ids     = [tx["txid"] for tx in txs[:20]] + [tx["txid"] for tx in mempool]
    elif network in EVM_NETWORKS:
        txs = await fetch_evm_confirmed(address, HTTP_SESSION, network)
        ids = [tx["hash"] for tx in txs[:20]]
    elif network == "solana":
        token_account, _ = await fetch_solana_usdt_account(address, HTTP_SESSION, use_cache=False)
        if token_account:
            sigs = await fetch_solana_signatures(token_account, HTTP_SESSION, limit=25)
            ids  = [s["signature"] for s in sigs]
    return ids

def is_valid_address(network, address):
    pattern = ADDRESS_PATTERNS.get(network)
    return bool(pattern and pattern.match(address.strip()))

def find_wallet_by_address(address):
    for name, cfg in WALLETS.items():
        if cfg["address"].lower() == address.lower():
            return name
    return None

async def add_wallet(name, network, address):
    """Yeni bir cuzdani WALLETS'a ekler, snapshot alir ve diske kaydeder."""
    cfg = {
        "address": address,
        "network": network,
        "symbol":  NETWORK_SYMBOL.get(network, "?"),
    }
    WALLETS[name] = cfg
    daily_txs.setdefault(name, [])
    ids = await collect_snapshot_ids(cfg)
    seen_txs[name] = ids
    save_json(WALLETS_FILE, WALLETS)
    save_json(STATE_FILE, seen_txs)
    log.info(f"Yeni cuzdan eklendi: {name} ({network}) - {len(ids)} eski islem isaretlendi")

def remove_wallet(name):
    """Cuzdani WALLETS ve ilgili tum state'lerden temizler."""
    cfg = WALLETS.pop(name, None)
    if cfg is None:
        return False
    seen_txs.pop(name, None)
    daily_txs.pop(name, None)
    if cfg["network"] == "solana":
        solana_token_account_cache.pop(cfg["address"], None)
    for txid in [k for k, v in pending_txs.items() if v.get("wallet") == name]:
        pending_txs.pop(txid, None)
    save_json(WALLETS_FILE, WALLETS)
    save_json(STATE_FILE, seen_txs)
    save_json(PENDING_FILE, pending_txs)
    log.info(f"Cuzdan silindi: {name}")
    return True

# ──────────────────────────────────────────────────────
# SNAPSHOT (baslangicta tum cuzdanlar icin)
# ──────────────────────────────────────────────────────
async def initialize_snapshots():
    global seen_txs
    log.info("Snapshot aliniyor...")
    for name, cfg in WALLETS.items():
        ids = await collect_snapshot_ids(cfg)
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
    for name, cfg in list(WALLETS.items()):
        address = cfg["address"]
        network = cfg["network"]

        # ── BTC ──
        if network == "btc":
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

            for tx in (await fetch_btc_txs(address, session))[:10]:
                txid = tx["txid"]
                if txid in pending_txs:
                    await bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=format_confirmed_update(name, txid, "btc"),
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
                        text=format_confirmed_update(name, txhash, network,
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
                continue

            sigs = await fetch_solana_signatures(token_account, session, limit=SOLANA_TX_LIMIT)
            for s in reversed(sigs):
                sig = s.get("signature")
                if not sig or sig in seen_txs.get(name, []):
                    continue

                seen_txs.setdefault(name, []).append(sig)

                if s.get("err"):
                    continue

                tx = await fetch_solana_tx(sig, session)
                delta = _solana_tx_delta(tx, address)
                if not delta:
                    continue

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
    if not WALLETS:
        return f"💼 <b>Cüzdan Bakiyeleri</b>\n{DIVIDER}\n✨ Henüz takip edilen cüzdan yok.\n➕ Eklemek için /cuzdanekle yazabilirsin."
    lines = [f"💼 <b>Cüzdan Bakiyeleri</b>\n🕐 {now_str()}\n{DIVIDER}"]
    session = HTTP_SESSION
    for name, cfg in WALLETS.items():
        address = cfg["address"]
        header = f"\n{net_emoji(cfg['network'])} <b>{e(name)}</b>"
        if cfg["network"] == "btc":
            info = await fetch_btc_address_info(address, session)
            if info:
                funded  = info.get("chain_stats", {}).get("funded_txo_sum", 0)
                spent   = info.get("chain_stats", {}).get("spent_txo_sum", 0)
                balance = (funded - spent) / 1e8
                mem     = info.get("mempool_stats", {})
                unconf  = (mem.get("funded_txo_sum", 0) - mem.get("spent_txo_sum", 0)) / 1e8
                lines.append(
                    f"{header}\n"
                    f"  💰 Bakiye: <code>{balance:.8f} BTC</code>\n"
                    f"  ⏳ Bekleyen: <code>{unconf:+.8f} BTC</code>\n"
                    f"  📍 <code>{e(address[:20])}...</code>"
                )
            else:
                lines.append(f"{header}\n  ❌ Bakiye alınamadı.")
        elif cfg["network"] in EVM_NETWORKS:
            balance = await fetch_evm_usdt_balance(address, session, cfg["network"])
            lines.append(
                f"{header}\n"
                f"  💰 Bakiye: <code>{balance:.2f} USDT</code>\n"
                f"  📍 <code>{e(address[:20])}...</code>"
            )
        elif cfg["network"] == "solana":
            _, balance = await fetch_solana_usdt_account(address, session)
            lines.append(
                f"{header}\n"
                f"  💰 Bakiye: <code>{balance:.6f} USDT</code>\n"
                f"  📍 <code>{e(address[:20])}...</code>"
            )
    lines.append(f"\n{DIVIDER}")
    return "\n".join(lines)

async def _rapor_text():
    lines = [
        "📊 <b>Günlük Özet</b>",
        f"🕐 {now_str()}",
        DIVIDER,
    ]
    has_data = False
    if not WALLETS:
        lines.append("\n✨ Takip edilen cüzdan yok.")
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
            f"\n{net_emoji(cfg['network'])} <b>{e(name)}</b>\n"
            f"  📥 Giriş: <code>{total_in:.6f} {cfg['symbol']}</code>\n"
            f"  📤 Çıkış: <code>{total_out:.6f} {cfg['symbol']}</code>\n"
            f"  🔢 İşlem: <code>{len(entries)} adet</code>"
        )
    if WALLETS and not has_data:
        lines.append("\n✨ Bugün hiç işlem gerçekleşmedi.")
    lines += ["", DIVIDER]
    return "\n".join(lines)

async def _sonislem_data():
    if not WALLETS:
        return (f"🔎 <b>Son İşlemler</b>\n{DIVIDER}\n✨ Henüz takip edilen cüzdan yok.", main_menu_keyboard())
    lines = [f"🔎 <b>Son İşlemler</b>\n🕐 {now_str()}\n{DIVIDER}"]
    last_txhash = last_txid = last_signature = last_network = None
    session = HTTP_SESSION

    for name, cfg in WALLETS.items():
        address = cfg["address"]
        lines.append(f"\n{net_emoji(cfg['network'])} <b>{e(name)}</b>")

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
                conf_str  = "✅ Onaylı" if status.get("confirmed") else "⏳ Bekliyor"
                lines.append(
                    f"  {icon} <code>{amount:.8f} BTC</code>\n"
                    f"  📋 {conf_str}\n"
                    f"  🕐 {ts_to_str(status.get('block_time'))}\n"
                    f'  <a href="https://blockstream.info/tx/{txid}">TX Görüntüle</a>'
                )
                last_txid    = txid
                last_network = "btc"
            else:
                lines.append("  Hiç işlem bulunamadı.")

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
                    f'  <a href="{explorer_url}{txhash}">TX Görüntüle</a>'
                )
                last_txhash  = txhash
                last_network = cfg["network"]
            else:
                lines.append("  Hiç işlem bulunamadı.")

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
                            f"  📋 Onaylı ✅\n"
                            f"  🕐 {ts_to_str(block_time)}\n"
                            f'  <a href="https://solscan.io/tx/{sig}">TX Görüntüle</a>'
                        )
                    else:
                        lines.append("  ℹ️ Son işlem USDT transferi değil / ayrıştırılamadı.")
            if sig:
                last_signature = sig
                last_network   = "solana"
            elif not token_account:
                lines.append("  Bu adres için henüz USDT token hesabı yok.")
            elif not sig:
                lines.append("  Hiç işlem bulunamadı.")

    lines.append(f"\n{DIVIDER}")
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
        return "✅ <b>Bekleyen işlem yok</b>\nTüm işlemler onaylandı.", main_menu_keyboard()
    lines = [f"⏳ <b>Bekleyen İşlemler</b>\n🕐 {now_str()}\n{DIVIDER}"]
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
    lines.append(f"\n{DIVIDER}")
    return "\n".join(lines), main_menu_keyboard()

def _cuzdanlar_text():
    if not WALLETS:
        return (
            f"📋 <b>Takip Edilen Cüzdanlar</b>\n{DIVIDER}\n"
            "✨ Henüz takip edilen cüzdan yok.\n"
            "➕ Eklemek için /cuzdanekle yazabilirsin veya aşağıdaki menüyü kullanabilirsin."
        )
    lines = [f"📋 <b>Takip Edilen Cüzdanlar</b> ({len(WALLETS)})\n{DIVIDER}"]
    for name, cfg in WALLETS.items():
        lines.append(
            f"\n{net_emoji(cfg['network'])} <b>{e(name)}</b>\n"
            f"  🌐 Ağ: {net_label(cfg['network'])}\n"
            f"  📍 <code>{e(cfg['address'])}</code>"
        )
    lines.append(f"\n{DIVIDER}")
    return "\n".join(lines)

def _sistemkontrol_text():
    uptime_sec = int(time.time() - BOT_START_TIME)
    h, rem = divmod(uptime_sec, 3600)
    m, s   = divmod(rem, 60)
    lines = [
        "🖥️ <b>Sistem Kontrol</b>",
        f"🕐 {now_str()}",
        DIVIDER,
        f"⏱ <b>Uptime:</b> <code>{h}s {m}dk {s}sn</code>",
        f"🔄 <b>Kontrol aralığı:</b> <code>{CHECK_INTERVAL_SECONDS} saniye</code>",
        f"📊 <b>Günlük özet:</b> <code>{DAILY_REPORT_HOUR:02d}:{DAILY_REPORT_MINUTE:02d} UTC</code>",
        "",
        f"👛 <b>Takip edilen:</b> <code>{len(WALLETS)}</code>",
        f"📝 <b>Görülmüş TX:</b> <code>{sum(len(v) for v in seen_txs.values())}</code>",
        f"⏳ <b>Bekleyen TX:</b> <code>{len(pending_txs)}</code>",
        f"📈 <b>Bugünün işlemi:</b> <code>{sum(len(v) for v in daily_txs.values())}</code>",
        "",
    ]
    if WALLETS:
        for name, cfg in WALLETS.items():
            lines.append(f"  ✅ {net_emoji(cfg['network'])} {e(name)} ({net_label(cfg['network'])})")
    else:
        lines.append("  ✨ Henüz cüzdan eklenmedi.")

    lines.append("")
    lines.append(f"🌐 <b>Nonlogs Reserves</b> (her {NONLOGS_CHECK_INTERVAL_SECONDS}sn):")
    if nonlogs_reserves:
        for coin, cfg in NONLOGS_TRACKED_ASSETS.items():
            val = nonlogs_reserves.get(coin)
            if val is not None:
                lines.append(f"  ✅ {coin}: <code>{val:.{cfg['decimals']}f}</code>")
    else:
        lines.append("  ⏳ Henüz snapshot alınmadı.")

    lines += [DIVIDER, "<i>Tüm sistemler çalışıyor.</i>"]
    return "\n".join(lines)

# ──────────────────────────────────────────────────────
# TEMEL KOMUT HANDLERLARI
# ──────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 <b>Cüzdan Takip Botu</b>\n\n"
        "Aşağıdaki butonları veya komutları kullanabilirsin:\n\n"
        "/rapor — Bugünün özet raporu\n"
        "/sonislem — Her cüzdanın son işlemi\n"
        "/bakiye — Tüm cüzdan bakiyeleri\n"
        "/cuzdanlar — Takip edilen cüzdanların listesi\n"
        "/cuzdanekle — Yeni cüzdan ekle\n"
        "/cuzdansil — Bir cüzdanı takipten çıkar\n"
        "/saat — Şimdi saat kaç (TR)\n"
        "/bekleyenler — Bekleyen (pending) işlemler\n"
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
        f"🕐 <b>Şimdiki Saat</b>\n\n"
        f"🇹🇷 <b>Türkiye:</b> <code>{tr.strftime('%d.%m.%Y %H:%M:%S')}</code>\n"
        f"🌍 <b>UTC:</b> <code>{utc.strftime('%d.%m.%Y %H:%M:%S')}</code>",
        parse_mode=ParseMode.HTML,
    )

async def cmd_rapor(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        await _rapor_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard()
    )

async def cmd_sonislem(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔄 Veriler çekiliyor...", parse_mode=ParseMode.HTML)
    text, kb = await _sonislem_data()
    await msg.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

async def cmd_bakiye(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔄 Bakiyeler çekiliyor...", parse_mode=ParseMode.HTML)
    await msg.edit_text(await _bakiye_data(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def cmd_bekleyenler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text, kb = _bekleyenler_data()
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

async def cmd_sistemkontrol(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        _sistemkontrol_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard()
    )

async def cmd_cuzdanlar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        _cuzdanlar_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard()
    )

# ──────────────────────────────────────────────────────
# CÜZDAN EKLEME AKIŞI (ConversationHandler)
# ──────────────────────────────────────────────────────
async def cuzdanekle_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "➕ <b>Yeni Cüzdan Ekle</b>\n"
        f"{DIVIDER}\n"
        "Lütfen takip etmek istediğin ağı seç:"
    )
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=network_choice_keyboard())
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=network_choice_keyboard())
    return ADDING_NETWORK

async def cuzdanekle_network_secildi(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    network = query.data.replace("addnet_", "")

    if network == "iptal":
        await query.edit_message_text("❌ Cüzdan ekleme iptal edildi.")
        return ConversationHandler.END

    ctx.user_data["yeni_network"] = network
    await query.edit_message_text(
        f"{net_emoji(network)} <b>{net_label(network)}</b> seçildi.\n"
        f"{DIVIDER}\n"
        "Şimdi lütfen cüzdan adresini gönder.\n"
        "(İptal etmek için /iptal yazabilirsin.)",
        parse_mode=ParseMode.HTML,
    )
    return ADDING_ADDRESS

async def cuzdanekle_adres_alindi(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    network = ctx.user_data.get("yeni_network")
    address = update.message.text.strip()

    if not is_valid_address(network, address):
        await update.message.reply_text(
            f"⚠️ Bu adres {net_label(network)} formatına uymuyor gibi görünüyor.\n"
            "Lütfen adresi kontrol edip tekrar gönder. (İptal için /iptal)"
        )
        return ADDING_ADDRESS

    existing = find_wallet_by_address(address)
    if existing:
        await update.message.reply_text(
            f"⚠️ Bu adres zaten <b>{e(existing)}</b> ismiyle takip ediliyor.\n"
            "Farklı bir adres gönder ya da /iptal ile çık.",
            parse_mode=ParseMode.HTML,
        )
        return ADDING_ADDRESS

    ctx.user_data["yeni_address"] = address
    await update.message.reply_text(
        "✅ Adres doğrulandı.\n"
        f"{DIVIDER}\n"
        "Şimdi bu cüzdana bir isim ver (örn. \"İş Cüzdanı\").\n"
        "İsim vermek istemezsen \"atla\" yazabilirsin.",
    )
    return ADDING_NAME

async def cuzdanekle_isim_alindi(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    network = ctx.user_data.get("yeni_network")
    address = ctx.user_data.get("yeni_address")
    raw_name = update.message.text.strip()

    if raw_name.lower() in ("atla", "skip", "-"):
        name = f"{net_label(network).split(' ', 1)[-1].strip()} {address[:6]}"
    else:
        name = raw_name[:64]

    base_name = name
    suffix = 2
    while name in WALLETS:
        name = f"{base_name} ({suffix})"
        suffix += 1

    msg = await update.message.reply_text("🔄 Cüzdan ekleniyor, geçmiş işlemler taranıyor...")
    await add_wallet(name, network, address)

    await msg.edit_text(
        "✅ <b>Cüzdan Eklendi</b>\n"
        f"{DIVIDER}\n"
        f"{net_emoji(network)} <b>İsim:</b> {e(name)}\n"
        f"🌐 <b>Ağ:</b> {net_label(network)}\n"
        f"📍 <code>{e(address)}</code>\n"
        f"{DIVIDER}\n"
        "Bundan sonraki işlemler otomatik olarak bildirilecek.",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(),
    )
    ctx.user_data.pop("yeni_network", None)
    ctx.user_data.pop("yeni_address", None)
    return ConversationHandler.END

async def cuzdanekle_iptal(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.pop("yeni_network", None)
    ctx.user_data.pop("yeni_address", None)
    await update.message.reply_text("❌ Cüzdan ekleme iptal edildi.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END

wallet_add_conversation = ConversationHandler(
    entry_points=[
        CommandHandler("cuzdanekle", cuzdanekle_start),
        CallbackQueryHandler(cuzdanekle_start, pattern="^cuzdanekle_baslat$"),
    ],
    states={
        ADDING_NETWORK: [CallbackQueryHandler(cuzdanekle_network_secildi, pattern="^addnet_")],
        ADDING_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, cuzdanekle_adres_alindi)],
        ADDING_NAME:    [MessageHandler(filters.TEXT & ~filters.COMMAND, cuzdanekle_isim_alindi)],
    },
    fallbacks=[CommandHandler("iptal", cuzdanekle_iptal)],
)

# ──────────────────────────────────────────────────────
# CÜZDAN SİLME AKIŞI (basit callback zinciri, ConversationHandler gerekmiyor)
# ──────────────────────────────────────────────────────
async def cuzdansil_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not WALLETS:
        await query.edit_message_text(
            "✨ Silinecek bir cüzdan yok.", reply_markup=main_menu_keyboard()
        )
        return
    await query.edit_message_text(
        "➖ <b>Cüzdan Sil</b>\n"
        f"{DIVIDER}\n"
        "Takipten çıkarmak istediğin cüzdanı seç:",
        parse_mode=ParseMode.HTML,
        reply_markup=wallet_delete_keyboard(),
    )

async def cuzdansil_confirm_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data.replace("delwallet_", "")

    if name == "iptal":
        await query.edit_message_text("❌ İşlem iptal edildi.", reply_markup=main_menu_keyboard())
        return

    cfg = WALLETS.get(name)
    if not cfg:
        await query.edit_message_text("⚠️ Bu cüzdan zaten bulunamadı.", reply_markup=main_menu_keyboard())
        return

    await query.edit_message_text(
        f"⚠️ <b>{e(name)}</b> ({net_label(cfg['network'])}) cüzdanını takipten çıkarmak\n"
        "istediğine emin misin?",
        parse_mode=ParseMode.HTML,
        reply_markup=wallet_delete_confirm_keyboard(name),
    )

async def cuzdansil_execute(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data.replace("delconfirm_", "")
    removed = remove_wallet(name)
    if removed:
        await query.edit_message_text(
            f"🗑️ <b>{e(name)}</b> takipten çıkarıldı.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
        )
    else:
        await query.edit_message_text("⚠️ Cüzdan bulunamadı (belki zaten silinmiş).", reply_markup=main_menu_keyboard())

async def cuzdansil_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("↩️ Silme işlemi iptal edildi.", reply_markup=main_menu_keyboard())

async def cmd_cuzdansil(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not WALLETS:
        await update.message.reply_text("✨ Silinecek bir cüzdan yok.", reply_markup=main_menu_keyboard())
        return
    await update.message.reply_text(
        "➖ <b>Cüzdan Sil</b>\n"
        f"{DIVIDER}\n"
        "Takipten çıkarmak istediğin cüzdanı seç:",
        parse_mode=ParseMode.HTML,
        reply_markup=wallet_delete_keyboard(),
    )

# ──────────────────────────────────────────────────────
# GENEL (ANA MENÜ) CALLBACK HANDLER
# ──────────────────────────────────────────────────────
async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data

    if data == "bakiye":
        await query.edit_message_text("🔄 Bakiyeler çekiliyor...", parse_mode=ParseMode.HTML)
        await query.edit_message_text(await _bakiye_data(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
    elif data == "rapor":
        await query.edit_message_text(await _rapor_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
    elif data == "sonislem":
        await query.edit_message_text("🔄 Veriler çekiliyor...", parse_mode=ParseMode.HTML)
        text, kb = await _sonislem_data()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
    elif data == "bekleyenler":
        text, kb = _bekleyenler_data()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
    elif data == "sistemkontrol":
        await query.edit_message_text(_sistemkontrol_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
    elif data == "cuzdanlar":
        await query.edit_message_text(_cuzdanlar_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

# ──────────────────────────────────────────────────────
# GÜNLÜK ÖZET
# ──────────────────────────────────────────────────────
async def send_daily_report(bot: Bot):
    text = await _rapor_text()
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=text.replace("Günlük Özet", "Günlük Özet Raporu (Otomatik)"),
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

    connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
    HTTP_SESSION = aiohttp.ClientSession(connector=connector)

    try:
        await initialize_snapshots()
        await initialize_nonlogs_snapshot()

        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

        # Cuzdan ekleme akisi (ConversationHandler) - digerlerinden ONCE eklenmeli
        app.add_handler(wallet_add_conversation)

        # Cuzdan silme akisi (ozel pattern'li callback'ler - genel handler'dan ONCE)
        app.add_handler(CallbackQueryHandler(cuzdansil_menu, pattern="^cuzdansil_baslat$"))
        app.add_handler(CallbackQueryHandler(cuzdansil_confirm_prompt, pattern="^delwallet_"))
        app.add_handler(CallbackQueryHandler(cuzdansil_execute, pattern="^delconfirm_"))
        app.add_handler(CallbackQueryHandler(cuzdansil_cancel, pattern="^delcancel$"))

        app.add_handler(CommandHandler("start",         cmd_start))
        app.add_handler(CommandHandler("yardim",        cmd_yardim))
        app.add_handler(CommandHandler("saat",          cmd_saat))
        app.add_handler(CommandHandler("rapor",         cmd_rapor))
        app.add_handler(CommandHandler("sonislem",      cmd_sonislem))
        app.add_handler(CommandHandler("bakiye",        cmd_bakiye))
        app.add_handler(CommandHandler("bekleyenler",   cmd_bekleyenler))
        app.add_handler(CommandHandler("sistemkontrol", cmd_sistemkontrol))
        app.add_handler(CommandHandler("cuzdanlar",     cmd_cuzdanlar))
        app.add_handler(CommandHandler("cuzdansil",     cmd_cuzdansil))

        # Genel menu butonlari - en sonda, ozel pattern'li olanlardan sonra
        app.add_handler(CallbackQueryHandler(callback_handler))

        await app.bot.set_my_commands([
            BotCommand("rapor",         "Bugunun ozet raporu"),
            BotCommand("sonislem",      "Her cuzdanin son islemi"),
            BotCommand("bakiye",        "Tum cuzdan bakiyeleri"),
            BotCommand("cuzdanlar",     "Takip edilen cuzdanlar"),
            BotCommand("cuzdanekle",    "Yeni cuzdan ekle"),
            BotCommand("cuzdansil",     "Bir cuzdani takipten cikar"),
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
                "✅ <b>Cüzdan Takip Botu Başladı</b>\n"
                f"{DIVIDER}\n"
                f"🔍 Takip: {len(WALLETS)} cüzdan\n"
                f"⏱ Kontrol: {CHECK_INTERVAL_SECONDS} saniye\n"
                f"🌐 Nonlogs Reserves (BTC/GRIN/USDT): {NONLOGS_CHECK_INTERVAL_SECONDS} saniyede bir\n"
                f"📊 Günlük özet: {DAILY_REPORT_HOUR:02d}:{DAILY_REPORT_MINUTE:02d} UTC\n"
                f"{DIVIDER}\n"
                "➕ Yeni cüzdan eklemek için /cuzdanekle, komut listesi için /yardim yazabilirsin."
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
