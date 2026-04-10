"""
Telegram Cuzdan Takip Botu
- 2x BTC cüzdanı
- 1x Polygon USDT
- Anlık + Gerçek Pending işlem bildirimi (Alchemy WebSocket)
- Inline butonlar
- Günlük özet raporu
- /komutlar desteği
"""

import asyncio
import logging
import os
import json
import time
import websockets
import aiohttp
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot, Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

# ──────────────────────────────────────────────────────
# AYARLAR
# ──────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN      = "7879516551:AAFdjViydhgRvVgdW4Rq3UMxYds1H45OzLo"
TELEGRAM_CHAT_ID        = "6674836959"

WALLETS = {
    "bc1qx37z09wa8uw0r9s9rhkg24a9zl88p92qn8reqq": {
        "address": "bc1qx37z09wa8uw0r9s9rhkg24a9zl88p92qn8reqq",
        "network": "btc",
        "symbol":  "BTC",
    },
    "bc1qpxq2asrywmtvct6ecs2yawx97n4lymjl82ampk": {
        "address": "bc1qpxq2asrywmtvct6ecs2yawx97n4lymjl82ampk",
        "network": "btc",
        "symbol":  "BTC",
    },
    "0x51126d2EFD5bbD63A97b01B5e40464da1547962B": {
        "address": "0x51126d2EFD5bbD63A97b01B5e40464da1547962B",
        "network": "polygon",
        "symbol":  "USDT",
    },
}

USDT_CONTRACT           = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"
POLYGONSCAN_API_KEY     = os.getenv("POLYGONSCAN_API_KEY", "RGSD69N6JG2KM9IIMJME2G8W8Y9N6FX6JY")

# Alchemy API key — Polygon mempool (pending) için WebSocket
# Ücretsiz: https://dashboard.alchemy.com → yeni app → Polygon Mainnet
ALCHEMY_API_KEY         = os.getenv("ALCHEMY_API_KEY", "3JbUlCuAJFfubuinvxdvL")
ALCHEMY_WS_URL          = f"wss://polygon-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

DAILY_REPORT_HOUR       = 20     # UTC
DAILY_REPORT_MINUTE     = 0
CHECK_INTERVAL_SECONDS  = 30

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

def load_json(path: str) -> dict:
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}

def save_json(path: str, data: dict):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

seen_txs    = load_json(STATE_FILE)
pending_txs = load_json(PENDING_FILE)
daily_txs   = {name: [] for name in WALLETS}

# ──────────────────────────────────────────────────────
# YARDIMCI
# ──────────────────────────────────────────────────────
def e(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def ts_to_str(ts) -> str:
    if not ts or int(ts) == 0:
        return "Bilinmiyor"
    dt    = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    dt_tr = dt + timedelta(hours=3)
    return dt_tr.strftime("%d.%m.%Y %H:%M (TR)")

def now_str() -> str:
    dt = datetime.now(tz=timezone.utc) + timedelta(hours=3)
    return dt.strftime("%d.%m.%Y %H:%M (TR)")

# ──────────────────────────────────────────────────────
# INLINE KLAVYELER
# ──────────────────────────────────────────────────────
def btc_tx_keyboard(txid: str) -> InlineKeyboardMarkup:
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

def polygon_tx_keyboard(txhash: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔍 Polygonscan'da Gör", url=f"https://polygonscan.com/tx/{txhash}"),
        ],
        [
            InlineKeyboardButton("💼 Bakiyeler", callback_data="bakiye"),
            InlineKeyboardButton("📊 Rapor", callback_data="rapor"),
        ],
    ])

def main_menu_keyboard() -> InlineKeyboardMarkup:
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
# API FONKSİYONLARI
# ──────────────────────────────────────────────────────
async def fetch_btc_txs(address: str, session: aiohttp.ClientSession) -> list:
    url = f"https://blockstream.info/api/address/{address}/txs"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as ex:
        log.warning(f"BTC confirmed API hatasi: {ex}")
    return []

async def fetch_btc_mempool_txs(address: str, session: aiohttp.ClientSession) -> list:
    url = f"https://blockstream.info/api/address/{address}/txs/mempool"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as ex:
        log.warning(f"BTC mempool API hatasi: {ex}")
    return []

async def fetch_btc_address_info(address: str, session: aiohttp.ClientSession) -> dict:
    url = f"https://blockstream.info/api/address/{address}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as ex:
        log.warning(f"BTC address info hatasi: {ex}")
    return {}

# Etherscan V2 — Polygon (chainid=137)
async def fetch_polygon_confirmed(address: str, session: aiohttp.ClientSession, offset: int = 10) -> list:
    url = (
        f"https://api.etherscan.io/v2/api"
        f"?chainid=137"
        f"&module=account&action=tokentx"
        f"&contractaddress={USDT_CONTRACT}"
        f"&address={address}"
        f"&sort=desc&page=1&offset={offset}"
        f"&apikey={POLYGONSCAN_API_KEY}"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
            if data.get("status") == "1":
                return data.get("result", [])
    except Exception as ex:
        log.warning(f"Polygon confirmed API hatasi: {ex}")
    return []

async def fetch_polygon_usdt_balance(address: str, session: aiohttp.ClientSession) -> float:
    url = (
        f"https://api.etherscan.io/v2/api"
        f"?chainid=137"
        f"&module=account&action=tokenbalance"
        f"&contractaddress={USDT_CONTRACT}"
        f"&address={address}"
        f"&tag=latest"
        f"&apikey={POLYGONSCAN_API_KEY}"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
            if data.get("status") == "1":
                return int(data.get("result", 0)) / 1e6
    except Exception as ex:
        log.warning(f"Polygon balance API hatasi: {ex}")
    return 0.0

# ──────────────────────────────────────────────────────
# POLYGON PENDING — ALCHEMY WEBSOCKET (gerçek zamanlı)
# ──────────────────────────────────────────────────────
async def polygon_pending_listener(bot: Bot):
    """
    Alchemy WebSocket üzerinden Polygon mempool'u dinler.
    USDT transfer() çağrılarını filtreler; adrese gelen/giden
    pending TX'leri anında bildirir.
    """
    TRANSFER_METHOD = "0xa9059cbb"  # transfer(address,uint256)

    polygon_addr = None
    polygon_name = None
    for name, cfg in WALLETS.items():
        if cfg["network"] == "polygon":
            polygon_addr = cfg["address"].lower()
            polygon_name = name
            break

    if not polygon_addr:
        log.info("Polygon cüzdanı yok, WebSocket başlatılmıyor.")
        return

    while True:
        try:
            log.info("Alchemy WebSocket bağlanıyor...")
            async with websockets.connect(ALCHEMY_WS_URL, ping_interval=30) as ws:
                # USDT contract'ına giden tüm pending TX'leri dinle
                await ws.send(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["alchemy_pendingTransactions", {
                        "toAddress": USDT_CONTRACT,
                    }]
                }))
                log.info("Alchemy WebSocket aktif — Polygon pending TX dinleniyor...")

                async for raw_msg in ws:
                    try:
                        msg    = json.loads(raw_msg)
                        tx     = msg.get("params", {}).get("result", {})
                        if not tx:
                            continue

                        txhash = tx.get("hash", "")
                        input_ = tx.get("input", "")
                        from_  = tx.get("from", "").lower()

                        # Sadece transfer() çağrıları
                        if not input_.startswith(TRANSFER_METHOD) or len(input_) < 138:
                            continue

                        # input decode: recipient (son 20 byte of 32) + amount
                        recipient = "0x" + input_[34:74].lower()
                        amount    = int(input_[74:138], 16) / 1e6

                        is_incoming = (recipient == polygon_addr)
                        is_outgoing = (from_ == polygon_addr)

                        if not is_incoming and not is_outgoing:
                            continue
                        if txhash in seen_txs.get(polygon_name, []):
                            continue

                        direction   = "GIRIS" if is_incoming else "CIKIS"
                        dir_icon    = "📥" if is_incoming else "📤"
                        label       = "Gonderen" if is_incoming else "Alici"
                        counterpart = from_ if is_incoming else recipient

                        text = (
                            f"⚡ <b>Yeni USDT Islemi - PENDING!</b>\n"
                            f"👛 <b>Cuzdan:</b> {e(polygon_name)}\n"
                            "────────────────────────────\n"
                            f"{dir_icon} <b>Yon:</b> {direction}\n"
                            f"💰 <b>Miktar:</b> <code>{amount:.2f} USDT</code>\n"
                            f"👤 <b>{label}:</b> <code>{e(counterpart)}</code>\n"
                            f"📋 <b>Durum:</b> ⏳ PENDING (Mempool)\n"
                            f"🕐 <b>Zaman:</b> {now_str()}\n"
                            f"🔑 <b>TX:</b> <code>{e(txhash)}</code>"
                        )
                        await bot.send_message(
                            chat_id=TELEGRAM_CHAT_ID,
                            text=text,
                            parse_mode=ParseMode.HTML,
                            reply_markup=polygon_tx_keyboard(txhash),
                            disable_web_page_preview=True,
                        )
                        seen_txs.setdefault(polygon_name, []).append(txhash)
                        pending_txs[txhash] = {"wallet": polygon_name, "type": "polygon"}
                        daily_txs[polygon_name].append({
                            "txid": txhash,
                            "type": "polygon_usdt",
                            "raw": {"value": int(amount * 1e6), "to": recipient, "from": from_},
                            "address": polygon_addr,
                        })
                        save_json(STATE_FILE, seen_txs)
                        save_json(PENDING_FILE, pending_txs)
                        log.info(f"Polygon pending TX bildirildi: {txhash}")

                    except Exception as parse_ex:
                        log.warning(f"WS mesaj parse hatasi: {parse_ex}")

        except Exception as conn_ex:
            log.warning(f"Alchemy WS hatasi: {conn_ex} — 10sn sonra tekrar...")
            await asyncio.sleep(10)

# ──────────────────────────────────────────────────────
# MESAJ FORMATLAMA
# ──────────────────────────────────────────────────────
def format_btc_tx(wallet_name: str, address: str, tx: dict, is_pending: bool = False) -> str:
    txid        = tx["txid"]
    vout        = tx.get("vout", [])
    status      = tx.get("status", {})
    is_incoming = any(out.get("scriptpubkey_address") == address for out in vout)
    direction   = "GIRIS" if is_incoming else "CIKIS"
    dir_icon    = "📥" if is_incoming else "📤"
    amount_sat  = sum(out.get("value", 0) for out in vout if out.get("scriptpubkey_address") == address)
    amount_str  = f"{amount_sat / 1e8:.8f} BTC" if amount_sat else "?"
    fee_sat     = tx.get("fee", 0)
    fee_str     = f"{fee_sat / 1e8:.8f} BTC" if fee_sat else "?"
    block_time  = status.get("block_time")

    if is_pending:
        status_str = "⏳ PENDING (Mempool)"
        header     = "⚡ <b>Yeni BTC Islemi - PENDING!</b>"
        time_str   = now_str()
    else:
        confirmed  = status.get("confirmed", False)
        status_str = "Onaylandi ✅" if confirmed else "Bekliyor ⏳"
        header     = "🔔 <b>Yeni BTC Islemi!</b>"
        time_str   = ts_to_str(block_time)

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

def format_polygon_tx(wallet_name: str, address: str, tx: dict, is_pending: bool = False) -> str:
    txhash        = tx.get("hash", "")
    value         = int(tx.get("value", 0)) / 1e6
    from_addr     = tx.get("from", "")
    to_addr       = tx.get("to", "")
    is_incoming   = to_addr.lower() == address.lower()
    direction     = "GIRIS" if is_incoming else "CIKIS"
    dir_icon      = "📥" if is_incoming else "📤"
    label         = "Gonderen" if is_incoming else "Alici"
    counterpart   = from_addr if is_incoming else to_addr
    confirmations = int(tx.get("confirmations", 0))
    gas_gwei      = int(tx.get("gasPrice", 0)) / 1e9
    ts            = tx.get("timeStamp")

    if is_pending:
        status_str = "⏳ PENDING (0 onay)"
        header     = "⚡ <b>Yeni USDT Islemi - PENDING!</b>"
        time_str   = now_str()
    else:
        status_str = f"Onaylandi ✅ ({confirmations} onay)"
        header     = "🔔 <b>Yeni USDT Islemi!</b>"
        time_str   = ts_to_str(ts)

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

def format_confirmed_update(wallet_name: str, tx_type: str, txid: str, extra: str = "") -> str:
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
    async with aiohttp.ClientSession() as session:
        for name, cfg in WALLETS.items():
            address = cfg["address"]
            network = cfg["network"]
            ids     = []
            if network == "btc":
                txs     = await fetch_btc_txs(address, session)
                mempool = await fetch_btc_mempool_txs(address, session)
                ids     = [tx["txid"] for tx in txs[:20]] + [tx["txid"] for tx in mempool]
            elif network == "polygon":
                txs = await fetch_polygon_confirmed(address, session)
                ids = [tx["hash"] for tx in txs[:20]]

            if name not in seen_txs:
                seen_txs[name] = ids
            else:
                existing = set(seen_txs[name])
                seen_txs[name].extend([i for i in ids if i not in existing])
            log.info(f"  {name}: {len(ids)} eski islem isaretlendi")

    save_json(STATE_FILE, seen_txs)
    log.info("Snapshot tamamlandi.")

# ──────────────────────────────────────────────────────
# KONTROL DÖNGÜSÜ
# ──────────────────────────────────────────────────────
async def check_wallets(bot: Bot):
    global seen_txs, pending_txs
    async with aiohttp.ClientSession() as session:
        for name, cfg in WALLETS.items():
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
                        daily_txs[name].append({"txid": txid, "type": "btc", "raw": tx, "address": address})

                for tx in (await fetch_btc_txs(address, session))[:10]:
                    txid = tx["txid"]
                    if txid in pending_txs:
                        await bot.send_message(
                            chat_id=TELEGRAM_CHAT_ID,
                            text=format_confirmed_update(name, "btc", txid),
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
                        daily_txs[name].append({"txid": txid, "type": "btc", "raw": tx, "address": address})

                seen_txs[name] = seen_txs.get(name, [])[-100:]

            # ── POLYGON confirmed (pending WebSocket ile geliyor) ──
            elif network == "polygon":
                for tx in await fetch_polygon_confirmed(address, session):
                    txhash = tx.get("hash", "")
                    if not txhash:
                        continue

                    if txhash in pending_txs:
                        # Pending → Confirmed
                        await bot.send_message(
                            chat_id=TELEGRAM_CHAT_ID,
                            text=format_confirmed_update(
                                name, "polygon", txhash,
                                f"📋 <b>Onay:</b> {tx.get('confirmations','?')}\n"
                            ),
                            parse_mode=ParseMode.HTML,
                            reply_markup=polygon_tx_keyboard(txhash),
                            disable_web_page_preview=True,
                        )
                        del pending_txs[txhash]
                    elif txhash not in seen_txs.get(name, []):
                        # Bot kapalıyken gerçekleşmiş TX
                        await bot.send_message(
                            chat_id=TELEGRAM_CHAT_ID,
                            text=format_polygon_tx(name, address, tx, is_pending=False),
                            parse_mode=ParseMode.HTML,
                            reply_markup=polygon_tx_keyboard(txhash),
                            disable_web_page_preview=True,
                        )
                        seen_txs.setdefault(name, []).append(txhash)
                        daily_txs[name].append({"txid": txhash, "type": "polygon_usdt", "raw": tx, "address": address})

                seen_txs[name] = seen_txs.get(name, [])[-100:]

    save_json(STATE_FILE, seen_txs)
    save_json(PENDING_FILE, pending_txs)

# ──────────────────────────────────────────────────────
# VERİ TOPLAMA FONKSİYONLARI (komutlar + callback için ortak)
# ──────────────────────────────────────────────────────
async def _bakiye_data() -> str:
    lines = [f"💼 <b>Cuzdan Bakiyeleri</b>\n🕐 {now_str()}\n══════════════════════════════"]
    async with aiohttp.ClientSession() as session:
        for name, cfg in WALLETS.items():
            address = cfg["address"]
            network = cfg["network"]
            if network == "btc":
                info = await fetch_btc_address_info(address, session)
                if info:
                    funded  = info.get("chain_stats", {}).get("funded_txo_sum", 0)
                    spent   = info.get("chain_stats", {}).get("spent_txo_sum", 0)
                    balance = (funded - spent) / 1e8
                    mempool = info.get("mempool_stats", {})
                    unconf  = (mempool.get("funded_txo_sum", 0) - mempool.get("spent_txo_sum", 0)) / 1e8
                    lines.append(
                        f"\n👛 <b>{e(name)}</b>\n"
                        f"  💰 Bakiye: <code>{balance:.8f} BTC</code>\n"
                        f"  ⏳ Bekleyen: <code>{unconf:+.8f} BTC</code>\n"
                        f"  📍 <code>{e(address[:20])}...</code>"
                    )
                else:
                    lines.append(f"\n👛 <b>{e(name)}</b>\n  ❌ Bakiye alinamadi.")
            elif network == "polygon":
                balance = await fetch_polygon_usdt_balance(address, session)
                lines.append(
                    f"\n👛 <b>{e(name)}</b>\n"
                    f"  💰 Bakiye: <code>{balance:.2f} USDT</code>\n"
                    f"  📍 <code>{e(address[:20])}...</code>"
                )
    lines.append("\n══════════════════════════════")
    return "\n".join(lines)

async def _rapor_text() -> str:
    lines = [
        "📊 <b>Gunluk Ozet (Talep Uzerine)</b>",
        f"🕐 {now_str()}",
        "══════════════════════════════",
    ]
    has_data = False
    for name, cfg in WALLETS.items():
        txs = daily_txs.get(name, [])
        total_in = total_out = 0.0
        for entry in txs:
            raw, address = entry["raw"], entry["address"]
            if entry["type"] == "btc":
                vout  = raw.get("vout", [])
                val   = sum(o.get("value", 0) for o in vout if o.get("scriptpubkey_address") == address) / 1e8
                is_in = any(o.get("scriptpubkey_address") == address for o in vout)
            else:
                val   = int(raw.get("value", 0)) / 1e6
                is_in = raw.get("to", "").lower() == address.lower()
            if is_in: total_in  += val
            else:     total_out += val
        if txs:
            has_data = True
        lines.append(
            f"\n👛 <b>{e(name)}</b>\n"
            f"  📥 Giris: <code>{total_in:.6f} {cfg['symbol']}</code>\n"
            f"  📤 Cikis: <code>{total_out:.6f} {cfg['symbol']}</code>\n"
            f"  🔢 Islem: <code>{len(txs)} adet</code>"
        )
    if not has_data:
        lines.append("\n✨ Bugun hic islem gerceklesmedi.")
    lines += ["", "══════════════════════════════"]
    return "\n".join(lines)

async def _sonislem_data():
    lines = [f"🔎 <b>Son Islemler</b>\n🕐 {now_str()}\n══════════════════════════════"]
    last_txhash  = None
    last_txid    = None
    last_network = None

    async with aiohttp.ClientSession() as session:
        for name, cfg in WALLETS.items():
            address = cfg["address"]
            network = cfg["network"]
            lines.append(f"\n👛 <b>{e(name)}</b>")

            if network == "btc":
                txs = await fetch_btc_txs(address, session)
                if txs:
                    tx        = txs[0]
                    txid      = tx["txid"]
                    vout      = tx.get("vout", [])
                    status    = tx.get("status", {})
                    confirmed = status.get("confirmed", False)
                    is_in     = any(o.get("scriptpubkey_address") == address for o in vout)
                    amount    = sum(o.get("value", 0) for o in vout if o.get("scriptpubkey_address") == address) / 1e8
                    time_str  = ts_to_str(status.get("block_time"))
                    icon      = "📥" if is_in else "📤"
                    conf_str  = "✅ Onayli" if confirmed else "⏳ Pending"
                    lines.append(
                        f"  {icon} <code>{amount:.8f} BTC</code>\n"
                        f"  📋 {conf_str}\n"
                        f"  🕐 {time_str}\n"
                        f'  <a href="https://blockstream.info/tx/{txid}">TX Goruntule</a>'
                    )
                    last_txid    = txid
                    last_network = "btc"
                else:
                    lines.append("  Hic islem bulunamadi.")

            elif network == "polygon":
                txs = await fetch_polygon_confirmed(address, session, offset=1)
                if txs:
                    tx      = txs[0]
                    txhash  = tx.get("hash", "")
                    value   = int(tx.get("value", 0)) / 1e6
                    is_in   = tx.get("to", "").lower() == address.lower()
                    icon    = "📥" if is_in else "📤"
                    confs   = tx.get("confirmations", "?")
                    lines.append(
                        f"  {icon} <code>{value:.2f} USDT</code>\n"
                        f"  📋 {confs} onay ✅\n"
                        f"  🕐 {ts_to_str(tx.get('timeStamp'))}\n"
                        f'  <a href="https://polygonscan.com/tx/{txhash}">TX Goruntule</a>'
                    )
                    last_txhash  = txhash
                    last_network = "polygon"
                else:
                    lines.append("  Hic islem bulunamadi.")

    lines.append("\n══════════════════════════════")

    if last_network == "btc" and last_txid:
        keyboard = btc_tx_keyboard(last_txid)
    elif last_network == "polygon" and last_txhash:
        keyboard = polygon_tx_keyboard(last_txhash)
    else:
        keyboard = main_menu_keyboard()

    return "\n".join(lines), keyboard

def _bekleyenler_data():
    if not pending_txs:
        return "✅ <b>Bekleyen islem yok.</b>\nTum islemler onaylandi.", main_menu_keyboard()
    lines = [f"⏳ <b>Bekleyen Islemler</b>\n🕐 {now_str()}\n══════════════════════════════"]
    for txid, info in pending_txs.items():
        wallet = info.get("wallet", "?")
        typ    = info.get("type", "?")
        link   = (f'<a href="https://mempool.space/tx/{txid}">Mempool.space</a>'
                  if typ == "btc" else
                  f'<a href="https://polygonscan.com/tx/{txid}">Polygonscan</a>')
        lines.append(
            f"\n👛 <b>{e(wallet)}</b>\n"
            f"  🔑 <code>{e(txid[:30])}...</code>\n"
            f"  {link}"
        )
    lines.append("\n══════════════════════════════")
    return "\n".join(lines), main_menu_keyboard()

def _sistemkontrol_text() -> str:
    uptime_sec = int(time.time() - BOT_START_TIME)
    hours, rem = divmod(uptime_sec, 3600)
    mins,  sec = divmod(rem, 60)
    lines = [
        "🖥️ <b>Sistem Kontrol</b>",
        f"🕐 {now_str()}",
        "══════════════════════════════",
        f"⏱ <b>Uptime:</b> <code>{hours}s {mins}dk {sec}sn</code>",
        f"🔄 <b>BTC kontrol:</b> <code>{CHECK_INTERVAL_SECONDS} saniye</code>",
        f"⚡ <b>Polygon pending:</b> <code>Alchemy WebSocket</code>",
        f"📊 <b>Gunluk ozet:</b> <code>{DAILY_REPORT_HOUR:02d}:{DAILY_REPORT_MINUTE:02d} UTC</code>",
        "",
        f"👛 <b>Takip edilen cuzdan:</b> <code>{len(WALLETS)}</code>",
        f"📝 <b>Gorulmus TX:</b> <code>{sum(len(v) for v in seen_txs.values())}</code>",
        f"⏳ <b>Bekleyen TX:</b> <code>{len(pending_txs)}</code>",
        f"📈 <b>Bugunun islemi:</b> <code>{sum(len(v) for v in daily_txs.values())}</code>",
        "",
    ]
    for name, cfg in WALLETS.items():
        net = "BTC" if cfg["network"] == "btc" else "Polygon"
        lines.append(f"  ✅ {e(name)} ({net})")
    lines += ["══════════════════════════════", "<i>Tum sistemler calisiyor.</i>"]
    return "\n".join(lines)

# ──────────────────────────────────────────────────────
# KOMUT HANDLERLARI
# ──────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 <b>Cuzdan Takip Botu</b>\n\n"
        "Asagidaki butonlari veya komutlari kullanabilirsin:\n\n"
        "/rapor — Bugunun ozet raporu\n"
        "/sonislem — Her cüzdanın son islemi\n"
        "/bakiye — Tum cüzdan bakiyeleri\n"
        "/saat — Simdi saat kac (TR)\n"
        "/bekleyenler — Pending islemler\n"
        "/sistemkontrol — Bot durumu\n"
        "/yardim — Bu mesaj"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def cmd_yardim(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, ctx)

async def cmd_saat(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    utc_now = datetime.now(tz=timezone.utc)
    tr_now  = utc_now + timedelta(hours=3)
    await update.message.reply_text(
        f"🕐 <b>Simdiki Saat</b>\n\n"
        f"🇹🇷 <b>Turkiye (UTC+3):</b> <code>{tr_now.strftime('%d.%m.%Y %H:%M:%S')}</code>\n"
        f"🌍 <b>UTC:</b> <code>{utc_now.strftime('%d.%m.%Y %H:%M:%S')}</code>",
        parse_mode=ParseMode.HTML,
    )

async def cmd_rapor(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = await _rapor_text()
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def cmd_sonislem(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔄 Veriler cekiliyor...", parse_mode=ParseMode.HTML)
    text, keyboard = await _sonislem_data()
    await msg.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard, disable_web_page_preview=True)

async def cmd_bakiye(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔄 Bakiyeler cekiliyor...", parse_mode=ParseMode.HTML)
    text = await _bakiye_data()
    await msg.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def cmd_bekleyenler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text, keyboard = _bekleyenler_data()
    await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                    reply_markup=keyboard, disable_web_page_preview=True)

async def cmd_sistemkontrol(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        _sistemkontrol_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard()
    )

# ──────────────────────────────────────────────────────
# CALLBACK QUERY HANDLER
# ──────────────────────────────────────────────────────
async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data

    if data == "bakiye":
        await query.edit_message_text("🔄 Bakiyeler cekiliyor...", parse_mode=ParseMode.HTML)
        text = await _bakiye_data()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

    elif data == "rapor":
        text = await _rapor_text()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

    elif data == "sonislem":
        await query.edit_message_text("🔄 Veriler cekiliyor...", parse_mode=ParseMode.HTML)
        text, keyboard = await _sonislem_data()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                      reply_markup=keyboard, disable_web_page_preview=True)

    elif data == "bekleyenler":
        text, keyboard = _bekleyenler_data()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                      reply_markup=keyboard, disable_web_page_preview=True)

    elif data == "sistemkontrol":
        await query.edit_message_text(
            _sistemkontrol_text(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard()
        )

# ──────────────────────────────────────────────────────
# GÜNLÜK ÖZET
# ──────────────────────────────────────────────────────
async def send_daily_report(bot: Bot):
    lines = ["📊 <b>Gunluk Ozet Raporu</b>", f"🕐 {now_str()}", "══════════════════════════════"]
    has_data = False
    for name, cfg in WALLETS.items():
        txs = daily_txs.get(name, [])
        total_in = total_out = 0.0
        for entry in txs:
            raw, address = entry["raw"], entry["address"]
            if entry["type"] == "btc":
                vout  = raw.get("vout", [])
                val   = sum(o.get("value", 0) for o in vout if o.get("scriptpubkey_address") == address) / 1e8
                is_in = any(o.get("scriptpubkey_address") == address for o in vout)
            else:
                val   = int(raw.get("value", 0)) / 1e6
                is_in = raw.get("to", "").lower() == address.lower()
            if is_in: total_in  += val
            else:     total_out += val
        if txs:
            has_data = True
        lines.append(
            f"\n👛 <b>{e(name)}</b>\n"
            f"  📥 Giris: <code>{total_in:.6f} {cfg['symbol']}</code>\n"
            f"  📤 Cikis: <code>{total_out:.6f} {cfg['symbol']}</code>\n"
            f"  🔢 Islem: <code>{len(txs)} adet</code>"
        )
    if not has_data:
        lines.append("\n✨ Bugun hic islem gerceklesmedi.")
    lines += ["", "══════════════════════════════", "<i>Otomatik rapor.</i>"]
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text="\n".join(lines),
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
    await initialize_snapshots()

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
    scheduler.add_job(
        check_wallets, "interval",
        seconds=CHECK_INTERVAL_SECONDS,
        args=[app.bot],
        id="check_wallets",
        max_instances=1,
    )
    scheduler.add_job(
        send_daily_report, "cron",
        hour=DAILY_REPORT_HOUR,
        minute=DAILY_REPORT_MINUTE,
        args=[app.bot],
        id="daily_report",
    )
    scheduler.start()

    wallet_list = "\n".join(f"  - {e(n)}" for n in WALLETS)
    await app.bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=(
            "✅ <b>Cuzdan Takip Botu Basladi!</b>\n\n"
            f"🔍 Takip edilen cüzdanlar:\n{wallet_list}\n\n"
            f"⏱ BTC kontrol: {CHECK_INTERVAL_SECONDS} saniye\n"
            f"⚡ Polygon pending: Alchemy WebSocket (gercek zamanli)\n"
            f"📊 Gunluk ozet: {DAILY_REPORT_HOUR:02d}:{DAILY_REPORT_MINUTE:02d} UTC\n\n"
            "Komutlar icin /yardim yaz."
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(),
    )

    log.info("Bot calisiyor. Polling basliyor...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    # Alchemy WebSocket arka planda çalışır
    asyncio.create_task(polygon_pending_listener(app.bot))

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        log.info("Bot durduruldu.")
        scheduler.shutdown()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())