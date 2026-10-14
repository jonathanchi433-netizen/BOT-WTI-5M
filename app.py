import csv
import hashlib
import hmac
import io
import json
import logging
import math
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import requests
from flask import (
    Flask,
    jsonify,
    redirect,
    render_template_string,
    request,
    send_file,
)

# ============================================================
# BOT PETRÓLEO WTI 3M - BINGX
# OPERACIONES REALES
#
# SEÑALES:
# LONG  = ABRIR COMPRA
# SHORT = ABRIR VENTA
# XL    = CERRAR COMPRA
# XS    = CERRAR VENTA
#
# NO HAY REVERSIÓN AUTOMÁTICA.
# NO HAY SIMULACIÓN.
# OFF IMPIDE NUEVAS ENTRADAS.
# ============================================================

BOT_NAME = "BOT PETRÓLEO WTI 3M"
BINGX_BASE_URL = "https://open-api.bingx.com"

BINGX_SYMBOL = os.getenv(
    "BINGX_SYMBOL",
    "NCCO1OILWTI2USD-USDT",
).strip().upper()

TV_SYMBOLS = {
    item.strip().upper()
    for item in os.getenv(
        "TV_SYMBOLS",
        "OILWTIUSDT.P,BINGX:OILWTIUSDT.P,"
        "OILWTI-USDT,OILWTIUSDT",
    ).split(",")
    if item.strip()
}

BINGX_API_KEY = os.getenv(
    "BINGX_API_KEY", ""
).strip()

BINGX_API_SECRET = (
    os.getenv("BINGX_API_SECRET", "").strip()
    or os.getenv("BINGX_SECRET_KEY", "").strip()
)

WEBHOOK_SECRET = os.getenv(
    "WEBHOOK_SECRET", ""
).strip()

CONTROL_SECRET = os.getenv(
    "CONTROL_SECRET",
    WEBHOOK_SECRET,
).strip()

MONITOR_SECRET = os.getenv(
    "MONITOR_SECRET", ""
).strip()

BALANCE_PERCENT = float(
    os.getenv("BALANCE_PERCENT", "90")
)

LEVERAGE = int(
    os.getenv("LEVERAGE", "2")
)

FEE_RATE = float(
    os.getenv("FEE_RATE", "0.0005")
)

POSITION_MODE = os.getenv(
    "POSITION_MODE", "HEDGE"
).strip().upper()

# Historial independiente de Ethereum.
STATE_PREFIX = "bot_petroleo_wti_3m_v1"

DATA_DIR = os.getenv(
    "DATA_DIR", "."
).strip()

UPSTASH_REDIS_REST_URL = os.getenv(
    "UPSTASH_REDIS_REST_URL", ""
).strip()

UPSTASH_REDIS_REST_TOKEN = os.getenv(
    "UPSTASH_REDIS_REST_TOKEN", ""
).strip()

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN", ""
).strip()

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID", ""
).strip()

VALID_MODES = [
    "OFF",
    "LONG_ONLY",
    "SHORT_ONLY",
    "CLOSE_ONLY",
    "BOTH",
]

SIGNAL_LOCK = threading.RLock()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(BOT_NAME)

app = Flask(__name__)


# ============================================================
# UTILIDADES
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def fnum(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def fint(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def secret_matches(received, expected):
    return (
        bool(received and expected)
        and hmac.compare_digest(
            str(received),
            str(expected),
        )
    )


def redact_error(message):
    result = str(message)

    for secret in [
        BINGX_API_KEY,
        BINGX_API_SECRET,
        WEBHOOK_SECRET,
        CONTROL_SECRET,
        MONITOR_SECRET,
        UPSTASH_REDIS_REST_TOKEN,
    ]:
        if secret:
            result = result.replace(secret, "***")

    return result


def floor_step(value, step):
    value = Decimal(str(value))
    step = Decimal(str(step))

    if step <= 0:
        raise ValueError("Paso de cantidad inválido")

    units = (value / step).to_integral_value(
        rounding=ROUND_DOWN
    )

    return float(units * step)


def notify(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    try:
        requests.post(
            (
                "https://api.telegram.org/bot"
                f"{TELEGRAM_BOT_TOKEN}/sendMessage"
            ),
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": str(message)[:3900],
            },
            timeout=10,
        )
    except Exception:
        logger.exception("Error enviando Telegram")


# ============================================================
# ALMACENAMIENTO
# ============================================================

class Store:

    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.lock = threading.RLock()

    @property
    def redis_enabled(self):
        return bool(
            UPSTASH_REDIS_REST_URL
            and UPSTASH_REDIS_REST_TOKEN
        )

    def _key(self, name):
        return f"{STATE_PREFIX}:{name}"

    def _path(self, name):
        return os.path.join(
            DATA_DIR,
            f"{STATE_PREFIX}_{name}.json",
        )

    def _redis(self, command):
        response = requests.post(
            UPSTASH_REDIS_REST_URL.rstrip("/"),
            headers={
                "Authorization": (
                    f"Bearer {UPSTASH_REDIS_REST_TOKEN}"
                ),
                "Content-Type": "application/json",
            },
            json=command,
            timeout=12,
        )

        response.raise_for_status()
        payload = response.json()

        if payload.get("error"):
            raise RuntimeError(
                f"Error Upstash: {payload['error']}"
            )

        return payload.get("result")

    def get(self, name, default):
        with self.lock:
            if self.redis_enabled:
                raw = self._redis([
                    "GET",
                    self._key(name),
                ])
                return json.loads(raw) if raw else default

            path = self._path(name)

            if not os.path.exists(path):
                return default

            with open(path, "r", encoding="utf-8") as file:
                return json.load(file)

    def set(self, name, value):
        with self.lock:
            raw = json.dumps(
                value,
                ensure_ascii=False,
            )

            if self.redis_enabled:
                self._redis([
                    "SET",
                    self._key(name),
                    raw,
                ])
                return

            path = self._path(name)
            temp = f"{path}.tmp"

            with open(temp, "w", encoding="utf-8") as file:
                json.dump(
                    value,
                    file,
                    ensure_ascii=False,
                    indent=2,
                )

            os.replace(temp, path)

    def get_mode(self):
        data = self.get("mode", {})
        mode = data.get("mode", "OFF")
        return mode if mode in VALID_MODES else "OFF"

    def set_mode(self, mode):
        if mode not in VALID_MODES:
            raise ValueError("Modo inválido")

        self.set("mode", {
            "mode": mode,
            "updated_at": utc_now(),
        })

    def get_active_trade(self):
        return self.get("active_trade", None)

    def set_active_trade(self, trade):
        self.set("active_trade", trade)

    def get_trades(self):
        data = self.get("trades", [])
        return data if isinstance(data, list) else []

    def set_trades(self, trades):
        self.set("trades", trades)

    def append_trade(self, trade):
        with self.lock:
            trades = self.get_trades()
            trades.append(trade)
            self.set_trades(trades)

    def get_events(self):
        data = self.get("events", [])
        return data if isinstance(data, list) else []

    def add_event(self, kind, detail):
        with self.lock:
            events = self.get_events()

            events.append({
                "time": utc_now(),
                "kind": str(kind),
                "detail": redact_error(detail),
            })

            self.set("events", events[-150:])

    def signal_seen(self, signal_id):
        ids = self.get("signal_ids", [])
        return signal_id in ids

    def remember_signal(self, signal_id):
        with self.lock:
            ids = self.get("signal_ids", [])

            if signal_id not in ids:
                ids.append(signal_id)

            self.set("signal_ids", ids[-500:])


store = Store()


# ============================================================
# API BINGX
# ============================================================

class BingX:

    def _request(
        self,
        method,
        path,
        params=None,
        private=True,
    ):
        params = dict(params or {})
        headers = {}

        if private:
            if not BINGX_API_KEY or not BINGX_API_SECRET:
                raise RuntimeError(
                    "Faltan las claves API de BingX"
                )

            params["timestamp"] = int(time.time() * 1000)
            params.setdefault("recvWindow", 5000)

            query = urlencode(sorted(params.items()))

            signature = hmac.new(
                BINGX_API_SECRET.encode(),
                query.encode(),
                hashlib.sha256,
            ).hexdigest()

            url = (
                f"{BINGX_BASE_URL}{path}"
                f"?{query}&signature={signature}"
            )

            headers["X-BX-APIKEY"] = BINGX_API_KEY

        else:
            query = urlencode(params)
            url = f"{BINGX_BASE_URL}{path}"

            if query:
                url += f"?{query}"

        response = requests.request(
            method,
            url,
            headers=headers,
            timeout=20,
        )

        try:
            payload = response.json()
        except Exception:
            raise RuntimeError(
                f"Respuesta no JSON de BingX: "
                f"HTTP {response.status_code}"
            )

        if (
            response.status_code >= 400
            or str(payload.get("code")) != "0"
        ):
            raise RuntimeError(
                f"Error BingX: {payload}"
            )

        return payload

    # --------------------------------------------------------
    # PRECIO
    # --------------------------------------------------------

    def price(self):
        payload = self._request(
            "GET",
            "/openApi/swap/v2/quote/price",
            {"symbol": BINGX_SYMBOL},
            private=False,
        )

        data = payload.get("data", {})

        if isinstance(data, list):
            data = data[0] if data else {}

        price = fnum(
            data.get("price")
            or data.get("lastPrice")
        )

        if price <= 0:
            raise RuntimeError(
                "No se pudo obtener el precio WTI"
            )

        return price

    # --------------------------------------------------------
    # BALANCE
    # --------------------------------------------------------

    def available_balance(self):
        payload = self._request(
            "GET",
            "/openApi/swap/v3/user/balance",
        )

        data = payload.get("data", {})
        candidates = []

        if isinstance(data, dict):
            for key in ["balance", "balances"]:
                nested = data.get(key)

                if isinstance(nested, dict):
                    candidates.append(nested)

                elif isinstance(nested, list):
                    candidates.extend(nested)

            candidates.append(data)

        elif isinstance(data, list):
            candidates.extend(data)

        for item in candidates:
            if not isinstance(item, dict):
                continue

            asset = str(
                item.get("asset")
                or item.get("currency")
                or item.get("coin")
                or ""
            ).upper()

            if asset and asset != "USDT":
                continue

            for key in [
                "availableMargin",
                "availableBalance",
                "available",
            ]:
                if key in item:
                    value = fnum(item[key], -1)

                    if value >= 0:
                        return value

        raise RuntimeError(
            "No se pudo leer el balance USDT disponible"
        )

    # --------------------------------------------------------
    # REGLAS DEL CONTRATO
    # --------------------------------------------------------

    def contract_rules(self):
        payload = self._request(
            "GET",
            "/openApi/swap/v2/quote/contracts",
            private=False,
        )

        data = payload.get("data", [])

        if isinstance(data, dict):
            data = (
                data.get("contracts")
                or data.get("data")
                or [data]
            )

        for item in data:
            if not isinstance(item, dict):
                continue

            if (
                str(item.get("symbol", "")).upper()
                != BINGX_SYMBOL
            ):
                continue

            precision = item.get("quantityPrecision")

            if precision is None:
                raise RuntimeError(
                    "El contrato no informa quantityPrecision"
                )

            step = Decimal(1).scaleb(-int(precision))

            minimum = fnum(
                item.get("tradeMinQuantity")
                or item.get("minQty")
                or item.get("minQuantity")
            )

            if minimum <= 0:
                raise RuntimeError(
                    "El contrato no informa cantidad mínima"
                )

            return {
                "symbol": BINGX_SYMBOL,
                "step": float(step),
                "minimum": minimum,
                "quantity_precision": int(precision),
                "min_notional": fnum(
                    item.get("tradeMinUSDT")
                    or item.get("minNotional")
                    or item.get("minTradeUSDT")
                ),
                "size": item.get("size"),
                "status": item.get("status"),
                "api_state": item.get("apiState"),
                "raw": item,
            }

        raise RuntimeError(
            f"Contrato {BINGX_SYMBOL} no encontrado "
            "en BingX"
        )

    # --------------------------------------------------------
    # POSICIONES REALES
    # --------------------------------------------------------

    def positions(self):
        payload = self._request(
            "GET",
            "/openApi/swap/v2/user/positions",
            {"symbol": BINGX_SYMBOL},
        )

        data = payload.get("data", [])

        if isinstance(data, dict):
            data = [data]

        result = []

        for item in data or []:
            if not isinstance(item, dict):
                continue

            symbol = str(
                item.get("symbol", "")
            ).upper()

            if symbol and symbol != BINGX_SYMBOL:
                continue

            amount = fnum(
                item.get("positionAmt")
                if item.get("positionAmt") is not None
                else item.get("positionAmount")
                if item.get("positionAmount") is not None
                else item.get("availableAmt")
                if item.get("availableAmt") is not None
                else item.get("quantity")
            )

            if abs(amount) <= 0:
                continue

            side = str(
                item.get("positionSide", "")
            ).upper()

            if side not in {"LONG", "SHORT"}:
                side = "LONG" if amount > 0 else "SHORT"

            result.append({
                "side": side,
                "quantity": abs(amount),
                "entry_price": fnum(
                    item.get("avgPrice")
                    or item.get("entryPrice")
                ),
                "leverage": fint(
                    item.get("leverage"),
                    LEVERAGE,
                ),
                "margin_used": fnum(
                    item.get("initialMargin")
                    or item.get("isolatedMargin")
                    or item.get("margin")
                ),
                "unrealized_pnl": fnum(
                    item.get("unrealizedProfit")
                    or item.get("unrealizedPnl")
                ),
                "liquidation_price": fnum(
                    item.get("liquidationPrice")
                ),
            })

        return result

    # --------------------------------------------------------
    # HEDGE
    # --------------------------------------------------------

    def position_mode_is_hedge(self):
        payload = self._request(
            "GET",
            "/openApi/swap/v1/positionSide/dual",
        )

        data = payload.get("data", {})

        raw = (
            data.get("dualSidePosition")
            if isinstance(data, dict)
            else data
        )

        if isinstance(raw, bool):
            return raw

        return str(raw).lower() == "true"

    def ensure_position_mode(self):
        if POSITION_MODE != "HEDGE":
            raise RuntimeError(
                "Este bot requiere POSITION_MODE=HEDGE"
            )

        if self.position_mode_is_hedge():
            return

        self._request(
            "POST",
            "/openApi/swap/v1/positionSide/dual",
            {"dualSidePosition": "true"},
        )

    # --------------------------------------------------------
    # MARGEN
    # --------------------------------------------------------

    def margin_type(self):
        payload = self._request(
            "GET",
            "/openApi/swap/v2/trade/marginType",
            {"symbol": BINGX_SYMBOL},
        )

        data = payload.get("data", {})

        return str(
            data.get("marginType", "")
            if isinstance(data, dict)
            else ""
        ).upper()

    def set_isolated(self):
        if self.margin_type() == "ISOLATED":
            return

        self._request(
            "POST",
            "/openApi/swap/v2/trade/marginType",
            {
                "symbol": BINGX_SYMBOL,
                "marginType": "ISOLATED",
            },
        )

    # --------------------------------------------------------
    # APALANCAMIENTO
    # --------------------------------------------------------

    def set_leverage(self):
        for side in ["LONG", "SHORT"]:
            self._request(
                "POST",
                "/openApi/swap/v2/trade/leverage",
                {
                    "symbol": BINGX_SYMBOL,
                    "side": side,
                    "leverage": LEVERAGE,
                },
            )

    # --------------------------------------------------------
    # CÁLCULO DE CANTIDAD
    # --------------------------------------------------------

    def quantity(self):
        balance = self.available_balance()
        price = self.price()
        rules = self.contract_rules()

        margin = balance * BALANCE_PERCENT / 100
        notional = margin * LEVERAGE

        if margin <= 0:
            raise RuntimeError(
                "Balance insuficiente"
            )

        # Para contratos lineales cuya cantidad representa
        # unidades del activo subyacente.
        #
        # La API debe confirmar que el contrato utiliza
        # esa convención antes de confiar en el resultado.
        #
        # Si BingX devuelve una unidad contractual distinta,
        # el cálculo debe ajustarse a esa especificación.

        quantity = floor_step(
            notional / price,
            rules["step"],
        )

        if quantity < rules["minimum"]:
            raise RuntimeError(
                f"Cantidad {quantity} inferior al mínimo "
                f"{rules['minimum']}. "
                f"Balance: {balance:.4f} USDT"
            )

        if (
            rules["min_notional"] > 0
            and quantity * price < rules["min_notional"]
        ):
            raise RuntimeError(
                "La orden no alcanza el nocional mínimo"
            )

        return {
            "balance": balance,
            "price": price,
            "margin": margin,
            "notional": notional,
            "quantity": quantity,
            "rules": rules,
        }

    # --------------------------------------------------------
    # ORDEN MARKET REAL
    # --------------------------------------------------------

    def market_order(
        self,
        order_side,
        direction,
        quantity,
    ):
        rules = self.contract_rules()

        quantity = floor_step(
            quantity,
            rules["step"],
        )

        if quantity <= 0:
            raise RuntimeError(
                "La cantidad de la orden es cero"
            )

        payload = self._request(
            "POST",
            "/openApi/swap/v2/trade/order",
            {
                "symbol": BINGX_SYMBOL,
                "side": order_side,
                "positionSide": direction,
                "type": "MARKET",
                "quantity": quantity,
            },
        )

        data = payload.get("data", {})

        if isinstance(data, dict):
            return data.get("order", data)

        return {}


bingx = BingX()


# ============================================================
# SINCRONIZACIÓN DE POSICIONES
# ============================================================

def sync_position():
    positions = bingx.positions()
    stored = store.get_active_trade()

    if len(positions) > 1:
        return {
            "status": "warning",
            "reason": "multiple_positions",
            "positions": positions,
        }

    if not positions:
        if stored:
            store.set_active_trade(None)

        return {
            "status": "flat",
            "position": None,
        }

    live = positions[0]

    if not stored:
        stored = {
            "id": str(uuid.uuid4()),
            "opened_at": utc_now(),
            "side": live["side"],
            "symbol": BINGX_SYMBOL,
            "quantity": live["quantity"],
            "entry_price": live["entry_price"],
            "leverage": live["leverage"],
            "margin_used": live["margin_used"],
            "source": "MANUAL_SYNC",
        }

        store.add_event(
            "MANUAL_SYNC",
            f"Posición {live['side']} adoptada",
        )

    else:
        for key in [
            "side",
            "quantity",
            "entry_price",
            "leverage",
            "margin_used",
        ]:
            stored[key] = live[key]

    store.set_active_trade(stored)

    return {
        "status": "synced",
        "position": {
            **stored,
            **live,
        },
    }


def wait_for_position(
    direction=None,
    timeout=8,
):
    deadline = time.time() + timeout

    while time.time() < deadline:
        positions = bingx.positions()

        if direction is None:
            if not positions:
                return None

        else:
            for position in positions:
                if position["side"] == direction:
                    return position

        time.sleep(0.4)

    return False


# ============================================================
# APERTURA REAL
# ============================================================

def open_trade(direction):
    sync = sync_position()

    if sync.get("reason") == "multiple_positions":
        return {
            "status": "blocked",
            "reason": "multiple_positions",
        }

    if sync.get("position"):
        return {
            "status": "ignored",
            "reason": "position_already_open",
        }

    calculation = bingx.quantity()

    bingx.ensure_position_mode()
    bingx.set_isolated()
    bingx.set_leverage()

    order_side = (
        "BUY" if direction == "LONG" else "SELL"
    )

    order = bingx.market_order(
        order_side,
        direction,
        calculation["quantity"],
    )

    live = wait_for_position(direction)

    if not live:
        store.add_event(
            "OPEN_UNCONFIRMED",
            f"{direction} enviado; posición sin confirmar",
        )

        raise RuntimeError(
            "Orden enviada pero posición no confirmada. "
            "Revisar BingX antes de reintentar."
        )

    trade = {
        "id": str(uuid.uuid4()),
        "opened_at": utc_now(),
        "side": direction,
        "symbol": BINGX_SYMBOL,
        "quantity": live["quantity"],
        "entry_price": live["entry_price"],
        "leverage": live["leverage"],
        "margin_used": (
            live["margin_used"]
            or calculation["margin"]
        ),
        "balance_before": calculation["balance"],
        "open_order_id": order.get("orderId"),
        "source": "AUTO",
    }

    store.set_active_trade(trade)

    store.add_event(
        "ORDER_OPENED",
        (
            f"{direction} qty={live['quantity']} "
            f"price={live['entry_price']}"
        ),
    )

    notify(
        f"{BOT_NAME}\n"
        f"APERTURA {direction}\n"
        f"Precio: {live['entry_price']}\n"
        f"Cantidad: {live['quantity']}\n"
        f"Margen: ISOLATED\n"
        f"Apalancamiento: {LEVERAGE}x"
    )

    return {
        "status": "opened",
        "trade": trade,
    }


# ============================================================
# CIERRE REAL
# ============================================================

def close_trade(expected_direction, reason):
    sync = sync_position()

    if sync.get("reason") == "multiple_positions":
        return {
            "status": "blocked",
            "reason": "multiple_positions",
        }

    live = sync.get("position")

    if not live:
        return {
            "status": "ignored",
            "reason": "no_position",
        }

    if live["side"] != expected_direction:
        return {
            "status": "ignored",
            "reason": "wrong_position_side",
        }

    saved = store.get_active_trade() or {}

    direction = live["side"]

    order_side = (
        "SELL" if direction == "LONG" else "BUY"
    )

    order = bingx.market_order(
        order_side,
        direction,
        live["quantity"],
    )

    flat = wait_for_position(None)

    if flat is False:
        store.add_event(
            "CLOSE_UNCONFIRMED",
            (
                f"{direction} cierre enviado; "
                "BingX aún muestra posición"
            ),
        )

        raise RuntimeError(
            "Cierre enviado pero BingX no confirmó FLAT"
        )

    # El cierre se registra sin inventar un PnL.
    # El PnL exacto requiere consultar fills,
    # comisiones y funding de BingX.

    closed = {
        "id": saved.get("id") or str(uuid.uuid4()),
        "opened_at": saved.get("opened_at", utc_now()),
        "closed_at": utc_now(),
        "side": direction,
        "symbol": BINGX_SYMBOL,
        "entry_price": live["entry_price"],
        "exit_price": 0,
        "quantity": live["quantity"],
        "leverage": live["leverage"],
        "margin_used": live["margin_used"],
        "gross_pnl": 0,
        "total_fees": 0,
        "funding": 0,
        "net_pnl": 0,
        "pnl_pending": True,
        "risk_usd": 0,
        "r_multiple": 0,
        "price_move_pct": 0,
        "source": saved.get("source", "AUTO"),
        "close_reason": reason,
        "close_order_id": order.get("orderId"),
    }

    store.append_trade(closed)
    store.set_active_trade(None)

    store.add_event(
        "ORDER_CLOSED",
        f"{direction} cerrado por {reason}",
    )

    notify(
        f"{BOT_NAME}\n"
        f"CIERRE {direction}\n"
        f"Motivo: {reason}\n"
        "PnL: pendiente de conciliación"
    )

    return {
        "status": "closed",
        "trade": closed,
    }


# ============================================================
# LÓGICA DE LAS CUATRO SEÑALES
# ============================================================

def process_signal(action):
    with SIGNAL_LOCK:
        mode = store.get_mode()

        if mode == "OFF":
            return {
                "status": "ignored",
                "reason": "mode_off",
            }

        if action == "LONG":
            if mode not in {"LONG_ONLY", "BOTH"}:
                return {
                    "status": "ignored",
                    "reason": "long_not_allowed",
                }

            return open_trade("LONG")

        if action == "SHORT":
            if mode not in {"SHORT_ONLY", "BOTH"}:
                return {
                    "status": "ignored",
                    "reason": "short_not_allowed",
                }

            return open_trade("SHORT")

        if action == "XL":
            return close_trade(
                "LONG",
                "tradingview_XL_3m",
            )

        if action == "XS":
            return close_trade(
                "SHORT",
                "tradingview_XS_3m",
            )

        return {
            "status": "ignored",
            "reason": "unknown_signal",
        }


# ============================================================
# NORMALIZACIÓN DE OPERACIONES
# ============================================================

def normalize_trade(raw):
    raw = dict(raw or {})

    side = str(
        raw.get("side", "")
    ).upper()

    if side in {"BUY", "L"}:
        side = "LONG"

    if side in {"SELL", "S"}:
        side = "SHORT"

    if side not in {"LONG", "SHORT"}:
        raise ValueError(
            "La operación necesita LONG o SHORT"
        )

    entry = fnum(
        raw.get("entry_price", raw.get("entry"))
    )

    exit_price = fnum(
        raw.get("exit_price", raw.get("exit"))
    )

    quantity = fnum(
        raw.get("quantity", raw.get("qty"))
    )

    margin = fnum(
        raw.get("margin_used", raw.get("margin"))
    )

    leverage = fnum(
        raw.get("leverage", LEVERAGE)
    )

    fees = fnum(
        raw.get("total_fees", raw.get("fees"))
    )

    funding = fnum(raw.get("funding"))
    risk = fnum(raw.get("risk_usd", raw.get("risk")))

    sign = 1 if side == "LONG" else -1

    gross = raw.get("gross_pnl")

    if gross is None:
        gross = (
            (exit_price - entry) * quantity * sign
            if entry > 0 and exit_price > 0
            else 0
        )

    gross = fnum(gross)

    net = raw.get("net_pnl")

    if net is None:
        net = gross - fees - funding

    net = fnum(net)

    move_pct = (
        (exit_price - entry) / entry * 100 * sign
        if entry > 0 and exit_price > 0
        else 0
    )

    return {
        **raw,
        "id": str(raw.get("id") or uuid.uuid4()),
        "opened_at": raw.get("opened_at") or utc_now(),
        "closed_at": raw.get("closed_at") or utc_now(),
        "side": side,
        "symbol": raw.get("symbol") or BINGX_SYMBOL,
        "entry_price": entry,
        "exit_price": exit_price,
        "quantity": quantity,
        "margin_used": margin,
        "leverage": leverage,
        "gross_pnl": gross,
        "total_fees": fees,
        "funding": funding,
        "net_pnl": net,
        "risk_usd": risk,
        "r_multiple": net / risk if risk > 0 else 0,
        "price_move_pct": move_pct,
        "pnl_pending": bool(raw.get("pnl_pending", False)),
        "source": raw.get("source", "AUTO"),
        "close_reason": raw.get("close_reason", ""),
    }


# ============================================================
# ESTADÍSTICAS
# ============================================================

def trade_summary(trades):
    confirmed = [
        t for t in trades
        if not t.get("pnl_pending")
    ]

    pending = len(trades) - len(confirmed)

    wins = [
        t for t in confirmed
        if fnum(t.get("net_pnl")) > 0.01
    ]

    losses = [
        t for t in confirmed
        if fnum(t.get("net_pnl")) < -0.01
    ]

    net = sum(
        fnum(t.get("net_pnl"))
        for t in confirmed
    )

    gross = sum(
        fnum(t.get("gross_pnl"))
        for t in confirmed
    )

    fees = sum(
        fnum(t.get("total_fees"))
        for t in confirmed
    )

    gains = sum(
        fnum(t.get("net_pnl"))
        for t in wins
    )

    losses_abs = abs(sum(
        fnum(t.get("net_pnl"))
        for t in losses
    ))

    running = 0
    peak = 0
    max_dd = 0
    curve = [0]

    for trade in confirmed:
        running += fnum(trade.get("net_pnl"))
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)
        curve.append(round(running, 8))

    current_type = "—"
    current_count = 0
    max_wins = 0
    max_losses = 0
    win_run = 0
    loss_run = 0

    for trade in confirmed:
        pnl = fnum(trade.get("net_pnl"))

        if pnl > 0.01:
            win_run += 1
            loss_run = 0
            max_wins = max(max_wins, win_run)
        elif pnl < -0.01:
            loss_run += 1
            win_run = 0
            max_losses = max(max_losses, loss_run)
        else:
            win_run = 0
            loss_run = 0

    for trade in reversed(confirmed):
        pnl = fnum(trade.get("net_pnl"))

        kind = (
            "W" if pnl > 0.01
            else "L" if pnl < -0.01
            else "BE"
        )

        if current_type == "—":
            current_type = kind
            current_count = 1
        elif kind == current_type:
            current_count += 1
        else:
            break

    base = 0

    for trade in confirmed:
        value = fnum(trade.get("balance_before"))

        if value > 0:
            base = value
            break

    longs = [
        t for t in confirmed
        if t.get("side") == "LONG"
    ]

    shorts = [
        t for t in confirmed
        if t.get("side") == "SHORT"
    ]

    values = [
        fnum(t.get("net_pnl"))
        for t in confirmed
    ]

    return {
        "count": len(trades),
        "confirmed": len(confirmed),
        "pending": pending,
        "wins": len(wins),
        "losses": len(losses),
        "be": len(confirmed) - len(wins) - len(losses),
        "winrate": (
            len(wins) / len(confirmed) * 100
            if confirmed else 0
        ),
        "net": net,
        "gross": gross,
        "fees": fees,
        "pf": (
            gains / losses_abs
            if losses_abs > 0
            else 999 if gains > 0 else 0
        ),
        "max_dd": max_dd,
        "net_pct": net / base * 100 if base > 0 else 0,
        "max_dd_pct": (
            max_dd / base * 100 if base > 0 else 0
        ),
        "curve": curve,
        "streak_type": current_type,
        "streak_count": current_count,
        "streak_label": (
            f"{current_count} "
            + {
                "W": "GANADAS",
                "L": "PERDIDAS",
                "BE": "BE",
            }.get(current_type, "")
            if current_type != "—" else "—"
        ),
        "max_win_streak": max_wins,
        "max_loss_streak": max_losses,
        "avg_win": gains / len(wins) if wins else 0,
        "avg_loss": (
            -losses_abs / len(losses)
            if losses else 0
        ),
        "long_count": len(longs),
        "short_count": len(shorts),
        "long_net": sum(
            fnum(t.get("net_pnl")) for t in longs
        ),
        "short_net": sum(
            fnum(t.get("net_pnl")) for t in shorts
        ),
        "best_trade": max(values, default=0),
        "worst_trade": min(values, default=0),
        "r_global": sum(
            fnum(t.get("r_multiple"))
            for t in confirmed
        ),
    }


# ============================================================
# PANEL HTML
# ============================================================

PANEL_HTML = r"""
<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport"
content="width=device-width,initial-scale=1">

<title>BOT PETRÓLEO WTI 3M</title>

<style>
:root {
    --bg:#05070a;
    --card:#12161c;
    --card2:#0c1016;
    --line:#29313c;
    --text:#f5f7fa;
    --muted:#94a0af;
    --green:#00df87;
    --red:#ff4963;
    --blue:#357df6;
    --gold:#c99a08;
}

* {box-sizing:border-box}

body {
    margin:0;
    background:var(--bg);
    color:var(--text);
    font-family:-apple-system,BlinkMacSystemFont,
                "Segoe UI",sans-serif;
}

main {
    max-width:1050px;
    margin:auto;
    padding:16px;
}

h1 {
    text-align:center;
    font-size:35px;
    margin:20px 0 8px;
}

.center {text-align:center}
.muted {color:var(--muted)}
.good {color:var(--green)}
.bad {color:var(--red)}
.warn {color:#ffd166}

.card {
    background:var(--card);
    border:1px solid var(--line);
    border-radius:24px;
    padding:22px;
    margin:18px 0;
}

.mode {
    text-align:center;
    color:var(--green);
    font-size:42px;
    font-weight:900;
    margin:12px 0;
}

.grid {
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:12px;
}

.stats {
    display:grid;
    grid-template-columns:repeat(4,1fr);
    gap:12px;
}

.stat {
    background:var(--card2);
    border:1px solid var(--line);
    border-radius:17px;
    padding:16px;
}

.stat small {
    color:var(--muted);
    display:block;
}

.stat strong {
    display:block;
    font-size:26px;
    margin-top:8px;
}

.btn,button {
    display:block;
    width:100%;
    border:0;
    border-radius:16px;
    padding:18px 12px;
    color:white;
    text-decoration:none;
    font-size:17px;
    font-weight:800;
    text-align:center;
    cursor:pointer;
}

.off {background:#68717f}
.long {background:#07934f}
.short {background:#bf233e}
.close {background:#2769bd}
.both {background:#bd9009}
.blue {background:var(--blue)}
.dark {background:#303640}

input,select {
    width:100%;
    padding:14px;
    border-radius:12px;
    border:1px solid var(--line);
    background:#0b0e12;
    color:white;
    font-size:15px;
}

table {
    width:100%;
    border-collapse:collapse;
    font-size:12px;
}

th,td {
    padding:10px;
    border-bottom:1px solid var(--line);
    text-align:left;
    white-space:nowrap;
}

.scroll {overflow:auto}

.event {
    background:#0b0e12;
    border:1px solid var(--line);
    border-radius:12px;
    padding:11px;
    margin:8px 0;
    font-size:12px;
    overflow-wrap:anywhere;
}

.tag {
    display:inline-block;
    padding:5px 9px;
    border-radius:20px;
    background:#202630;
}

#curve {
    display:block;
    width:100%;
    height:250px;
    background:#090d12;
    border-radius:18px;
}

@media(max-width:850px) {
    .stats {grid-template-columns:1fr 1fr}
}

@media(max-width:600px) {
    h1 {font-size:29px}
    main {padding:12px}
    .mode {font-size:35px}
    .stats {grid-template-columns:1fr 1fr}
}
</style>
</head>

<body>
<main>

<h1>🛢 BOT PETRÓLEO WTI 3M</h1>

<p class="center muted">
{{symbol}} · 3M · {{leverage}}x ·
ISOLATED ·
{{'%.1f'|format(balance_percent)}}% del balance · REAL
</p>

<section class="card">
    <p class="center muted">MODO ACTUAL</p>
    <div class="mode">
        {{'LONG + SHORT' if mode == 'BOTH' else mode}}
    </div>
</section>

<section class="grid">

<form method="post"
action="/setmode/OFF?secret={{secret}}">
<button class="off">OFF</button>
</form>

<form method="post"
action="/setmode/LONG_ONLY?secret={{secret}}">
<button class="long">SOLO LONG</button>
</form>

<form method="post"
action="/setmode/SHORT_ONLY?secret={{secret}}">
<button class="short">SOLO SHORT</button>
</form>

<form method="post"
action="/setmode/CLOSE_ONLY?secret={{secret}}">
<button class="close">SOLO CERRAR</button>
</form>

<form method="post"
action="/setmode/BOTH?secret={{secret}}">
<button class="both">LONG + SHORT</button>
</form>

<a class="btn blue"
href="/diagnostic?secret={{secret}}">
PROBAR BINGX
</a>

</section>

<section class="card">

<div class="grid">
<h2>Posición real BingX</h2>

<form method="post"
action="/sync?secret={{secret}}">
<button class="blue">SINCRONIZAR</button>
</form>
</div>

{% if warning %}
<p class="warn">{{warning}}</p>
{% endif %}

{% if live %}
<h2 class="good">
{{live.side}} · {{live.quantity}} contratos
</h2>

<p>
Entrada: {{live.entry_price}} ·
{{live.leverage}}x ·
PnL flotante:
{{'%.4f'|format(live.unrealized_pnl)}} USDT
</p>
{% else %}
<p class="center muted">
FLAT · Sin posición WTI abierta
</p>
{% endif %}

</section>

<section class="card">

<h2>Curva de rendimiento</h2>

<h1 class="{{'good' if stats.net >= 0 else 'bad'}}">
${{'%.2f'|format(stats.net)}}
</h1>

<p class="muted">
PnL confirmado ·
{{'%+.2f'|format(stats.net_pct)}}%
</p>

<svg id="curve"
viewBox="0 0 1000 260"
preserveAspectRatio="none"></svg>

</section>

<section class="card">

<h2>Estadísticas</h2>

<div class="stats">

{% for label,value in cards %}
<div class="stat">
<small>{{label}}</small>
<strong>{{value}}</strong>
</div>
{% endfor %}

</div>

{% if stats.pending %}
<p class="warn">
{{stats.pending}} operaciones con PnL
pendiente de conciliación.
</p>
{% endif %}

</section>

<section class="card">

<h2>Agregar operación manual</h2>

<form class="grid" method="post"
action="/manual-trade?secret={{secret}}">

<select name="side">
<option>LONG</option>
<option>SHORT</option>
</select>

<input name="entry_price"
type="number" step="any"
placeholder="Precio entrada" required>

<input name="exit_price"
type="number" step="any"
placeholder="Precio salida" required>

<input name="quantity"
type="number" step="any"
placeholder="Cantidad contratos">

<input name="margin_used"
type="number" step="any"
placeholder="Margen USDT">

<input name="risk_usd"
type="number" step="any"
placeholder="Riesgo USDT">

<input name="gross_pnl"
type="number" step="any"
placeholder="PnL bruto real">

<input name="net_pnl"
type="number" step="any"
placeholder="PnL neto real">

<input name="fees"
type="number" step="any"
placeholder="Comisiones">

<input name="funding"
type="number" step="any"
placeholder="Funding">

<input name="opened_at"
type="datetime-local">

<input name="closed_at"
type="datetime-local">

<input name="notes"
placeholder="Notas">

<button class="long">AGREGAR</button>

</form>
</section>

<section class="card">

<h2>Importar historial JSON</h2>

<form class="grid"
method="post"
enctype="multipart/form-data"
action="/import-json?secret={{secret}}">

<input type="file" name="file"
accept=".json,application/json" required>

<select name="mode">
<option value="append">AGREGAR</option>
<option value="replace">REEMPLAZAR</option>
</select>

<button class="blue">IMPORTAR</button>

</form>
</section>

<section class="grid">

<a class="btn dark"
href="/download?secret={{secret}}">
DESCARGAR CSV
</a>

<a class="btn dark"
href="/export-json?secret={{secret}}">
EXPORTAR JSON
</a>

</section>

<section class="card scroll">

<h2>Historial de operaciones</h2>

<table>
<thead>
<tr>
<th>Cierre</th>
<th>Lado</th>
<th>Origen</th>
<th>Entrada</th>
<th>Salida</th>
<th>Movimiento</th>
<th>Margen</th>
<th>Fees</th>
<th>PnL neto</th>
<th>R</th>
<th>Motivo</th>
</tr>
</thead>

<tbody>

{% for t in trades %}
<tr>

<td>{{t.closed_at[:19]}}</td>
<td>{{t.side}}</td>

<td>
<span class="tag">{{t.source}}</span>
</td>

<td>{{'%.4f'|format(t.entry_price)}}</td>

<td>
{{'%.4f'|format(t.exit_price)
if t.exit_price else 'Pendiente'}}
</td>

<td>
{{'%.3f'|format(t.price_move_pct)}}%
</td>

<td>
${{'%.2f'|format(t.margin_used)}}
</td>

<td>
${{'%.2f'|format(t.total_fees)}}
</td>

<td class="{{'good' if t.net_pnl >= 0 else 'bad'}}">
{{'Pendiente' if t.pnl_pending
else '$%.2f'|format(t.net_pnl)}}
</td>

<td>{{'%.2f'|format(t.r_multiple)}}R</td>
<td>{{t.close_reason}}</td>

</tr>
{% else %}
<tr>
<td colspan="11" class="center muted">
Todavía no hay operaciones.
</td>
</tr>
{% endfor %}

</tbody>
</table>

</section>

<section class="card">

<h2>Últimos eventos</h2>

{% for e in events %}
<div class="event">
{{e.time[:19].replace('T',' ')}}
· {{e.kind}} · {{e.detail}}
</div>
{% else %}
<p class="muted">Todavía no hay eventos.</p>
{% endfor %}

</section>

</main>

<script>
const values = {{stats.curve|tojson}};
const svg = document.getElementById("curve");

function drawCurve() {
    if (!svg || !values.length) return;

    const W = 1000;
    const H = 260;
    const px = 24;
    const py = 28;

    const minVal = Math.min(0,...values);
    const maxVal = Math.max(0,...values);
    const span = maxVal - minVal || 1;

    const x = i =>
        values.length === 1
        ? W/2
        : px + i/(values.length-1)*(W-2*px);

    const y = v =>
        H-py-(v-minVal)/span*(H-2*py);

    const points = values.map(
        (v,i) => `${x(i)},${y(v)}`
    ).join(" ");

    const last = values[values.length-1];
    const color = last >= 0 ? "#00df87" : "#ff4963";

    svg.innerHTML = `
        <line x1="${px}" y1="${y(0)}"
        x2="${W-px}" y2="${y(0)}"
        stroke="#38414c" stroke-width="2"
        stroke-dasharray="7 7"/>

        <polyline points="${points}"
        fill="none" stroke="#ffffff"
        stroke-width="4"
        stroke-linejoin="round"/>

        <circle cx="${x(values.length-1)}"
        cy="${y(last)}" r="9"
        fill="${color}" stroke="white"
        stroke-width="3"/>
    `;
}

drawCurve();
</script>

</body>
</html>
"""


# ============================================================
# AUTORIZACIÓN
# ============================================================

def control_authorized():
    return secret_matches(
        request.args.get("secret", ""),
        CONTROL_SECRET,
    )


# ============================================================
# HOME
# ============================================================

@app.get("/")
def home():
    return (
        f"{BOT_NAME} | "
        f"SYMBOL={BINGX_SYMBOL} | "
        f"MODE={store.get_mode()} | "
        "REAL",
        200,
    )


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
def health():
    return jsonify({
        "status": "ok",
        "bot": BOT_NAME,
        "real_trading": True,
        "symbol": BINGX_SYMBOL,
        "timeframe": "3m",
        "leverage": LEVERAGE,
        "balance_percent": BALANCE_PERCENT,
        "position_mode": POSITION_MODE,
        "mode": store.get_mode(),
        "tv_symbols": sorted(TV_SYMBOLS),
        "state_prefix": STATE_PREFIX,
        "redis": store.redis_enabled,
        "version": "oil-four-alerts-v1",
    })


# ============================================================
# PANEL
# ============================================================

@app.get("/control")
def control():
    if not control_authorized():
        return "Clave de panel inválida", 403

    live = None
    warning = ""

    try:
        sync = sync_position()

        if sync.get("reason") == "multiple_positions":
            warning = (
                "Hay varias posiciones WTI abiertas. "
                "Revisar BingX."
            )
        else:
            live = sync.get("position")

    except Exception as exc:
        warning = (
            "Error sincronizando BingX: "
            + redact_error(exc)
        )

    trades = store.get_trades()
    stats = trade_summary(trades)

    cards = [
        ("Operaciones", stats["count"]),
        ("Confirmadas", stats["confirmed"]),
        ("Pendientes", stats["pending"]),
        ("Ganadas", stats["wins"]),
        ("Perdidas", stats["losses"]),
        ("Winrate", f"{stats['winrate']:.1f}%"),
        ("Profit Factor", f"{stats['pf']:.2f}"),
        ("PnL neto", f"${stats['net']:.2f}"),
        ("PnL %", f"{stats['net_pct']:+.2f}%"),
        ("Fees", f"${stats['fees']:.2f}"),
        ("Max DD", f"${stats['max_dd']:.2f}"),
        ("Max DD %", f"{stats['max_dd_pct']:.2f}%"),
        ("R global", f"{stats['r_global']:.2f}R"),
        ("Racha actual", stats["streak_label"]),
        ("Mejor racha W", stats["max_win_streak"]),
        ("Peor racha L", stats["max_loss_streak"]),
        ("Promedio W", f"${stats['avg_win']:.2f}"),
        ("Promedio L", f"${stats['avg_loss']:.2f}"),
        ("LONG", stats["long_count"]),
        ("SHORT", stats["short_count"]),
        ("PnL LONG", f"${stats['long_net']:.2f}"),
        ("PnL SHORT", f"${stats['short_net']:.2f}"),
        ("Mejor operación", f"${stats['best_trade']:.2f}"),
        ("Peor operación", f"${stats['worst_trade']:.2f}"),
    ]

    return render_template_string(
        PANEL_HTML,
        secret=request.args["secret"],
        symbol=BINGX_SYMBOL,
        leverage=LEVERAGE,
        balance_percent=BALANCE_PERCENT,
        mode=store.get_mode(),
        live=live,
        warning=warning,
        trades=list(reversed(trades)),
        stats=stats,
        cards=cards,
        events=list(reversed(store.get_events()[-15:])),
    )


# ============================================================
# CAMBIAR MODO
# ============================================================

@app.post("/setmode/<mode>")
def set_mode(mode):
    if not control_authorized():
        return "Clave de panel inválida", 403

    mode = mode.upper()

    if mode not in VALID_MODES:
        return jsonify({"error": "Modo inválido"}), 400

    with SIGNAL_LOCK:
        store.set_mode(mode)

    store.add_event(
        "MODE",
        f"Modo cambiado a {mode}",
    )

    notify(
        f"{BOT_NAME}: modo cambiado a {mode}"
    )

    return redirect(
        "/control?secret=" + request.args["secret"]
    )


# ============================================================
# SINCRONIZACIÓN MANUAL
# ============================================================

@app.post("/sync")
def sync_route():
    if not control_authorized():
        return "Clave de panel inválida", 403

    try:
        result = sync_position()

        store.add_event(
            "SYNC",
            json.dumps(result, ensure_ascii=False)[:800],
        )

    except Exception as exc:
        store.add_event(
            "SYNC_ERROR",
            redact_error(exc),
        )

    return redirect(
        "/control?secret=" + request.args["secret"]
    )


# ============================================================
# DIAGNÓSTICO
# ============================================================

@app.get("/diagnostic")
def diagnostic():
    if not control_authorized():
        return jsonify({
            "error": "No autorizado"
        }), 403

    checks = {
        "time": utc_now(),
        "bot": BOT_NAME,
        "symbol": BINGX_SYMBOL,
        "mode": store.get_mode(),
        "real_trading": True,
    }

    try:
        checks["price"] = bingx.price()
        checks["contract"] = bingx.contract_rules()
        checks["balance_available_usdt"] = (
            bingx.available_balance()
        )
        checks["positions"] = bingx.positions()
        checks["hedge_mode"] = (
            bingx.position_mode_is_hedge()
        )
        checks["margin_type"] = bingx.margin_type()

        try:
            checks["calculated_order"] = bingx.quantity()
        except Exception as exc:
            checks["calculated_order_error"] = (
                redact_error(exc)
            )

        checks["status"] = "ok"

        store.add_event(
            "DIAGNOSTIC_OK",
            "Conexión BingX comprobada",
        )

        return jsonify(checks)

    except Exception as exc:
        message = redact_error(exc)

        store.add_event(
            "DIAGNOSTIC_ERROR",
            message,
        )

        return jsonify({
            **checks,
            "status": "error",
            "error": message,
        }), 500


# ============================================================
# OPERACIÓN MANUAL
# ============================================================

@app.post("/manual-trade")
def manual_trade():
    if not control_authorized():
        return "Clave de panel inválida", 403

    raw = {
        "id": str(uuid.uuid4()),
        "opened_at": (
            request.form.get("opened_at") or utc_now()
        ),
        "closed_at": (
            request.form.get("closed_at") or utc_now()
        ),
        "side": request.form.get("side", "LONG"),
        "symbol": BINGX_SYMBOL,
        "entry_price": fnum(
            request.form.get("entry_price")
        ),
        "exit_price": fnum(
            request.form.get("exit_price")
        ),
        "quantity": fnum(
            request.form.get("quantity")
        ),
        "margin_used": fnum(
            request.form.get("margin_used")
        ),
        "risk_usd": fnum(
            request.form.get("risk_usd")
        ),
        "fees": fnum(
            request.form.get("fees")
        ),
        "funding": fnum(
            request.form.get("funding")
        ),
        "leverage": LEVERAGE,
        "source": "MANUAL",
        "notes": request.form.get("notes", ""),
        "close_reason": "manual_panel",
    }

    for field in ["gross_pnl", "net_pnl"]:
        value = request.form.get(field)

        if value not in {None, ""}:
            raw[field] = fnum(value)

    store.append_trade(normalize_trade(raw))

    return redirect(
        "/control?secret=" + request.args["secret"]
    )


# ============================================================
# IMPORTAR JSON
# ============================================================

@app.post("/import-json")
def import_json():
    if not control_authorized():
        return "Clave de panel inválida", 403

    uploaded = request.files.get("file")

    if not uploaded:
        return "Falta archivo JSON", 400

    try:
        payload = json.loads(
            uploaded.read().decode("utf-8")
        )

        if isinstance(payload, dict):
            incoming = payload.get("trades", [])
        elif isinstance(payload, list):
            incoming = payload
        else:
            return "Formato JSON inválido", 400

        normalized = [
            normalize_trade(item)
            for item in incoming
            if isinstance(item, dict)
        ]

    except Exception as exc:
        return (
            "JSON inválido: " + redact_error(exc),
            400,
        )

    import_mode = request.form.get(
        "mode", "append"
    )

    if import_mode == "replace":
        store.set_trades(normalized)

    else:
        existing = store.get_trades()

        ids = {
            str(t.get("id"))
            for t in existing
            if t.get("id")
        }

        for trade in normalized:
            if trade["id"] in ids:
                continue

            existing.append(trade)
            ids.add(trade["id"])

        store.set_trades(existing)

    return redirect(
        "/control?secret=" + request.args["secret"]
    )


# ============================================================
# EXPORTAR CSV
# ============================================================

@app.get("/download")
def download():
    if not control_authorized():
        return "Clave de panel inválida", 403

    fields = [
        "id",
        "opened_at",
        "closed_at",
        "side",
        "symbol",
        "entry_price",
        "exit_price",
        "quantity",
        "leverage",
        "margin_used",
        "gross_pnl",
        "total_fees",
        "funding",
        "net_pnl",
        "pnl_pending",
        "risk_usd",
        "r_multiple",
        "source",
        "close_reason",
    ]

    output = io.StringIO()

    writer = csv.DictWriter(
        output,
        fieldnames=fields,
        extrasaction="ignore",
    )

    writer.writeheader()

    for trade in store.get_trades():
        writer.writerow(trade)

    return send_file(
        io.BytesIO(
            output.getvalue().encode("utf-8")
        ),
        mimetype="text/csv",
        as_attachment=True,
        download_name="petroleo_wti_historial.csv",
    )


# ============================================================
# EXPORTAR JSON
# ============================================================

@app.get("/export-json")
def export_json():
    if not control_authorized():
        return "Clave de panel inválida", 403

    raw = json.dumps(
        {
            "app": BOT_NAME,
            "version": 1,
            "exportedAt": utc_now(),
            "trades": store.get_trades(),
        },
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    return send_file(
        io.BytesIO(raw),
        mimetype="application/json",
        as_attachment=True,
        download_name="petroleo_wti_historial.json",
    )


# ============================================================
# WEBHOOK TRADINGVIEW
# ============================================================

@app.post("/webhook")
def webhook():
    payload = request.get_json(silent=True) or {}

    if not isinstance(payload, dict):
        return jsonify({
            "error": "JSON inválido"
        }), 400

    if not secret_matches(
        payload.get("secret"),
        WEBHOOK_SECRET,
    ):
        store.add_event(
            "WEBHOOK_REJECTED",
            "Clave inválida",
        )

        return jsonify({
            "error": "Clave de webhook inválida"
        }), 403

    action = str(
        payload.get("action")
        or payload.get("side")
        or ""
    ).upper().strip()

    symbol = str(
        payload.get("symbol", "")
    ).upper().strip()

    timeframe = str(
        payload.get("timeframe", "")
    ).lower().strip()

    if action not in {
        "LONG",
        "SHORT",
        "XL",
        "XS",
    }:
        return jsonify({
            "error": "Usa LONG, SHORT, XL o XS"
        }), 400

    if (
        symbol not in TV_SYMBOLS
        and symbol != BINGX_SYMBOL
    ):
        return jsonify({
            "error": "Símbolo WTI no permitido",
            "received": symbol,
            "allowed": sorted(TV_SYMBOLS),
        }), 400

    if timeframe not in {"3", "3m"}:
        return jsonify({
            "error": "Solo señales 3M",
            "received": timeframe,
        }), 400

    signal_id = str(
        payload.get("signal_id")
        or payload.get("bar_time")
        or ""
    ).strip()

    if signal_id:
        signal_id = (
            f"{symbol}:{timeframe}:"
            f"{action}:{signal_id}"
        )

    logger.info(
        "WEBHOOK: %s %s %s",
        action,
        symbol,
        timeframe,
    )

    store.add_event(
        "WEBHOOK_RECEIVED",
        f"{action} {symbol} {timeframe}",
    )

    try:
        with SIGNAL_LOCK:
            if signal_id and store.signal_seen(signal_id):
                return jsonify({
                    "ok": True,
                    "result": {
                        "status": "ignored",
                        "reason": "duplicate_signal",
                    },
                })

            # Se registra antes de ejecutar para evitar
            # que una alerta repetida mande otra orden.
            if signal_id:
                store.remember_signal(signal_id)

            result = process_signal(action)

        store.add_event(
            "WEBHOOK_PROCESSED",
            json.dumps(
                result,
                ensure_ascii=False,
            )[:1400],
        )

        return jsonify({
            "ok": True,
            "result": result,
        })

    except Exception as exc:
        message = redact_error(exc)

        logger.exception(
            "ERROR procesando webhook"
        )

        store.add_event(
            "WEBHOOK_ERROR",
            message,
        )

        notify(
            f"ERROR {BOT_NAME}\n{message}"
        )

        return jsonify({
            "ok": False,
            "error": message,
        }), 500


# ============================================================
# MONITOR
# ============================================================

@app.get("/monitor")
def monitor():
    if not secret_matches(
        request.args.get("token", ""),
        MONITOR_SECRET,
    ):
        return jsonify({
            "error": "No autorizado"
        }), 401

    try:
        sync = sync_position()

    except Exception as exc:
        sync = {
            "status": "error",
            "error": redact_error(exc),
        }

    return jsonify({
        "checked_at": utc_now(),
        "bot": BOT_NAME,
        "real_trading": True,
        "mode": store.get_mode(),
        "sync": sync,
        "active_trade": store.get_active_trade(),
        "stats_all": trade_summary(
            store.get_trades()
        ),
        "events": store.get_events()[-10:],
    })


# ============================================================
# INICIO
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv("PORT", "10000")
        ),
    )