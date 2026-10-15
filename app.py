import csv
import hashlib
import hmac
import io
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, redirect, render_template_string, request, send_file

# BOT PETROLEO WTI 3 MIN - ORDENES REALES
# SHORT/LONG: abren solo si FLAT. XL/XS: cierran el lado correspondiente.
# Sin reversion automatica, sin TP/SL añadidos, sin simulacion.
BOT_NAME = 'BOT PETRÓLEO WTI 3M'
BASE = 'https://open-api.bingx.com'
SYMBOL = os.getenv('BINGX_SYMBOL', 'NCCO1OILWTI2USD-USDT').strip().upper()
TV_SYMBOLS = {s.strip().upper() for s in os.getenv('TV_SYMBOLS', 'OILWTIUSDT.P,BINGX:OILWTIUSDT.P,OILWTI-USDT,OILWTIUSDT').split(',') if s.strip()}
API_KEY = os.getenv('BINGX_API_KEY', '').strip()
API_SECRET = (os.getenv('BINGX_API_SECRET', '') or os.getenv('BINGX_SECRET_KEY', '')).strip()
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', '').strip()
CONTROL_SECRET = os.getenv('CONTROL_SECRET', WEBHOOK_SECRET).strip()
MONITOR_SECRET = os.getenv('MONITOR_SECRET', '').strip()
TG_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TG_CHAT = os.getenv('TELEGRAM_CHAT_ID', '').strip()
BALANCE_PERCENT = float(os.getenv('BALANCE_PERCENT', '90'))
LEVERAGE = int(os.getenv('LEVERAGE', '2'))
DATA_DIR = os.getenv('DATA_DIR', '.')
REDIS_URL = os.getenv('UPSTASH_REDIS_REST_URL', '').strip()
REDIS_TOKEN = os.getenv('UPSTASH_REDIS_REST_TOKEN', '').strip()
STATE_PREFIX = 'bot_petroleo_wti_3m_v1'  # No cambiar: preserva historial WTI.
MODES = ('OFF', 'LONG_ONLY', 'SHORT_ONLY', 'CLOSE_ONLY', 'BOTH')
LOCK = threading.RLock()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('wti-3m')


def now():
    return datetime.now(timezone.utc).isoformat()


def number(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def dec(x):
    return Decimal(str(x))


def epoch_ms(value):
    if isinstance(value, (int, float)):
        return int(value if value > 10**12 else value * 1000)
    try:
        return int(datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp() * 1000)
    except (TypeError, ValueError):
        return 0


def auth(value, expected):
    return bool(value and expected) and hmac.compare_digest(str(value), str(expected))


def safe_error(message):
    text = str(message)
    for key in (API_KEY, API_SECRET, WEBHOOK_SECRET, CONTROL_SECRET, MONITOR_SECRET, REDIS_TOKEN, TG_TOKEN):
        if key:
            text = text.replace(key, '***')
    return text[:500]


# Diagnostico seguro: nunca registra valores de la cuenta ni credenciales.
def pnl_shape(value, depth=0):
    if depth >= 4:
        return type(value).__name__
    if isinstance(value, dict):
        keys = sorted(str(k) for k in value.keys())[:18]
        parts = [f"dict(keys={keys})"]
        for key in ("data", "list", "positions", "positionHistory",
                    "records", "rows", "items", "result", "history",
                    "positionHistoryList", "closedPositions"):
            if key in value:
                parts.append(f"{key}={pnl_shape(value[key], depth + 1)}")
                break
        return " ".join(parts)
    if isinstance(value, list):
        return (f"list(count={len(value)}"
                + (f", first={pnl_shape(value[0], depth + 1)}" if value else "")
                + ")")
    if isinstance(value, str):
        return f"str(length={len(value)})"
    if value is None:
        return "null"
    return type(value).__name__


def pnl_rows(value, depth=0):
    """Acepta únicamente listas de historiales o objetos inequívocos."""
    if depth > 5:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            return pnl_rows(json.loads(value), depth + 1)
        except (ValueError, TypeError):
            return None
    if isinstance(value, dict):
        if ("positionId" in value or "positionSide" in value) and (
            "openTime" in value or "netProfit" in value
        ):
            return [value]
        for key in ("list", "positions", "positionHistory", "records",
                    "rows", "items", "data", "result", "history",
                    "positionHistoryList", "closedPositions"):
            if key in value and value[key] is not None:
                result = pnl_rows(value[key], depth + 1)
                if result is not None:
                    return result
        if value.get("total") in (0, "0"):
            return []
    return None


def telegram(message):
    if TG_TOKEN and TG_CHAT:
        try:
            requests.post(f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage', json={'chat_id': TG_CHAT, 'text': str(message)[:3900]}, timeout=8)
        except requests.RequestException:
            logger.exception('Telegram no disponible')


class Store:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.lock = threading.RLock()

    @property
    def redis(self):
        return bool(REDIS_URL and REDIS_TOKEN)

    def cmd(self, args):
        response = requests.post(REDIS_URL.rstrip('/'), headers={'Authorization': f'Bearer {REDIS_TOKEN}'}, json=args, timeout=12)
        response.raise_for_status()
        result = response.json()
        if result.get('error'):
            raise RuntimeError('Upstash no pudo guardar o recuperar los datos')
        return result.get('result')

    def get(self, name, default):
        with self.lock:
            if self.redis:
                raw = self.cmd(['GET', f'{STATE_PREFIX}:{name}'])
                return json.loads(raw) if raw else default
            path = os.path.join(DATA_DIR, f'{STATE_PREFIX}_{name}.json')
            if not os.path.isfile(path):
                return default
            with open(path, encoding='utf-8') as handle:
                return json.load(handle)

    def set(self, name, value):
        with self.lock:
            raw = json.dumps(value, ensure_ascii=False)
            if self.redis:
                self.cmd(['SET', f'{STATE_PREFIX}:{name}', raw])
            else:
                path = os.path.join(DATA_DIR, f'{STATE_PREFIX}_{name}.json')
                temp = path + '.tmp'
                with open(temp, 'w', encoding='utf-8') as handle:
                    handle.write(raw)
                os.replace(temp, path)

    def mode(self):
        selected = self.get('mode', {'mode': 'OFF'}).get('mode', 'OFF')
        return selected if selected in MODES else 'OFF'

    def trades(self):
        return self.get('trades', [])

    def append_trade(self, item):
        with self.lock:
            trades = self.trades()
            if not any(str(t.get('id')) == str(item.get('id')) for t in trades):
                trades.append(item)
                self.set('trades', trades)

    def event(self, kind, detail):
        with self.lock:
            entries = self.get('events', [])
            entries.append({'time': now(), 'kind': kind, 'detail': safe_error(detail)})
            self.set('events', entries[-150:])

    def seen(self, signal):
        return signal in self.get('signal_ids', [])

    def remember(self, signal):
        with self.lock:
            ids = self.get('signal_ids', [])
            if signal not in ids:
                ids.append(signal)
                self.set('signal_ids', ids[-500:])


store = Store()


class BingX:
    def call(self, method, path, params=None, private=True, envelope=False):
        params = dict(params or {})
        headers = {}
        if private:
            if not API_KEY or not API_SECRET:
                raise RuntimeError('Faltan claves de BingX')
            params.update(timestamp=int(time.time() * 1000), recvWindow=5000)
            encoded = urlencode(sorted(params.items()))
            sign = hmac.new(API_SECRET.encode(), encoded.encode(), hashlib.sha256).hexdigest()
            url = f'{BASE}{path}?{encoded}&signature={sign}'
            headers['X-BX-APIKEY'] = API_KEY
        else:
            encoded = urlencode(params)
            url = f'{BASE}{path}' + (f'?{encoded}' if encoded else '')
        response = requests.request(method, url, headers=headers, timeout=20)
        try:
            result = response.json()
        except ValueError:
            raise RuntimeError(f'BingX no devolvió JSON (HTTP {response.status_code})')
        if response.status_code >= 400 or str(result.get('code')) != '0':
            raise RuntimeError('Error BingX: ' + safe_error(result))
        return result if envelope else result.get('data')

    def price(self):
        data = self.call('GET', '/openApi/swap/v2/quote/price', {'symbol': SYMBOL}, False)
        if isinstance(data, list):
            data = data[0] if data else {}
        price = number((data or {}).get('price') or (data or {}).get('lastPrice'))
        if price <= 0:
            raise RuntimeError('Precio WTI no disponible')
        return price

    def contract(self):
        data = self.call('GET', '/openApi/swap/v2/quote/contracts', private=False)
        if isinstance(data, dict):
            data = data.get('contracts') or data.get('data') or [data]
        found = [c for c in (data or []) if isinstance(c, dict) and str(c.get('symbol', '')).upper() == SYMBOL]
        if len(found) != 1:
            raise RuntimeError('Contrato WTI no encontrado de manera única')
        c = found[0]
        precision = c.get('quantityPrecision')
        minimum = number(c.get('tradeMinQuantity') or c.get('minQty'))
        if precision is None or minimum <= 0:
            raise RuntimeError('Datos de cantidad incompletos en el contrato')
        return {'step': str(Decimal('1').scaleb(-int(precision))), 'minimum': minimum, 'min_notional': number(c.get('tradeMinUSDT')), 'raw': c}

    def balance(self):
        data = self.call('GET', '/openApi/swap/v3/user/balance')
        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for item in (data.get('balance'), data.get('balances'), data):
                items += item if isinstance(item, list) else [item] if isinstance(item, dict) else []
        for item in items:
            if not isinstance(item, dict) or str(item.get('asset') or item.get('currency') or 'USDT').upper() != 'USDT':
                continue
            for key in ('availableMargin', 'availableBalance', 'available'):
                if key in item and number(item[key], -1) >= 0:
                    return number(item[key])
        raise RuntimeError('Balance USDT disponible no encontrado')

    def positions(self):
        data = self.call('GET', '/openApi/swap/v2/user/positions', {'symbol': SYMBOL})
        if isinstance(data, dict):
            data = [data]
        output = []
        for p in data or []:
            if not isinstance(p, dict) or str(p.get('symbol', SYMBOL)).upper() != SYMBOL:
                continue
            qty = number(p.get('positionAmt') if p.get('positionAmt') is not None else p.get('positionAmount') if p.get('positionAmount') is not None else p.get('availableAmt') if p.get('availableAmt') is not None else p.get('quantity'))
            if qty == 0:
                continue
            side = str(p.get('positionSide', '')).upper()
            if side not in ('LONG', 'SHORT'):
                side = 'LONG' if qty > 0 else 'SHORT'
            output.append({'side': side, 'quantity': abs(qty), 'entry_price': number(p.get('avgPrice') or p.get('entryPrice')), 'leverage': number(p.get('leverage'), LEVERAGE), 'margin_used': number(p.get('initialMargin') or p.get('isolatedMargin') or p.get('margin')), 'unrealized_pnl': number(p.get('unrealizedProfit') or p.get('unrealizedPnl')), 'position_id': str(p.get('positionId') or '')})
        return output

    def hedge(self):
        data = self.call('GET', '/openApi/swap/v1/positionSide/dual')
        value = data.get('dualSidePosition') if isinstance(data, dict) else data
        return value is True or str(value).lower() == 'true'

    def margin_type(self):
        data = self.call('GET', '/openApi/swap/v2/trade/marginType', {'symbol': SYMBOL})
        return str(data.get('marginType', '')).upper() if isinstance(data, dict) else ''

    def configure(self):
        if not self.hedge():
            self.call('POST', '/openApi/swap/v1/positionSide/dual', {'dualSidePosition': 'true'})
        if self.margin_type() != 'ISOLATED':
            self.call('POST', '/openApi/swap/v2/trade/marginType', {'symbol': SYMBOL, 'marginType': 'ISOLATED'})
        for direction in ('LONG', 'SHORT'):
            self.call('POST', '/openApi/swap/v2/trade/leverage', {'symbol': SYMBOL, 'side': direction, 'leverage': LEVERAGE})

    def order(self, side, direction, quantity):
        data = self.call('POST', '/openApi/swap/v2/trade/order', {'symbol': SYMBOL, 'side': side, 'positionSide': direction, 'type': 'MARKET', 'quantity': quantity})
        if isinstance(data, dict) and isinstance(data.get('order'), dict):
            data = data['order']
        return data if isinstance(data, dict) else {}

    def order_detail(self, order_id):
        data = self.call('GET', '/openApi/swap/v2/trade/order', {'symbol': SYMBOL, 'orderId': str(order_id)})
        if isinstance(data, dict) and isinstance(data.get('order'), dict):
            data = data['order']
        return data if isinstance(data, dict) else {}

    def orders_window(self, start_ms, end_ms):
        data = self.call('GET', '/openApi/swap/v2/trade/allOrders', {'symbol': SYMBOL, 'startTime': start_ms, 'endTime': end_ms, 'limit': 1000})
        if isinstance(data, dict):
            data = data.get('orders') or data.get('list') or data.get('data') or []
        return data if isinstance(data, list) else []

    # --------------------------------------------------------
    # HISTORIAL DE POSICIONES CERRADAS (FUENTE DE PNL NETO)
    # --------------------------------------------------------

    def position_history(self, start_ms, end_ms):
        """Historial real de posiciones. Diagnostico sin datos privados."""
        rows = []
        page_size = 100
        for page in range(1, 21):
            response = self.call(
                'GET',
                '/openApi/swap/v1/trade/positionHistory',
                {
                    'symbol': SYMBOL,
                    'currency': 'USDT',
                    'startTs': int(start_ms),
                    'endTs': int(end_ms),
                    'pageIndex': page,
                    'pageSize': page_size,
                },
                envelope=True,
            )
            # No se guarda el cuerpo ni los valores de la respuesta.
            data = response.get('data') if isinstance(response, dict) else None
            store.event(
                'PNL_DIAGNOSTIC',
                f"positionHistory page={page}; "
                f"code={response.get('code') if isinstance(response, dict) else 'unknown'}; "
                f"envelope_keys={sorted(response.keys()) if isinstance(response, dict) else []}; "
                f"data={pnl_shape(data)}"
            )

            current = pnl_rows(data)
            if current is None:
                raise RuntimeError(
                    'Formato de positionHistory no reconocido; ' 
                    'envíame el evento PNL_DIAGNOSTIC'
                )
            if not all(isinstance(item, dict) for item in current):
                raise RuntimeError(
                    'La lista de historial tiene elementos inesperados; ' 
                    'ver PNL_DIAGNOSTIC'
                )

            rows.extend(current)
            total = None
            if isinstance(data, dict):
                total = data.get('total')
                if total is None and isinstance(data.get('data'), dict):
                    total = data['data'].get('total')
            if total is not None:
                try:
                    if len(rows) >= int(total):
                        break
                except (TypeError, ValueError):
                    raise RuntimeError('Total de posiciones desconocido')
            if len(current) < page_size:
                break
        else:
            raise RuntimeError(
                'Historial paginado incompleto; no se atribuye PnL'
            )
        return rows


bingx = BingX()


def sync_position():
    positions = bingx.positions()
    if len(positions) > 1:
        raise RuntimeError('Varias posiciones WTI detectadas: revisar BingX')
    live = positions[0] if positions else None
    saved = store.get('active_trade', None)
    if live:
        if not saved or saved.get('side') != live['side']:
            saved = {'id': str(uuid.uuid4()), 'opened_at': now(), 'side': live['side'], 'symbol': SYMBOL, 'source': 'MANUAL_SYNC'}
            store.event('SYNC', f'Posición {live["side"]} recuperada desde BingX')
        saved.update({key: live[key] for key in ('side', 'quantity', 'entry_price', 'leverage', 'margin_used', 'position_id')})
        store.set('active_trade', saved)
    elif saved:
        store.event('SYNC_WARNING', 'Posición ausente en BingX; revisar cierre externo y PnL')
        store.set('active_trade', None)
    return live


def wait_position(direction=None, timeout=9):
    end = time.time() + timeout
    while time.time() < end:
        positions = bingx.positions()
        if len(positions) > 1:
            raise RuntimeError('Hay varias posiciones; revisar cuenta')
        if direction is None and not positions:
            return None
        if direction and len(positions) == 1 and positions[0]['side'] == direction:
            return positions[0]
        time.sleep(0.4)
    raise RuntimeError('Orden enviada pero BingX aún no confirma estado. Verificar cuenta antes de repetir')


def open_trade(direction):
    if sync_position():
        return {'status': 'ignored', 'reason': 'position_already_open'}
    if not 0 < BALANCE_PERCENT <= 100 or LEVERAGE < 1:
        raise RuntimeError('Porcentaje o leverage inválido')
    rules = bingx.contract()
    price = bingx.price()
    balance = bingx.balance()
    margin = balance * BALANCE_PERCENT / 100
    qty_dec = (dec(margin * LEVERAGE / price) / dec(rules['step'])).to_integral_value(rounding=ROUND_DOWN) * dec(rules['step'])
    if float(qty_dec) < rules['minimum'] or (rules['min_notional'] and float(qty_dec) * price < rules['min_notional']):
        raise RuntimeError('Saldo insuficiente para la cantidad mínima WTI')
    qty = format(qty_dec, 'f')
    bingx.configure()
    order = bingx.order('BUY' if direction == 'LONG' else 'SELL', direction, qty)
    live = wait_position(direction)
    trade = {'id': str(uuid.uuid4()), 'opened_at': now(), 'side': direction, 'symbol': SYMBOL, 'quantity': live['quantity'], 'entry_price': live['entry_price'], 'leverage': live['leverage'], 'margin_used': live['margin_used'] or margin, 'balance_before': balance, 'open_order_id': str(order.get('orderID') or order.get('orderId') or ''), 'source': 'AUTO'}
    store.set('active_trade', trade)
    store.event('ORDER_OPENED', f'{direction} qty={trade["quantity"]} price={trade["entry_price"]}')
    telegram(f'{BOT_NAME}\nAPERTURA {direction}\nPrecio: {trade["entry_price"]}\nCantidad: {trade["quantity"]}')
    return {'status': 'opened', 'trade': trade}


def close_trade(direction, reason):
    live = sync_position()
    if not live or live['side'] != direction:
        return {'status': 'ignored', 'reason': 'no_matching_position'}
    saved = store.get('active_trade', None) or {}
    order = bingx.order('SELL' if direction == 'LONG' else 'BUY', direction, live['quantity'])
    wait_position(None)
    trade = {
        'id': saved.get('id') or str(uuid.uuid4()), 'opened_at': saved.get('opened_at', now()), 'closed_at': now(),
        'side': direction, 'symbol': SYMBOL, 'quantity': live['quantity'], 'entry_price': live['entry_price'],
        'exit_price': 0, 'leverage': live['leverage'], 'margin_used': live['margin_used'],
        'position_id': str(saved.get('position_id') or live.get('position_id') or ''),
        'open_order_id': str(saved.get('open_order_id') or ''),
        'close_order_id': str(order.get('orderID') or order.get('orderId') or ''),
        'gross_pnl': 0, 'total_fees': 0, 'funding': 0, 'net_pnl': 0, 'pnl_pending': True,
        'risk_usd': 0, 'r_multiple': 0, 'price_move_pct': 0,
        'balance_before': saved.get('balance_before', 0), 'source': saved.get('source', 'AUTO'),
        'close_reason': reason,
    }
    store.append_trade(trade)
    store.set('active_trade', None)
    store.event('ORDER_CLOSED', f'{direction} cerrado: iniciando conciliación del PnL')
    telegram(f'{BOT_NAME}\nCIERRE {direction}\nConsultando PnL real de BingX')
    try:
        reconcile_pending(force=True)
    except Exception as exc:
        store.event('PNL_ERROR', safe_error(exc))
    return {'status': 'closed', 'trade_id': trade['id'], 'pnl_status': 'ver panel'}


# ============================================================
# PNL REAL DE CADA POSICION CERRADA: BingX positionHistory
# ============================================================

def position_fingerprint(p):
    """Clave única del cierre; evita atribuirlo a dos operaciones."""
    posid = str(p.get('positionId') or '')
    if posid:
        return 'position:' + posid
    return 'fallback:' + ':'.join(str(p.get(k, '')) for k in (
        'symbol', 'positionSide', 'openTime', 'updateTime',
        'avgPrice', 'avgClosePrice', 'closePositionAmt'))


def match_position(trade, positions, used):
    """Coincidencia única basada en tiempo, lado, precio y cantidad.

    No usa solo una hora o solo la dirección: dos shorts seguidos no
    pueden consumir un mismo registro de posición.
    """
    opened = epoch_ms(trade.get('opened_at'))
    closed = epoch_ms(trade.get('closed_at'))
    if not opened or not closed or closed < opened:
        raise RuntimeError('El registro pendiente no tiene fechas válidas')
    side = str(trade.get('side', '')).upper()
    entry = number(trade.get('entry_price'))
    qty = number(trade.get('quantity'))
    known_id = str(trade.get('position_id') or '')
    matches = []
    for p in positions:
        if not isinstance(p, dict):
            continue
        if str(p.get('symbol', '')).upper() != SYMBOL:
            continue
        if str(p.get('positionSide', '')).upper() != side:
            continue
        key = position_fingerprint(p)
        if key in used:
            continue
        if p.get('closeAllPositions') is False:
            continue
        # Debe ser un cierre real, con PnL neto suministrado por BingX.
        if p.get('netProfit') is None:
            continue
        pid = str(p.get('positionId') or '')
        if known_id and pid and known_id != pid:
            continue
        a = epoch_ms(p.get('openTime'))
        b = epoch_ms(p.get('updateTime') or p.get('closeTime'))
        if not a or not b:
            continue
        if abs(a - opened) > 15 * 60 * 1000:
            continue
        if abs(b - closed) > 15 * 60 * 1000:
            continue
        px = number(p.get('avgPrice'))
        if entry > 0 and (px <= 0 or abs(px - entry) > max(0.16, entry * 0.002)):
            continue
        amount_raw = p.get('closePositionAmt')
        if amount_raw is None:
            amount_raw = p.get('positionAmt')
        amount = abs(number(amount_raw))
        if qty > 0 and amount_raw is not None and amount > 0:
            if abs(qty - amount) > max(0.00005, qty * 0.006):
                continue
        matches.append(p)
    if not matches:
        raise RuntimeError('Aún no hay posición cerrada coincidente en BingX')
    if len(matches) != 1:
        raise RuntimeError(f'Hay {len(matches)} cierres posibles; PnL no asignado para evitar errores')
    return matches[0]


def apply_position_pnl(trade, p):
    """Usa netProfit final; NO vuelve a descontar comisión/funding."""
    net = dec(p['netProfit'])
    gross_val = p.get('realisedProfit')
    if gross_val is None:
        gross_val = p.get('realizedProfit')
    if gross_val is None:
        raise RuntimeError('BingX no proporcionó realisedProfit')
    gross = dec(gross_val)
    commission_raw = p.get('positionCommission')
    if commission_raw is None:
        raise RuntimeError('BingX no proporcionó positionCommission')
    funding_raw = p.get('totalFunding')
    if funding_raw is None:
        raise RuntimeError('BingX no proporcionó totalFunding')
    # Comisión siempre se muestra como costo positivo.
    fees = abs(dec(commission_raw))
    funding = dec(funding_raw)
    entry = number(p.get('avgPrice'))
    exit_price = number(p.get('avgClosePrice'))
    if entry <= 0 or exit_price <= 0:
        raise RuntimeError('Precios de historial de posición incompletos')
    out = dict(trade)
    out.update({
        'position_id': str(p.get('positionId') or ''),
        'entry_price': entry,
        'exit_price': exit_price,
        'gross_pnl': float(gross),
        'total_fees': float(fees),
        'funding': float(funding),
        'net_pnl': float(net),
        'pnl_pending': False,
        'pnl_source': 'BINGX_POSITION_HISTORY',
        'pnl_confirmed_at': now(),
        'position_fingerprint': position_fingerprint(p),
        'price_move_pct': (exit_price - entry) / entry * 100 * (1 if trade['side'] == 'LONG' else -1),
    })
    risk = number(trade.get('risk_usd'))
    out['r_multiple'] = float(net) / risk if risk > 0 else 0
    return out


def reconcile_pending(force=False):
    """Una sola lectura del historial real, actualizaciones sin duplicados."""
    with LOCK:
        prev = number(store.get('last_reconcile', 0))
        if not force and time.time() - prev < 30:
            return {'checked': False, 'reason': 'cooldown'}
        store.set('last_reconcile', time.time())
        trades = store.trades()
        pending = [(i, t) for i, t in enumerate(trades) if t.get('pnl_pending')]
        if not pending:
            return {'checked': True, 'confirmed': 0, 'pending': 0, 'reasons': []}
        starts = [epoch_ms(t.get('opened_at')) for _, t in pending]
        ends = [epoch_ms(t.get('closed_at')) for _, t in pending]
        if not all(starts) or not all(ends):
            return {'checked': True, 'confirmed': 0, 'pending': len(pending), 'reasons': ['Una operación tiene fecha inválida']}
        # Un periodo pequeño, específico de las operaciones pendientes.
        begin = max(0, min(starts) - 60 * 60 * 1000)
        finish = min(int(time.time() * 1000), max(ends) + 60 * 60 * 1000)
        if finish - begin > 89 * 86400000:
            raise RuntimeError('El rango de historial supera el máximo seguro de BingX')
        positions = bingx.position_history(begin, finish)
        used = set()
        for t in trades:
            if t.get('pnl_pending'):
                continue
            if t.get('position_fingerprint'):
                used.add(t['position_fingerprint'])
            elif t.get('position_id'):
                used.add('position:' + str(t['position_id']))
        changed, waiting = 0, []
        for index, original in pending:
            try:
                p = match_position(original, positions, used)
                complete = apply_position_pnl(original, p)
                used.add(position_fingerprint(p))
                trades[index] = complete
                store.set('trades', trades)
                changed += 1
                store.event('PNL_CONFIRMED', f'{complete["side"]} neto {complete["net_pnl"]:+.4f} USDT')
                telegram(f'{BOT_NAME}\nPnL real confirmado: {complete["net_pnl"]:+.4f} USDT')
            except Exception as exc:
                waiting.append(safe_error(exc))
        # No muestra identificadores privados ni registros de la cuenta.
        if waiting:
            store.event('PNL_DIAGNOSTIC', f'Posiciones consultadas: {len(positions)}; campos: {sorted(positions[0].keys()) if positions and isinstance(positions[0], dict) else []}')
        return {'checked': True, 'confirmed': changed, 'pending': len(waiting), 'reasons': waiting[:5]}


def process_signal(action):
    mode = store.mode()
    if mode == 'OFF':
        return {'status': 'ignored', 'reason': 'mode_off'}
    if action == 'XL':
        return close_trade('LONG', 'tradingview_XL_3m')
    if action == 'XS':
        return close_trade('SHORT', 'tradingview_XS_3m')
    if action == 'LONG' and mode in ('LONG_ONLY', 'BOTH'):
        return open_trade('LONG')
    if action == 'SHORT' and mode in ('SHORT_ONLY', 'BOTH'):
        return open_trade('SHORT')
    return {'status': 'ignored', 'reason': 'mode_disallows_entry'}


def normalize_trade(item):
    t = dict(item)
    side = str(t.get('side', '')).upper()
    if side not in ('LONG', 'SHORT'):
        raise ValueError('Dirección inválida')
    t.setdefault('id', str(uuid.uuid4()))
    t.setdefault('opened_at', now())
    t.setdefault('closed_at', now())
    t.setdefault('symbol', SYMBOL)
    t.setdefault('source', 'MANUAL')
    for key in ('entry_price', 'exit_price', 'quantity', 'margin_used', 'leverage', 'gross_pnl', 'total_fees', 'funding', 'risk_usd'):
        t[key] = number(t.get(key))
    t['net_pnl'] = number(t.get('net_pnl', t['gross_pnl'] - t['total_fees'] + t['funding']))
    t['r_multiple'] = t['net_pnl'] / t['risk_usd'] if t['risk_usd'] else 0
    t['price_move_pct'] = ((t['exit_price'] - t['entry_price']) / t['entry_price'] * 100 * (1 if side == 'LONG' else -1)) if t['entry_price'] and t['exit_price'] else 0
    t['pnl_pending'] = bool(t.get('pnl_pending', False))
    return t


def summary(trades):
    done = [t for t in trades if not t.get('pnl_pending')]
    pnl = [number(t.get('net_pnl')) for t in done]
    wins = [p for p in pnl if p > 0.000001]
    losses = [p for p in pnl if p < -0.000001]
    curve = [0.0]
    peak = dd = 0.0
    longest_win = longest_loss = win_streak = loss_streak = 0
    for p in pnl:
        curve.append(curve[-1] + p)
        peak = max(peak, curve[-1]); dd = max(dd, peak - curve[-1])
        win_streak = win_streak + 1 if p > 0.000001 else 0
        loss_streak = loss_streak + 1 if p < -0.000001 else 0
        longest_win = max(longest_win, win_streak); longest_loss = max(longest_loss, loss_streak)
    longs = [t for t in done if t.get('side') == 'LONG']
    shorts = [t for t in done if t.get('side') == 'SHORT']
    return {
        'count': len(trades), 'confirmed': len(done), 'pending': len(trades) - len(done),
        'wins': len(wins), 'losses': len(losses), 'be': len(done) - len(wins) - len(losses),
        'winrate': 100 * len(wins) / len(done) if done else 0,
        'net': sum(pnl), 'gross': sum(number(t.get('gross_pnl')) for t in done),
        'fees': sum(number(t.get('total_fees')) for t in done),
        'funding': sum(number(t.get('funding')) for t in done),
        'pf': sum(wins) / abs(sum(losses)) if losses else (999 if wins else 0),
        'max_dd': dd, 'curve': curve,
        'long_count': len(longs), 'short_count': len(shorts),
        'long_net': sum(number(t.get('net_pnl')) for t in longs),
        'short_net': sum(number(t.get('net_pnl')) for t in shorts),
        'best': max(pnl, default=0), 'worst': min(pnl, default=0),
        'avg_win': sum(wins) / len(wins) if wins else 0,
        'avg_loss': sum(losses) / len(losses) if losses else 0,
        'max_win_streak': longest_win, 'max_loss_streak': longest_loss,
        'r_total': sum(number(t.get('r_multiple')) for t in done),
    }

PANEL = r'''<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BOT PETRÓLEO WTI 3M</title>
<style>
:root{--bg:#070a0f;--panel:#151a21;--inner:#0e1218;--stroke:#2d3541;--white:#f4f7fa;--gray:#a3adbc;--green:#00d489;--red:#fc536b;--blue:#3977e9;--gold:#cfa01a}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--white);font:15px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{max-width:1100px;margin:auto;padding:16px}.center{text-align:center}.muted{color:var(--gray)}.green{color:var(--green)}.red{color:var(--red)}.yellow{color:#ffcd6e}
h1{text-align:center;font-size:32px}h2{margin:0 0 18px;font-size:19px}section{margin:18px 0}
.card{background:var(--panel);border:1px solid var(--stroke);padding:21px;border-radius:23px}.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
.stat{background:var(--inner);border:1px solid var(--stroke);border-radius:15px;padding:14px;overflow-wrap:anywhere}.stat small{display:block;color:var(--gray)}.stat strong{display:block;margin-top:7px;font-size:25px}
.mode{font-size:38px;font-weight:900;color:var(--green);margin:18px auto}.btn,button{border:0;border-radius:14px;color:white;font-size:16px;font-weight:750;padding:16px 12px;display:block;text-align:center;width:100%;text-decoration:none;cursor:pointer}
.off{background:#606b79}.long{background:#048856}.short{background:#bf2945}.close{background:#3265b9}.both{background:#b38a0d}.blue{background:var(--blue)}.dark{background:#343c48}
input,select{width:100%;min-width:0;border:1px solid var(--stroke);background:#0c1116;color:white;padding:13px;border-radius:12px;font-size:15px}
table{border-collapse:collapse;width:100%;font-size:12px}th,td{border-bottom:1px solid var(--stroke);padding:11px 9px;text-align:left;white-space:nowrap}.scroll{overflow:auto}.event{background:#0c1116;border:1px solid var(--stroke);padding:12px;border-radius:12px;margin:7px 0;overflow-wrap:anywhere;font-size:12px}
#curve{display:block;width:100%;height:240px;background:#080c12;border-radius:14px}
@media(max-width:800px){.stats{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(max-width:500px){h1{font-size:25px}.card{padding:16px}.stat strong{font-size:21px}}
</style></head><body><main>
<h1>🛢 BOT PETRÓLEO WTI 3M</h1>
<p class="center muted">{{symbol}} · 3M · {{leverage}}x · ISOLATED · {{percent}}% del balance · REAL</p>
<section class="card center"><span class="muted">MODO ACTUAL</span><div class="mode">{{mode_label}}</div></section>
<section class="grid">
{% for code,label,style in modes %}<form method="post" action="/setmode/{{code}}?secret={{secret}}"><button class="{{style}}">{{label}}</button></form>{% endfor %}
<a class="btn blue" href="/diagnostic?secret={{secret}}">PROBAR BINGX</a>
<form method="post" action="/sync?secret={{secret}}"><button class="blue">SINCRONIZAR</button></form>

</section>
<section class="card"><h2>Posición real en BingX</h2>
{% if warning %}<p class="yellow">{{warning}}</p>{% endif %}
{% if live %}<h2 class="{{'green' if live.side=='LONG' else 'red'}}">{{live.side}} · {{live.quantity}} contratos</h2>
<p>Entrada: {{live.entry_price}} · Apalancamiento: {{live.leverage}}x · PnL flotante: {{'%.4f'|format(live.unrealized_pnl)}} USDT</p>
{% elif not warning %}<p class="muted">FLAT · Sin posición WTI abierta</p>{% endif %}</section>
<section class="card"><h2>Curva de rendimiento confirmado</h2>
<h1 class="{{'green' if stats.net>=0 else 'red'}}">{{'%+.4f'|format(stats.net)}} USDT</h1>
<svg id="curve" viewBox="0 0 1000 260" preserveAspectRatio="none"></svg>
</section>
<section class="card"><h2>Estadísticas · Histórico WTI</h2>
<div class="stats">{% for label,value in cards %}<div class="stat"><small>{{label}}</small><strong>{{value}}</strong></div>{% endfor %}</div>
{% if stats.pending %}<p class="yellow">{{stats.pending}} operación(es) pendientes de datos confirmados por BingX. Pulsa SINCRONIZAR para comprobar los resultados reales en BingX.</p>{% endif %}</section>
<section class="card"><h2>Agregar operación manual</h2>
<form class="grid" method="post" action="/manual-trade?secret={{secret}}">
<select name="side"><option>LONG</option><option>SHORT</option></select>
<input name="entry_price" type="number" step="any" placeholder="Precio de entrada" required>
<input name="exit_price" type="number" step="any" placeholder="Precio de cierre" required>
<input name="quantity" type="number" step="any" placeholder="Cantidad">
<input name="margin_used" type="number" step="any" placeholder="Margen en USDT">
<input name="gross_pnl" type="number" step="any" placeholder="PnL bruto real">
<input name="net_pnl" type="number" step="any" placeholder="PnL neto real">
<input name="fees" type="number" step="any" placeholder="Comisiones">
<input name="funding" type="number" step="any" placeholder="Funding firmado">
<input name="risk_usd" type="number" step="any" placeholder="Riesgo en USDT">
<input name="opened_at" type="datetime-local"><input name="closed_at" type="datetime-local">
<input name="notes" placeholder="Notas"><button class="long">AGREGAR OPERACIÓN</button>
</form></section>
<section class="card"><h2>Importar historial JSON</h2>
<form class="grid" method="post" enctype="multipart/form-data" action="/import-json?secret={{secret}}">
<input type="file" name="file" accept=".json,application/json" required>
<select name="mode"><option value="append">AGREGAR</option><option value="replace">REEMPLAZAR</option></select><button class="blue">IMPORTAR</button></form></section>
<section class="grid"><a class="btn dark" href="/download?secret={{secret}}">DESCARGAR CSV</a><a class="btn dark" href="/export-json?secret={{secret}}">EXPORTAR JSON</a></section>
<section class="card scroll"><h2>Historial de operaciones</h2>
<table><thead><tr><th>Cierre</th><th>Dirección</th><th>Origen</th><th>Entrada</th><th>Salida</th><th>Movimiento</th><th>Margen</th><th>Comisiones</th><th>Funding</th><th>PnL neto</th><th>R</th><th>Motivo</th></tr></thead>
<tbody>{% for t in trades %}<tr>
<td>{{t.closed_at[:19]}}</td><td>{{t.side}}</td><td>{{t.source}}</td>
<td>{{'%.4f'|format(t.entry_price)}}</td>
<td>{{'%.4f'|format(t.exit_price) if t.exit_price else 'Pendiente'}}</td>
<td>{{'%.3f'|format(t.price_move_pct)}}%</td>
<td>${{'%.2f'|format(t.margin_used)}}</td><td>${{'%.4f'|format(t.total_fees)}}</td><td>{{'%+.4f'|format(t.funding)}}</td>
<td class="{{'green' if t.net_pnl>=0 else 'red'}}">{{'Pendiente' if t.pnl_pending else '%+.4f'|format(t.net_pnl)}}</td>
<td>{{'%.2f'|format(t.r_multiple)}}R</td><td>{{t.close_reason}}</td></tr>
{% else %}<tr><td colspan="12" class="muted">No hay operaciones registradas.</td></tr>{% endfor %}</tbody></table></section>
<section class="card"><h2>Últimos eventos</h2>{% for e in events %}<div class="event">{{e.time[:19]}} · {{e.kind}} · {{e.detail}}</div>{% else %}<p class="muted">Sin eventos.</p>{% endfor %}</section>
</main><script>
const v={{stats.curve|tojson}};const s=document.getElementById('curve');
if(v.length){const mn=Math.min(0,...v),mx=Math.max(0,...v),span=mx-mn||1;
const x=i=>v.length==1?500:25+i/(v.length-1)*950;const y=p=>234-(p-mn)/span*204;
const pts=v.map((p,i)=>`${x(i)},${y(p)}`).join(' ');
s.innerHTML=`<line x1="25" y1="${y(0)}" x2="975" y2="${y(0)}" stroke="#38414c" stroke-dasharray="6 5"/><polyline points="${pts}" fill="none" stroke="#eef3fa" stroke-width="4"/>`;}
</script></body></html>'''


def panel_authorized():
    return auth(request.args.get('secret'), CONTROL_SECRET)


@app.get('/')
def root():
    return f'{BOT_NAME} | {SYMBOL} | MODE={store.mode()} | REAL', 200


@app.get('/health')
def health():
    return jsonify({'ok': True, 'bot': BOT_NAME, 'symbol': SYMBOL, 'mode': store.mode(), 'timeframe': '3m', 'state_prefix': STATE_PREFIX, 'redis': store.redis, 'version': 'WTI-REAL-PNL-4-POSITION-HISTORY'})


@app.get('/control')
def control():
    if not panel_authorized():
        return 'Acceso denegado', 403
    warning = ''
    live = None
    try:
        live = sync_position()
    except Exception as exc:
        warning = 'Error consultando posición: ' + safe_error(exc)
    try:
        reconcile_pending()
    except Exception as exc:
        store.event('PNL_ERROR', safe_error(exc))
        warning += ' · PnL: ' + safe_error(exc)
    trades = store.trades()
    stats = summary(trades)
    cards = [
        ('Operaciones', stats['count']), ('Confirmadas', stats['confirmed']), ('Pendientes', stats['pending']),
        ('Ganadas', stats['wins']), ('Perdidas', stats['losses']), ('Winrate', f"{stats['winrate']:.1f}%"),
        ('Profit Factor', f"{stats['pf']:.2f}"), ('PnL neto', f"${stats['net']:+.4f}"),
        ('PnL bruto', f"${stats['gross']:+.4f}"), ('Comisiones', f"${stats['fees']:.4f}"),
        ('Funding', f"${stats['funding']:+.4f}"), ('Máx. DD', f"${stats['max_dd']:.4f}"),
        ('LONG', stats['long_count']), ('SHORT', stats['short_count']),
        ('PnL LONG', f"${stats['long_net']:+.4f}"), ('PnL SHORT', f"${stats['short_net']:+.4f}"),
        ('Mejor trade', f"${stats['best']:+.4f}"), ('Peor trade', f"${stats['worst']:+.4f}"),
        ('Promedio W', f"${stats['avg_win']:+.4f}"), ('Promedio L', f"${stats['avg_loss']:+.4f}"),
        ('Mejor racha W', stats['max_win_streak']), ('Peor racha L', stats['max_loss_streak']),
        ('R global', f"{stats['r_total']:.2f}R"),
    ]
    normalized = [normalize_trade(t) for t in reversed(trades)]
    return render_template_string(PANEL, secret=request.args['secret'], symbol=SYMBOL, mode=store.mode(), mode_label={'BOTH': 'LONG + SHORT', 'LONG_ONLY': 'SOLO LONG', 'SHORT_ONLY': 'SOLO SHORT', 'CLOSE_ONLY': 'SOLO CERRAR', 'OFF': 'OFF'}.get(store.mode(), store.mode()), leverage=LEVERAGE, percent=BALANCE_PERCENT, warning=warning, live=live, modes=[('OFF', 'OFF', 'off'), ('LONG_ONLY', 'SOLO LONG', 'long'), ('SHORT_ONLY', 'SOLO SHORT', 'short'), ('CLOSE_ONLY', 'SOLO CERRAR', 'close'), ('BOTH', 'LONG + SHORT', 'both')], stats=stats, cards=cards, trades=normalized, events=list(reversed(store.get('events', [])[-15:])))


@app.post('/setmode/<mode>')
def setmode(mode):
    if not panel_authorized():
        return 'No autorizado', 403
    mode = mode.upper()
    if mode not in MODES:
        return 'Modo no válido', 400
    with LOCK:
        store.set('mode', {'mode': mode, 'updated_at': now()})
    store.event('MODE', f'Modo cambiado a {mode}')
    telegram(f'{BOT_NAME}\nModo: {mode}')
    return redirect('/control?secret=' + request.args['secret'])


@app.post('/sync')
def sync():
    if not panel_authorized():
        return 'No autorizado', 403
    with LOCK:
        try:
            store.event('SYNC', json.dumps(sync_position(), ensure_ascii=False)[:400])
        except Exception as exc:
            store.event('SYNC_ERROR', safe_error(exc))
        try:
            result = reconcile_pending(force=True)
            store.event('PNL_CHECK', f"Confirmadas {result.get('confirmed', 0)}, pendientes {result.get('pending', 0)}; {result.get('reasons', [])[:1]}")
        except Exception as exc:
            store.event('PNL_ERROR', safe_error(exc))
    return redirect('/control?secret=' + request.args['secret'])


@app.get('/diagnostic')
def diagnostic():
    if not panel_authorized():
        return jsonify({'error': 'No autorizado'}), 403
    result = {'bot': BOT_NAME, 'symbol': SYMBOL, 'mode': store.mode(), 'real_trading': True, 'time': now()}
    try:
        result.update({'price': bingx.price(), 'contract': bingx.contract(), 'balance_available_usdt': bingx.balance(), 'positions': bingx.positions(), 'hedge_mode': bingx.hedge(), 'margin_type': bingx.margin_type(), 'status': 'ok'})
    except Exception as exc:
        result['status'] = 'error'
        result['error'] = safe_error(exc)
        store.event('DIAGNOSTIC_ERROR', result['error'])
        return jsonify(result), 500
    store.event('DIAGNOSTIC_OK', 'Conexión BingX comprobada')
    return jsonify(result)


@app.post('/manual-trade')
def manual_trade():
    if not panel_authorized():
        return 'No autorizado', 403
    form = request.form
    if form.get('net_pnl', '') == '' and form.get('gross_pnl', '') == '':
        return 'Indica PnL real bruto o neto', 400
    data = {'id': str(uuid.uuid4()), 'opened_at': form.get('opened_at') or now(), 'closed_at': form.get('closed_at') or now(), 'side': form.get('side', 'LONG'), 'symbol': SYMBOL, 'entry_price': form.get('entry_price'), 'exit_price': form.get('exit_price'), 'quantity': form.get('quantity'), 'margin_used': form.get('margin_used'), 'gross_pnl': form.get('gross_pnl', 0), 'total_fees': form.get('fees', 0), 'funding': form.get('funding', 0), 'risk_usd': form.get('risk_usd', 0), 'leverage': LEVERAGE, 'source': 'MANUAL', 'close_reason': 'manual_panel', 'notes': form.get('notes', '')}
    if form.get('net_pnl') not in (None, ''):
        data['net_pnl'] = form['net_pnl']
    store.append_trade(normalize_trade(data))
    return redirect('/control?secret=' + request.args['secret'])


@app.post('/import-json')
def import_json():
    if not panel_authorized():
        return 'No autorizado', 403
    uploaded = request.files.get('file')
    if not uploaded:
        return 'Falta JSON', 400
    try:
        payload = json.loads(uploaded.read().decode('utf-8'))
        if isinstance(payload, dict):
            if payload.get('app') and payload['app'] != BOT_NAME:
                return 'Este archivo corresponde a otro bot', 400
            records = payload.get('trades', [])
        else:
            records = payload
        if not isinstance(records, list):
            raise ValueError('Se esperaba lista de operaciones')
        incoming = []
        for entry in records:
            if str(entry.get('symbol', SYMBOL)).upper() != SYMBOL:
                raise ValueError('No se permite historial de otro símbolo')
            incoming.append(normalize_trade(entry))
    except Exception as exc:
        return 'JSON inválido: ' + safe_error(exc), 400
    with LOCK:
        current = [] if request.form.get('mode') == 'replace' else store.trades()
        ids = {str(t.get('id')) for t in current}
        for trade in incoming:
            if str(trade['id']) not in ids:
                current.append(trade)
                ids.add(str(trade['id']))
        store.set('trades', current)
    return redirect('/control?secret=' + request.args['secret'])


@app.get('/export-json')
def export_json():
    if not panel_authorized():
        return 'No autorizado', 403
    content = json.dumps({'app': BOT_NAME, 'version': 2, 'exportedAt': now(), 'trades': store.trades()}, ensure_ascii=False, indent=2).encode('utf-8')
    return send_file(io.BytesIO(content), mimetype='application/json', as_attachment=True, download_name='wti_3m_historial.json')


@app.get('/download')
def download():
    if not panel_authorized():
        return 'No autorizado', 403
    cols = ['id', 'opened_at', 'closed_at', 'side', 'symbol', 'entry_price', 'exit_price', 'quantity', 'leverage', 'margin_used', 'gross_pnl', 'total_fees', 'funding', 'net_pnl', 'pnl_pending', 'risk_usd', 'r_multiple', 'source', 'close_reason', 'open_order_id', 'close_order_id']
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=cols, extrasaction='ignore')
    writer.writeheader()
    for row in store.trades():
        writer.writerow(row)
    return send_file(io.BytesIO(output.getvalue().encode('utf-8-sig')), mimetype='text/csv', as_attachment=True, download_name='wti_3m_historial.csv')


@app.post('/webhook')
def webhook():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({'error': 'JSON inválido'}), 400
    if not auth(payload.get('secret'), WEBHOOK_SECRET):
        return jsonify({'error': 'Clave webhook inválida'}), 403
    action = str(payload.get('action') or payload.get('side') or '').upper().strip()
    symbol = str(payload.get('symbol', '')).upper().strip()
    timeframe = str(payload.get('timeframe', '')).lower().strip()
    if action not in ('LONG', 'SHORT', 'XL', 'XS'):
        return jsonify({'error': 'Usa LONG, SHORT, XL o XS'}), 400
    if symbol not in TV_SYMBOLS and symbol != SYMBOL:
        return jsonify({'error': 'Símbolo WTI no permitido', 'symbol': symbol}), 400
    if timeframe not in ('3', '3m'):
        return jsonify({'error': 'Temporalidad debe ser 3M'}), 400
    stamp = str(payload.get('signal_id') or payload.get('bar_time') or '').strip()
    signal = f'{symbol}:{timeframe}:{action}:{stamp}' if stamp else ''
    store.event('WEBHOOK_RECEIVED', f'{action} {symbol} 3M')
    try:
        with LOCK:
            if signal and store.seen(signal):
                return jsonify({'ok': True, 'result': {'status': 'ignored', 'reason': 'duplicate_signal'}})
            if signal:
                store.remember(signal)
            result = process_signal(action)
            store.event('WEBHOOK_PROCESSED', json.dumps(result, ensure_ascii=False)[:450])
            return jsonify({'ok': True, 'result': result})
    except Exception as exc:
        message = safe_error(exc)
        store.event('WEBHOOK_ERROR', message)
        telegram(f'{BOT_NAME}\nERROR: {message}')
        logger.exception('Webhook error')
        return jsonify({'ok': False, 'error': message}), 500


@app.get('/monitor')
def monitor():
    if not auth(request.args.get('token'), MONITOR_SECRET):
        return jsonify({'error': 'No autorizado'}), 403
    try:
        position = sync_position()
        status = {'ok': True, 'position': position}
    except Exception as exc:
        status = {'ok': False, 'error': safe_error(exc)}
    return jsonify({'time': now(), 'bot': BOT_NAME, 'mode': store.mode(), 'sync': status, 'active_trade': store.get('active_trade', None), 'stats': summary(store.trades()), 'events': store.get('events', [])[-10:]})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '10000')))
