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

# BOT PETROLEO WTI 5 MIN - ORDENES REALES
# SHORT/LONG: abren solo si FLAT. XL/XS: cierran el lado correspondiente.
# Sin reversion automatica, sin TP/SL añadidos, sin simulacion.
BOT_NAME = 'BOT PETRÓLEO WTI 5M'
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
STATE_PREFIX = 'bot_petroleo_wti_5m_v1'  # Historial 5M independiente; el 3M permanece en bot_petroleo_wti_3m_v1.
MODES = ('OFF', 'LONG_ONLY', 'SHORT_ONLY', 'CLOSE_ONLY', 'BOTH')
LOCK = threading.RLock()
PNL_LOCK = threading.Lock()
# Entrada opuesta válida solo junto al cierre de la MISMA vela de 5m.
# No añade temporizador/espera para ejecutar órdenes.
PAIR_MAX_DELAY_SECONDS = 45
PAIR_BAR_SECONDS = 300
PAIR_KEY = 'pending_opposite_entry'
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('wti-5m')


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
    """BingX devuelve milisegundos numéricos, a veces como texto."""
    if value is None or isinstance(value, bool):
        return 0
    try:
        if isinstance(value, (int, float, Decimal)) or (
            isinstance(value, str) and value.strip().lstrip('+-').isdigit()
        ):
            n = int(value)
            return n if abs(n) > 10**12 else n * 1000
        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
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
    # La conciliacion requiere varias consultas. No puede bloquear al webhook:
    # TradingView debe poder enviar la siguiente entrada inmediatamente.
    thread = threading.Thread(target=background_pnl_check, daemon=True)
    thread.start()
    return {'status': 'closed', 'trade_id': trade['id'], 'pnl_status': 'conciliando'}


def background_pnl_check():
    try:
        time.sleep(4)  # breve espera para que BingX publique los movimientos
        reconcile_pending(force=True)
    except Exception as exc:
        store.event('PNL_ERROR', safe_error(exc))


# ============================================================
# CONCILIACION WTI: MOVIMIENTOS REALES E IDs DE EJECUCION
# Los eventos de ingreso se asignan de forma EXCLUSIVA a una orden.
# Primero tradeId de BingX, despues la hora exacta de la orden.
# Si una asignacion no es unica, se deja pendiente. Nunca se inventa PnL.
# ============================================================

def income_rows(start_ms, end_ms):
    """Ingreso real de BingX, sin filtro de simbolo (compatible WTI)."""
    data = bingx.call('GET', '/openApi/swap/v2/user/income', {
        'startTime': int(start_ms), 'endTime': int(end_ms), 'limit': 1000,
    })
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = next((data[k] for k in ('income','list','rows','records','items','data')
                     if isinstance(data.get(k), list)), None)
    else:
        rows = None
    if not isinstance(rows, list) or any(not isinstance(x, dict) for x in rows):
        store.event('PNL_DIAGNOSTIC', 'income estructura=' + pnl_shape(data))
        raise RuntimeError('BingX devolvio un historial de ingresos desconocido')
    if len(rows) >= 1000:
        # La API limita a 1000; dividimos sin omitir eventos de frontera.
        if end_ms - start_ms < 1000:
            raise RuntimeError('Demasiados movimientos en el mismo segundo; PnL pendiente')
        middle = (start_ms + end_ms) // 2
        return income_rows(start_ms, middle) + income_rows(middle + 1, end_ms)
    allowed = {'REALIZED_PNL', 'TRADING_FEE', 'FUNDING_FEE'}
    return [r for r in rows
            if str(r.get('symbol','')).upper() == SYMBOL
            and str(r.get('incomeType','')).upper() in allowed
            and str(r.get('asset','USDT')).upper() == 'USDT'
            and r.get('income') is not None]


def merge_intervals(ranges):
    merged = []
    for start, end in sorted(ranges):
        if not merged or start > merged[-1][1] + 1:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return merged


def order_data(order_id, expected_side):
    """Consulta solo lectura; el precio se acepta solo si la orden es FILLED."""
    if not order_id:
        return None
    d = bingx.order_detail(order_id)
    if str(d.get('orderId') or d.get('orderID') or '') != str(order_id):
        raise RuntimeError('El ID de orden devuelto no coincide')
    if str(d.get('symbol', SYMBOL)).upper() != SYMBOL:
        raise RuntimeError('La orden pertenece a otro contrato')
    status = str(d.get('status', '')).upper()
    if status != 'FILLED':
        raise RuntimeError('La orden aun no consta FILLED en BingX')
    if str(d.get('side','')).upper() not in ('', expected_side):
        raise RuntimeError('La direccion de orden no coincide')
    px = number(d.get('avgPrice'))
    stamp = epoch_ms(d.get('updateTime') or d.get('time'))
    return {'price': px, 'time': stamp}


def fills_trade_ids(order_id, start_ms, end_ms):
    """Lee tradeIds; no presupone qty/realizedPnl en la respuesta."""
    if not order_id:
        return set()
    for unit in ('COIN', 'CONT'):
        try:
            data = bingx.call('GET', '/openApi/swap/v2/trade/allFillOrders', {
                'tradingUnit': unit, 'startTs': int(start_ms),
                'endTs': int(end_ms), 'orderId': str(order_id), 'currency': 'USDT',
            })
        except Exception:
            continue
        if isinstance(data, dict):
            rows = next((data[k] for k in ('fill_orders', 'fills', 'list','data','orders')
                         if isinstance(data.get(k), list)), None)
        elif isinstance(data, list):
            rows = data
        else:
            rows = None
        if not isinstance(rows, list) or len(rows) >= 1000:
            continue
        matching = [r for r in rows if isinstance(r, dict)
                    and str(r.get('orderId') or r.get('orderID') or '') == str(order_id)
                    and str(r.get('symbol', SYMBOL)).upper() == SYMBOL]
        ids = {str(r['tradeId']) for r in matching if r.get('tradeId')}
        if ids:
            return ids
    return set()


def load_trade_anchors(trades, indexes, earliest, latest):
    """Incluye trades confirmados para impedir reasignarles movimientos."""
    anchors = []
    order_cache = {}
    ids_cache = {}
    for index in indexes:
        trade = trades[index]
        start = epoch_ms(trade.get('opened_at'))
        end = epoch_ms(trade.get('closed_at'))
        if not start or not end or end < start:
            continue
        side = str(trade.get('side','')).upper()
        if side not in ('LONG', 'SHORT'):
            continue
        for role, order_id, direction, nominal in (
            ('open', str(trade.get('open_order_id') or ''),
             'BUY' if side == 'LONG' else 'SELL', start),
            ('close', str(trade.get('close_order_id') or ''),
             'SELL' if side == 'LONG' else 'BUY', end),
        ):
            info = None
            ids = set()
            if order_id:
                if order_id not in order_cache:
                    try:
                        order_cache[order_id] = order_data(order_id, direction)
                    except Exception as exc:
                        store.event('PNL_ORDER_INFO', safe_error(exc))
                        order_cache[order_id] = None
                info = order_cache[order_id]
            anchor_time = info['time'] if info and info['time'] else nominal
            anchors.append({'index': index, 'role': role, 'time': anchor_time,
                            'ids': ids, 'price': info['price'] if info else 0.0,
                            'order_id': order_id})
    # Solo investigar tradeIds cuando dos órdenes distintas están muy juntas.
    # Reduce peticiones y evita golpear los límites de tasa de BingX.
    ambiguous_anchors = [a for a in anchors
                         if a['order_id'] and any(
                             a is not b and abs(a['time'] - b['time']) <= 12000
                             for b in anchors)]
    for a in ambiguous_anchors[:16]:
        order_id = a['order_id']
        if order_id not in ids_cache:
            ids_cache[order_id] = fills_trade_ids(
                order_id, max(0, a['time'] - 180000),
                min(int(time.time()*1000), a['time'] + 180000))
        a['ids'] = ids_cache[order_id]
    return anchors


def ledger_key(row):
    # Los tranId son identificadores unicos de movimiento en BingX.
    tran_id = str(row.get('tranId') or '')
    if tran_id:
        return 'tran:' + tran_id
    # Dos movimientos SIN identificador y de mismo importe pueden ser validos.
    # Sus indices originales se preservan; no fusionamos filas indistinguibles.
    return None


def assign_income(row, anchors, trades):
    """Retorna (indice_trade, open/close/funding) o None si es ambiguo."""
    kind = str(row.get('incomeType','')).upper()
    event_time = epoch_ms(row.get('time'))
    if not event_time:
        return None
    trade_id = str(row.get('tradeId') or '')
    # Prioridad 1: coincidencia exacta por tradeId de una ejecucion.
    if trade_id and kind != 'FUNDING_FEE':
        matches = [a for a in anchors if trade_id in a['ids']
                   and (kind != 'REALIZED_PNL' or a['role'] == 'close')]
        if len(matches) == 1:
            a = matches[0]
            return a['index'], a['role']
        if len(matches) > 1:
            return None
    # Prioridad 2: coincidencia temporal unica, sin ventanas solapadas.
    if kind == 'FUNDING_FEE':
        possible = []
        for idx, t in enumerate(trades):
            if str(t.get('symbol', SYMBOL)).upper() != SYMBOL:
                continue
            start, end = epoch_ms(t.get('opened_at')), epoch_ms(t.get('closed_at'))
            if start and end and start <= event_time <= end:
                possible.append(idx)
        if len(possible) == 1:
            return possible[0], 'funding'
        return None
    eligible = [a for a in anchors
                if kind == 'TRADING_FEE' or a['role'] == 'close']
    ranked = sorted(((abs(a['time'] - event_time), a) for a in eligible
                    if abs(a['time'] - event_time) <= 90000), key=lambda x: x[0])
    if not ranked:
        return None
    distance, nearest = ranked[0]
    # Si hay una orden competidora a menos de dos segundos de diferencia,
    # no se atribuye la comision de forma arbitraria.
    if len(ranked) > 1 and abs(ranked[1][0] - distance) <= 2000:
        return None
    return nearest['index'], nearest['role']


def reconcile_pending(force=False):
    """Unico sincronizador: no cambia señales ni coloca ordenes."""
    with PNL_LOCK:
        last = number(store.get('last_reconcile', 0))
        if not force and time.time() - last < 45:
            return {'checked': False, 'reason': 'cooldown'}
        store.set('last_reconcile', time.time())
        trades = store.trades()
        pending = [i for i, t in enumerate(trades) if t.get('pnl_pending')]
        missing_prices = [i for i, t in enumerate(trades)
                          if not t.get('pnl_pending') and not number(t.get('exit_price'))
                          and t.get('close_order_id')]
        if not pending and not missing_prices:
            return {'checked': True, 'confirmed': 0, 'pending': 0,
                    'prices_updated': 0, 'reasons': []}
        # Consulta exit price de las órdenes incluso si faltan ingresos históricos.
        price_updates = 0
        for i in sorted(set(pending + missing_prices)):
            t = trades[i]
            close_id = str(t.get('close_order_id') or '')
            if not close_id or number(t.get('exit_price')):
                continue
            side = str(t.get('side','')).upper()
            try:
                info = order_data(close_id, 'SELL' if side == 'LONG' else 'BUY')
            except Exception as exc:
                store.event('PRICE_PENDING', safe_error(exc))
                continue
            if info and info['price'] > 0:
                t['exit_price'] = info['price']
                entry = number(t.get('entry_price'))
                if entry > 0:
                    t['price_move_pct'] = (info['price'] - entry) / entry * 100 * (1 if side == 'LONG' else -1)
                trades[i] = t
                price_updates += 1
        if price_updates:
            with LOCK:
                fresh = store.trades()
                changed_by_id = {str(trades[i].get('id')): trades[i]
                                 for i in sorted(set(pending + missing_prices))
                                 if number(trades[i].get('exit_price')) > 0}
                for j, old in enumerate(fresh):
                    new_price = changed_by_id.get(str(old.get('id')))
                    if new_price and not number(old.get('exit_price')):
                        old.update({'exit_price': new_price['exit_price'],
                                    'price_move_pct': new_price.get('price_move_pct', 0)})
                        fresh[j] = old
                store.set('trades', fresh)
        if not pending:
            return {'checked': True, 'confirmed': 0, 'pending': 0,
                    'prices_updated': price_updates, 'reasons': []}
        # Agrupar períodos cercanos y consultar movimientos reales una sola vez.
        ranges = []
        for i in pending:
            a, b = epoch_ms(trades[i].get('opened_at')), epoch_ms(trades[i].get('closed_at'))
            if a and b and b >= a:
                ranges.append((max(0, a - 120000), min(int(time.time()*1000), b + 120000)))
        if not ranges:
            raise RuntimeError('Los trades pendientes no tienen fechas válidas')
        ranges = merge_intervals(ranges)
        rows = []
        seen = set()
        for a, b in ranges:
            # Devolver error en vez de truncar 1000 eventos.
            for r in income_rows(a, b):
                key = ledger_key(r)
                if key and key in seen:
                    continue
                if key:
                    seen.add(key)
                rows.append(r)
        earliest = min(r[0] for r in ranges)
        latest = max(r[1] for r in ranges)
        nearby = [i for i, t in enumerate(trades)
                  if str(t.get('symbol', SYMBOL)).upper() == SYMBOL
                  and any(epoch_ms(t.get('opened_at')) <= b + 120000
                          and epoch_ms(t.get('closed_at')) >= a - 120000
                          for a,b in ranges)]
        anchors = load_trade_anchors(trades, nearby, earliest, latest)
        assigned = {i: {'open': [], 'close': [], 'funding': []} for i in pending}
        ambiguous = 0
        for row in rows:
            association = assign_income(row, anchors, trades)
            if not association:
                ambiguous += 1
                continue
            i, role = association
            if i in assigned:
                assigned[i][role].append(row)
        confirmed, reasons = 0, []
        for i in pending:
            t = trades[i]
            events = assigned[i]
            opening_fees = [r for r in events['open'] if str(r.get('incomeType')).upper() == 'TRADING_FEE']
            closing_fees = [r for r in events['close'] if str(r.get('incomeType')).upper() == 'TRADING_FEE']
            realized = [r for r in events['close'] if str(r.get('incomeType')).upper() == 'REALIZED_PNL']
            funding = [r for r in events['funding'] if str(r.get('incomeType')).upper() == 'FUNDING_FEE']
            if not opening_fees or not closing_fees or not realized:
                reasons.append(f"{t.get('side')}: falta movimiento preciso de "
                               + ('apertura' if not opening_fees else 'cierre' if not closing_fees else 'PnL'))
                continue
            gross = sum((dec(r['income']) for r in realized), Decimal('0'))
            fee_signed = sum((dec(r['income']) for r in opening_fees + closing_fees), Decimal('0'))
            funding_signed = sum((dec(r['income']) for r in funding), Decimal('0'))
            net = gross + fee_signed + funding_signed
            updated = dict(t)
            updated.update({'gross_pnl': float(gross), 'total_fees': float(-fee_signed),
                            'funding': float(funding_signed), 'net_pnl': float(net),
                            'pnl_pending': False, 'pnl_source': 'BINGX_INCOME_EXCLUSIVE',
                            'pnl_confirmed_at': now()})
            risk = number(t.get('risk_usd'))
            updated['r_multiple'] = float(net) / risk if risk > 0 else 0
            with LOCK:
                fresh = store.trades()
                for j, old in enumerate(fresh):
                    if str(old.get('id')) == str(updated.get('id')) and old.get('pnl_pending'):
                        fresh[j] = updated
                        store.set('trades', fresh)
                        confirmed += 1
                        break
            if not updated.get('pnl_pending'):
                trades[i] = updated
            store.event('PNL_CONFIRMED',
                        f"{updated.get('side')} neto {float(net):+.4f} USDT")
            telegram(f'{BOT_NAME}\nPnL real confirmado: {float(net):+.4f} USDT')
        store.event('PNL_DIAGNOSTIC',
                    f'income={len(rows)} movimientos, sin asignacion segura={ambiguous}; '
                    f'pendientes={len(pending)-confirmed}')
        return {'checked': True, 'confirmed': confirmed,
                'pending': len(pending)-confirmed, 'prices_updated': price_updates,
                'reasons': reasons[:5]}


def bar_start_ms(payload):
    """La marca {{time}} de TradingView es el INICIO de la vela de 5m."""
    raw = str(payload.get('bar_time') or '').strip()
    # No utilizar un ID arbitrario como si fuese una hora de vela.
    return epoch_ms(raw) if raw and '{{' not in raw else 0


def fresh_paired_bar(start_ms):
    """No recuperar entradas de velas históricas o alertas muy tardías."""
    if not start_ms:
        return False
    elapsed = time.time() * 1000 - (start_ms + PAIR_BAR_SECONDS * 1000)
    # Se admite un pequeño desfase de reloj, pero nunca señales viejas.
    return -30000 <= elapsed <= PAIR_MAX_DELAY_SECONDS * 1000


def clear_paired_entry(reason=None):
    item = store.get(PAIR_KEY, None)
    if item:
        store.set(PAIR_KEY, None)
        if reason:
            store.event('PAIR_CANCELLED', reason)


def get_fresh_paired_entry():
    item = store.get(PAIR_KEY, None)
    if not item:
        return None
    if (not fresh_paired_bar(int(item.get('bar_ms') or 0))
            or time.time() - number(item.get('received_at')) > PAIR_MAX_DELAY_SECONDS):
        clear_paired_entry('Entrada opuesta vencida; no ejecutada')
        return None
    return item


def entry_allowed(action, mode):
    return ((action == 'LONG' and mode in ('LONG_ONLY', 'BOTH'))
            or (action == 'SHORT' and mode in ('SHORT_ONLY', 'BOTH')))


def process_signal(action, payload=None):
    """Coordina dos alertas independientes, sin invertir por una sola salida.

    Es invocado dentro de LOCK. Una entrada opuesta recibida antes de su XL/XS
    permanece en Upstash unos segundos. Al llegar el cierre de la MISMA vela,
    espera únicamente a que BingX confirme FLAT y entonces abre la entrada.
    """
    payload = payload or {}
    mode = store.mode()
    if mode == 'OFF':
        clear_paired_entry('Modo OFF')
        return {'status': 'ignored', 'reason': 'mode_off'}

    pending = get_fresh_paired_entry()
    bar_ms = bar_start_ms(payload)

    if action in ('XL', 'XS'):
        direction = 'LONG' if action == 'XL' else 'SHORT'
        paired_action = 'SHORT' if action == 'XL' else 'LONG'
        should_pair = bool(
            pending
            and pending.get('entry') == paired_action
            and pending.get('exit') == action
            and bar_ms
            and int(pending.get('bar_ms') or 0) == bar_ms
            and pending.get('symbol') == str(payload.get('symbol', '')).upper().strip()
            and entry_allowed(paired_action, mode)
            and fresh_paired_bar(bar_ms)
        )
        # Si la posición cambió mientras esperábamos, no usar una señal vieja.
        if should_pair and pending.get('position_id'):
            live_positions = bingx.positions()
            should_pair = (
                len(live_positions) == 1
                and live_positions[0]['side'] == direction
                and str(live_positions[0].get('position_id') or '') == str(pending['position_id'])
            )
            if not should_pair:
                clear_paired_entry('Posición original ya no coincide')

        closed = close_trade(direction, f'tradingview_{action}_5m')
        if not should_pair or closed.get('status') != 'closed':
            return closed

        # El cierre ya está confirmado FLAT en close_trade().
        # Limpiar antes de intentar abrir: jamás reintentar a ciegas una MARKET.
        clear_paired_entry()
        store.event('PAIR_MATCHED', f'{action} + {paired_action} de la misma vela; FLAT confirmado')
        try:
            opened = open_trade(paired_action)
        except Exception as exc:
            store.event('PAIR_OPEN_ERROR', safe_error(exc))
            raise
        return {'status': 'closed_and_opened', 'close': closed, 'entry': opened}

    if action not in ('LONG', 'SHORT') or not entry_allowed(action, mode):
        return {'status': 'ignored', 'reason': 'mode_disallows_entry'}

    live = sync_position()
    if not live:
        # Entrada normal en FLAT: se ejecuta inmediatamente sin esperar a XL/XS.
        clear_paired_entry()
        return open_trade(action)
    if live['side'] == action:
        return {'status': 'ignored', 'reason': 'position_already_open'}

    # Entrada opuesta recibida antes del cierre: solo emparejar misma vela.
    if not fresh_paired_bar(bar_ms):
        return {'status': 'ignored', 'reason': 'opposite_position_open_without_fresh_bar_time'}
    exit_action = 'XL' if live['side'] == 'LONG' else 'XS'
    if pending and pending.get('bar_ms') == bar_ms and pending.get('entry') != action:
        return {'status': 'ignored', 'reason': 'conflicting_pending_signal'}
    store.set(PAIR_KEY, {
        'entry': action,
        'exit': exit_action,
        'bar_ms': bar_ms,
        'symbol': str(payload.get('symbol', '')).upper().strip(),
        'position_id': str(live.get('position_id') or ''),
        'received_at': time.time(),
    })
    store.event('ENTRY_QUEUED', f'{action} espera {exit_action} de la misma vela; no se abrió orden')
    return {'status': 'queued_for_matching_exit', 'entry': action, 'requires': exit_action}


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
    """Todos los indicadores incluyen SOLO trades realizados y confirmados.

    El rendimiento y DD% se calculan como indice acumulado por cada retorno
    neto / capital disponible ANTES de esa operacion. Esto neutraliza entradas
    de capital entre trades sin tratar los depositos como PnL. Cuando falta
    un balance_before se deja el porcentaje sin dato: no se inventa.
    """
    done = [t for t in trades if not t.get('pnl_pending')]
    done.sort(key=lambda t: (epoch_ms(t.get('closed_at')), str(t.get('id',''))))
    vals = [number(t.get('net_pnl')) for t in done]
    wins = [v for v in vals if v > 0.000001]
    losses = [v for v in vals if v < -0.000001]
    curve = [0.0]
    peak = 0.0
    dd = 0.0
    max_win = max_loss = streak_w = streak_l = 0
    nav = Decimal('1')
    max_nav = Decimal('1')
    dd_pct = Decimal('0')
    pct_complete = True
    for t, value in zip(done, vals):
        total = curve[-1] + value
        curve.append(total)
        peak = max(peak, total)
        dd = max(dd, peak - total)
        streak_w = streak_w + 1 if value > 0.000001 else 0
        streak_l = streak_l + 1 if value < -0.000001 else 0
        max_win = max(max_win, streak_w)
        max_loss = max(max_loss, streak_l)
        base = number(t.get('balance_before'))
        if base <= 0 or base + value <= 0:
            pct_complete = False
        elif pct_complete:
            nav *= Decimal('1') + dec(value) / dec(base)
            max_nav = max(max_nav, nav)
            dd_pct = max(dd_pct, (max_nav - nav) / max_nav * 100)
    if not done or not pct_complete:
        ret_pct = None
        max_dd_pct = None
    else:
        ret_pct = float((nav - 1) * 100)
        max_dd_pct = float(dd_pct)
    longs = [t for t in done if str(t.get('side')).upper() == 'LONG']
    shorts = [t for t in done if str(t.get('side')).upper() == 'SHORT']
    return {
        'count': len(trades), 'confirmed': len(done),
        'pending': len(trades) - len(done),
        'wins': len(wins), 'losses': len(losses),
        'be': len(done) - len(wins) - len(losses),
        'winrate': 100 * len(wins) / len(done) if done else 0,
        'net': sum(vals), 'gross': sum(number(t.get('gross_pnl')) for t in done),
        'fees': sum(number(t.get('total_fees')) for t in done),
        'funding': sum(number(t.get('funding')) for t in done),
        'pf': sum(wins) / abs(sum(losses)) if losses else (999 if wins else 0),
        'max_dd': dd, 'max_dd_pct': max_dd_pct,
        'net_pct': ret_pct, 'curve': curve,
        'long_count': len(longs), 'short_count': len(shorts),
        'long_net': sum(number(t.get('net_pnl')) for t in longs),
        'short_net': sum(number(t.get('net_pnl')) for t in shorts),
        'best': max(vals, default=0), 'worst': min(vals, default=0),
        'avg_win': sum(wins) / len(wins) if wins else 0,
        'avg_loss': sum(losses) / len(losses) if losses else 0,
        'max_win_streak': max_win, 'max_loss_streak': max_loss,
        'r_total': sum(number(t.get('r_multiple')) for t in done),
    }


PANEL = r'''<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BOT PETRÓLEO WTI 5M</title>
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
<h1>🛢 BOT PETRÓLEO WTI 5M</h1>
<p class="center muted">{{symbol}} · 5M · {{leverage}}x · ISOLATED · {{percent}}% del balance · REAL</p>
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
<section class="card"><h2>Curva de PnL realizado confirmado</h2>
<h1 class="{{'green' if stats.net>=0 else 'red'}}">{{'%+.4f'|format(stats.net)}} USDT</h1>
<svg id="curve" viewBox="0 0 1000 260" preserveAspectRatio="none"></svg>
<p class="muted">DD y rentabilidad porcentual: índice estimado con PnL realizado / balance disponible antes de cada entrada; los depósitos no se cuentan como ganancias. Si falta el capital histórico, aparecerá —. No se incluye PnL flotante.</p>
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
<table><thead><tr><th>Cierre</th><th>Dirección</th><th>Origen</th><th>Entrada</th><th>Salida</th><th>Movimiento</th><th>ROI neto %</th><th>Margen</th><th>Comisiones</th><th>Funding</th><th>PnL neto</th><th>R</th><th>Motivo</th></tr></thead>
<tbody>{% for t in trades %}<tr>
<td>{{t.closed_at[:19]}}</td><td>{{t.side}}</td><td>{{t.source}}</td>
<td>{{'%.4f'|format(t.entry_price)}}</td>
<td>{{'%.4f'|format(t.exit_price) if t.exit_price else 'Pendiente'}}</td>
<td>{{'%.3f'|format(t.price_move_pct) ~ '%' if t.exit_price else '—'}}</td>
<td>{{'%+.2f'|format(t.net_pnl/t.margin_used*100) ~ '%' if not t.pnl_pending and t.margin_used>0 else '—'}}</td>
<td>${{'%.2f'|format(t.margin_used)}}</td><td>${{'%.4f'|format(t.total_fees)}}</td><td>{{'%+.4f'|format(t.funding)}}</td>
<td class="{{'green' if t.net_pnl>=0 else 'red'}}">{{'Pendiente' if t.pnl_pending else '%+.4f'|format(t.net_pnl)}}</td>
<td>{{'%.2f'|format(t.r_multiple) ~ 'R' if t.risk_usd>0 else '—'}}</td><td>{{t.close_reason}}</td></tr>
{% else %}<tr><td colspan="13" class="muted">No hay operaciones registradas.</td></tr>{% endfor %}</tbody></table></section>
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
    return jsonify({'ok': True, 'bot': BOT_NAME, 'symbol': SYMBOL, 'mode': store.mode(), 'timeframe': '5m', 'state_prefix': STATE_PREFIX, 'redis': store.redis, 'version': 'WTI-5M-REAL-PNL-8-SAME-BAR-PAIR'})


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
    # El PnL se concilia tras cierres en segundo plano y al pulsar SINCRONIZAR.
    # Abrir el panel no dispara consultas extensas a BingX.
    trades = store.trades()
    stats = summary(trades)
    cards = [
        ('Operaciones', stats['count']), ('Confirmadas', stats['confirmed']), ('Pendientes', stats['pending']),
        ('Ganadas', stats['wins']), ('Perdidas', stats['losses']), ('Winrate', f"{stats['winrate']:.1f}%"),
        ('Profit Factor', f"{stats['pf']:.2f}"), ('PnL neto', f"${stats['net']:+.4f}"), ('Rentabilidad est. %', f"{stats['net_pct']:+.2f}%" if stats['net_pct'] is not None else '—'), ('DD realizado est. %', f"{stats['max_dd_pct']:.2f}%" if stats['max_dd_pct'] is not None else '—'),
        ('PnL bruto', f"${stats['gross']:+.4f}"), ('Comisiones', f"${stats['fees']:.4f}"),
        ('Funding', f"${stats['funding']:+.4f}"), ('DD realizado USDT', f"${stats['max_dd']:.4f}"),
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
        clear_paired_entry('Cambio de modo: señal pendiente descartada')
    store.event('MODE', f'Modo cambiado a {mode}')
    telegram(f'{BOT_NAME}\nModo: {mode}')
    return redirect('/control?secret=' + request.args['secret'])


@app.post('/sync')
def sync():
    if not panel_authorized():
        return 'No autorizado', 403
    try:
        with LOCK:
            live = sync_position()
        store.event('SYNC', f"Posicion: {live['side'] if live else 'FLAT'}")
    except Exception as exc:
        store.event('SYNC_ERROR', safe_error(exc))
    try:
        result = reconcile_pending(force=True)
        store.event('PNL_CHECK', f"Confirmadas {result.get('confirmed', 0)}, pendientes {result.get('pending', 0)}; precios {result.get('prices_updated', 0)}; {result.get('reasons', [])[:2]}")
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
    return send_file(io.BytesIO(content), mimetype='application/json', as_attachment=True, download_name='wti_5m_historial.json')


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
    return send_file(io.BytesIO(output.getvalue().encode('utf-8-sig')), mimetype='text/csv', as_attachment=True, download_name='wti_5m_historial.csv')


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
    if timeframe not in ('5', '5m'):
        return jsonify({'error': 'Temporalidad debe ser 5M'}), 400
    stamp = str(payload.get('signal_id') or payload.get('bar_time') or '').strip()
    signal = f'{symbol}:{timeframe}:{action}:{stamp}' if stamp else ''
    store.event('WEBHOOK_RECEIVED', f'{action} {symbol} 5M')
    try:
        with LOCK:
            if signal and store.seen(signal):
                return jsonify({'ok': True, 'result': {'status': 'ignored', 'reason': 'duplicate_signal'}})
            if signal:
                store.remember(signal)
            result = process_signal(action, payload)
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
