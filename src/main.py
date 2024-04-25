### 打印交易信息
import requests
import websocket
import json
import hmac
import base64
import hashlib
from dataclasses import dataclass
from time import sleep
import threading
import time, queue
import pandas as pd
from typing import Literal
from env import API_KEY, API_SECRET, PASSPHRASE, LARK_URL, EXCHANGE, PRODUCT_TYPE
from util import send_lark_message, logger
from util import Exchange, decorator_try_catch, get_curr_time

BinanceOrderStatus = Literal["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"]
OkxOrderStatus = Literal["canceled", "live", "partially_filled", "filled", "mmp_canceled"]

@dataclass
class Execution:
    symbol: str
    price: float
    quantity: float
    amount_usd: float
    side: str
    orderId: str
    client_order_id: str
    is_maker: bool
    time: int
    receive_exec_latency_ms: int # 订单成交到收到间隔了多久 ms

@dataclass
class RawOrder:
    symbol: str
    price: float
    side: str
    order_id: str
    order_link_id: str
    leaves_qty: float
    status: BinanceOrderStatus | OkxOrderStatus # https://binance-docs.github.io/apidocs/spot/en/#public-api-definitions
    time: int

class CryptoMonitor:
    def __init__(self, exchange: str = EXCHANGE):
        self.exchange = exchange
        logger.info(f"self.exchange: {self.exchange}")
        self.api_url = "https://api.binance.com"
        if PRODUCT_TYPE == "linear" and self.exchange == Exchange.binance:
            self.api_url = "https://fapi.binance.com"
        if self.exchange == Exchange.okx:
            self.api_url = "wss://ws.okx.com:8443/ws/v5/private"

        logger.info(f"api url: {self.api_url}")
        self.headers = {'X-MBX-APIKEY': API_KEY} if self.exchange == Exchange.binance else {}
        
        # Queue for storing txns stream
        self.txns = queue.Queue()
        self.exec_list: list[Execution] = []   # used to store list of execution
        self.order_list: list[RawOrder] = []   # used to store list of orders
        self.product = "spot" if PRODUCT_TYPE == "spot" else "linear"
        self.okx_product_type: float = "SPOT"if PRODUCT_TYPE == "spot" else "SWAP" 
        self.TXN_QUEUE_TIMEOUT = 1
    
    @decorator_try_catch
    def process_txns(self):
        """ Process and send transaction messages to the channel. """
        logger.info('process txns started!')
        while True:
            try:
                all_msg = ''
                while True:
                    try:
                        msg = self.txns.get(timeout = self.TXN_QUEUE_TIMEOUT)  # timeout after 5 seconds if queue is empty
                        # logger.info(f'Processing message: {msg}')
                        all_msg += msg + '\n'
                        self.txns.task_done()
                    except queue.Empty:
                        break  # break the inner loop if the queue is empty

                if all_msg:
                    # 打印交易信息
                    send_lark_message(LARK_URL, all_msg.strip())  # Send the concatenated messages
                    
            except Exception as e:
                logger.warning(f"Exception occurred in process_txns: {e}")
                sleep(5)

    @decorator_try_catch
    def format_txn_message(self, message):
        """ process txn message """
        # logger.info(f'recevied raw message: {message}')
        if message == "pong":
            return
        received_msg_ms: int = get_curr_time()
        execution: Execution = None
        order: RawOrder = None
        data = json.loads(message)
        if self.exchange == Exchange.binance and PRODUCT_TYPE == "spot" and data['e'] == 'executionReport' and 'x' in data.keys():
            if data['x'] == 'TRADE': # process execution
                logger.info(f"ws received execution: {data}")
                quantity: float = float(data['l'])
                price: float = float(data['L'])
                amount_usd = quantity * price
                receive_exec_latency_ms: int = int(received_msg_ms - int(data['E']))
                execution = Execution(data['s'], price, quantity, amount_usd, data['S'].lower(), data["i"], data["c"], data["m"], int(data['E']), receive_exec_latency_ms)
            else: # process order
                leaves_qty: float = float(data["q"]) - float(data["z"])
                order = RawOrder(data["s"], float(data["p"]), data["S"].lower(), str(data["i"]), data["c"],
                                            leaves_qty, data["X"], int(data["T"]))
        elif self.exchange == Exchange.binance and PRODUCT_TYPE == "linear" and data['e'] == 'ORDER_TRADE_UPDATE' and 'o' in data.keys():
            order_detail: dict = data["o"].copy()
            if order_detail['x'] == 'TRADE':
                quantity = float(order_detail['l'])
                price = float(order_detail['L'])
                amount_usd = quantity * price
                receive_exec_latency_ms: int = int(received_msg_ms - int(data['E']))
                execution = Execution(order_detail['s'], price, float(order_detail['l']), amount_usd, order_detail['S'].lower(), order_detail["i"], order_detail["c"], order_detail["m"], int(data['E']), receive_exec_latency_ms)
            else:
                leaves_qty: float = float(order_detail["q"]) - float(order_detail["z"])
                order = RawOrder(order_detail["s"], float(order_detail["p"]), order_detail["S"].lower(), str(order_detail["i"]), data["c"],
                                            leaves_qty, order_detail["X"], int(order_detail["E"]))
        elif self.exchange == Exchange.okx and "arg" in data and data["arg"]["channel"] == 'orders' and "data" in data:
            logger.info(f"received okx info: {data}")
            for d in data["data"]:
                # {'instType': 'SWAP', 'instId': 'ETH-USDT-SWAP', 'tgtCcy': '', 'ccy': '', 'ordId': '1392847056980709376', 'clOrdId': '', 'algoClOrdId': '', 
                # 'algoId': '', 'tag': '', 'px': '3161.2', 'sz': '0.1', 'notionalUsd': '31.614528960000005', 'ordType': 'limit', 'side': 'buy', 'posSide': 'net',
                # 'tdMode': 'isolated', 'accFillSz': '0.1', 'fillNotionalUsd': '31.614528960000005', 'avgPx': '3161.2', 'state': 'filled', 'lever': '3', 'pnl': '0',
                # 'feeCcy': 'USDT', 'fee': '-0.0063224', 'rebateCcy': 'USDT', 'rebate': '0', 'category': 'normal', 'uTime': '1714012506315', 'cTime': '1714012477028',
                #'source': '', 'reduceOnly': 'false', 'cancelSource': '', 'quickMgnType': '', 'stpId': '', 'stpMode': 'cancel_maker', 'attachAlgoClOrdId': '', 'lastPx': '3161.67',
                #'isTpLimit': 'false', 'slTriggerPx': '', 'slTriggerPxType': '', 'tpOrdPx': '', 'tpTriggerPx': '', 'tpTriggerPxType': '', 'slOrdPx': '', 'fillPx': '3161.2',
                #'tradeId': '1216386262', 'fillSz': '0.1', 'fillTime': '1714012506314', 'fillPnl': '0', 'fillFee': '-0.0063224', 'fillFeeCcy': 'USDT', 'execType': 'M',
                # 'fillPxVol': '', 'fillPxUsd': '', 'fillMarkVol': '', 'fillFwdPx': '', 'fillMarkPx': '3161.62', 'amendSource': '', 'reqId': '', 'amendResult': '', 'code': '0', 'msg': '',
                #'pxType': '', 'pxUsd': '', 'pxVol': '', 'linkedAlgoOrd': {'algoId': ''}, 'attachAlgoOrds': []}
                state: OkxOrderStatus = d["state"]
                if state in ["partially_filled", "filled"]: # execution
                    amount_usd: float = float(d["px"]) * float(d["fillSz"])
                    is_maker: bool = d["ordType"] == "limit"
                    receive_exec_latency_ms: int = get_curr_time() - int(d["uTime"])
                    execution = Execution(d["instId"], float(d["px"]), float(d["fillSz"]), amount_usd, d["side"].lower(), d["ordId"], d["clOrdId"], is_maker, int(d["uTime"]), receive_exec_latency_ms)
                else: #"canceled", "live", "mmp_canceled"
                    order = RawOrder(d["instId"], float(d["px"]), d["side"].lower(), d["ordId"], d["clOrdId"],
                                                float(d["sz"]), state, int(d["uTime"]))

        else:
            msg: str = f'unconsidered scenario! {data}'
            logger.error(msg)
            send_lark_message(LARK_URL, msg)
                
        if execution:
            side = 'B' if execution.side == 'buy' else 'S' # BUY => buy
            time_str = str(pd.to_datetime(execution.time, unit='ms').strftime('%Y-%m-%d %H:%M:%S.%f'))[:-3][-6:]
            msg_str = f"{execution.symbol[:-4]}{self.product} |{side}|{execution.price}|{time_str}|{execution.quantity}|{round(execution.amount_usd,1)} {execution.client_order_id} [{execution.receive_exec_latency_ms}ms]"
            if execution.is_maker:
                msg_str = "**" + msg_str + "**"
            else:
                msg_str = "⚠️ " + msg_str
            logger.info(f"prepare to send exec info: {msg_str}")
            self.txns.put(msg_str)
        
        if order:
            side = 'B' if order.side == 'buy' else 'S' # BUY => buy
            time_str = str(pd.to_datetime(order.time, unit='ms').strftime('%Y-%m-%d %H:%M:%S.%f'))[:-3][-6:]
            amount_usd: float = order.price * order.leaves_qty
            msg_str = f"{order.symbol[:-4]}{self.product} |{side}|{order.price}|{time_str}|{order.leaves_qty}|{round(amount_usd,1)} {order.order_link_id} {order.status}"
            if order.status in ["NEW", "live"]:
                msg_str = "🟢 " + msg_str
            else:
                msg_str = "🟠 " + msg_str
            logger.info(f"prepare to send order info: {msg_str}")
            self.txns.put(msg_str)
    
        
    def on_message(self, ws, message):
        # logger.info(f"received message: {message}")
        self.format_txn_message(message)

    def on_error(self, ws, error):
        logger.info(f"error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.error("WebSocket closed")

    def on_open(self, ws):
        logger.info("WebSocket opened")
        if self.exchange == Exchange.okx:
            self.authenticate_with_okx(ws)
            time.sleep(1)  # Wait for authentication to complete
            self.subscribe_to_okx_streams(ws)
            def run(*args):
                while True:
                    time.sleep(20)  # Ping interval
                    ws.send('ping')  # Send a ping to the server
            thread = threading.Thread(target=run)
            thread.start()

    def authenticate_with_okx(self, ws):
        timestamp = str(int(time.time()))
        message = timestamp + 'GET' + '/users/self/verify'
        signature = hmac.new(bytes(API_SECRET, 'utf-8'), msg=message.encode('utf-8'), digestmod='sha256').digest()
        signature = base64.b64encode(signature).decode('utf-8')
        
        auth_msg = {
            "op": "login",
            "args": [{
                "apiKey": API_KEY,
                "passphrase": PASSPHRASE,  # Ensure you have a passphrase set correctly for OKX
                "timestamp": timestamp,
                "sign": signature
            }]
        }
        ws.send(json.dumps(auth_msg))

    def subscribe_to_okx_streams(self, ws):
        # Example: subscribing to trades and order book
        subscription_msg = {
            "op": "subscribe",
            "args": [
                {"channel": "orders", "instType": self.okx_product_type}
            ]
        }
        ws.send(json.dumps(subscription_msg))
    
    @decorator_try_catch
    def create_websocket(self):
        if self.exchange == Exchange.binance:
            if PRODUCT_TYPE == "spot":
                response = requests.post(f"{self.api_url}/api/v3/userDataStream", headers=self.headers)
                listenKey = response.json()['listenKey']
                self.ws = websocket.WebSocketApp(f"wss://stream.binance.com:9443/ws/{listenKey}",
                                            on_open=self.on_open,
                                            on_message=self.on_message,
                                            on_error=self.on_error,
                                            on_close=self.on_close)
        elif self.exchange == Exchange.okx:
            self.ws = websocket.WebSocketApp(self.api_url,
                                            on_open=self.on_open,
                                            on_message=self.on_message,
                                            on_error=self.on_error,
                                            on_close=self.on_close)
    
    def keep_listen_key_alive(self, listen_key):
        """ keep listen for bn """
        while True:
            try:
                if PRODUCT_TYPE == "spot":
                    requests.put(f"{self.api_url}/api/v3/userDataStream?listenKey={listen_key}", headers=self.headers)
                elif PRODUCT_TYPE == "linear":
                    requests.put(f"{self.api_url}/fapi/v3/userDataStream?listenKey={listen_key}", headers=self.headers)
                time.sleep(60 * 30)  # Renew listenKey every 30 minutes
            except Exception as e:
                logger.error(f"Error renewing listen key: {e}")
                break
    
    @decorator_try_catch
    def start(self):
        while True:
            try:
                self.create_websocket()
                # start a thread to send message
                transaction_log_thread = threading.Thread(target=self.process_txns)
                transaction_log_thread.daemon = True
                transaction_log_thread.start()

                if self.exchange == Exchange.binance:
                    listenKey = self.ws.url.split("/")[-1]
                    keep_alive_thread = threading.Thread(target=self.keep_listen_key_alive, args=(listenKey,), daemon = True)
                    keep_alive_thread.start()
                self.ws.run_forever()
            except Exception as e:
                logger.error(f"{self.exchange} WebSocket connection failed: {e}, attempting to reconnect...")
            time.sleep(10)  # Wait a bit before attempting to reconnect


if __name__ == '__main__':
    monitor = CryptoMonitor()
    monitor.start()