### 打印交易信息
import requests
import websocket
import json
import hmac
from dataclasses import dataclass
from time import sleep
import threading
import time, queue
import pandas as pd
from typing import Literal
from env import API_KEY, API_SECRET, LARK_URL, EXCHANGE, PRODUCT_TYPE
from util import send_lark_message, logger
from util import Exchange, decorator_try_catch, get_curr_time

BinanceOrderStatus = Literal["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"]

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
    status: BinanceOrderStatus # https://binance-docs.github.io/apidocs/spot/en/#public-api-definitions
    time: int

class CryptoMonitor:
    def __init__(self, exchange: str = EXCHANGE):
        self.exchange = exchange
        logger.info(f"self.exchange: {self.exchange}")
        self.api_url = "https://api.binance.com" if self.exchange == Exchange.binance else "https://api.bybit.com"
        if PRODUCT_TYPE == "linear" and self.exchange == Exchange.binance:
            self.api_url = "https://fapi.binance.com"
        logger.info(f"api url: {self.api_url}")
        self.headers = {'X-MBX-APIKEY': API_KEY} if self.exchange == Exchange.binance else {}
        
        # Queue for storing txns stream
        self.txns = queue.Queue()
        self.exec_list: list[Execution] = []   # used to store list of execution
        self.order_list: list[RawOrder] = []   # used to store list of orders

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
        received_msg_ms: int = get_curr_time()
        execution: Execution = None
        order: RawOrder = None
        data = json.loads(message)
        # execution
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
        else:
            msg: str = 'unconsidered scenario!'
            logger.erro(msg)
            send_lark_message(LARK_URL, msg)
                
        if execution:
            side = 'B' if execution.side == 'buy' else 'S' # BUY => buy
            time_str = str(pd.to_datetime(execution.time, unit='ms').strftime('%Y-%m-%d %H:%M:%S.%f'))[:-3][-6:]
            msg_str = f"{execution.symbol[:-4]} |{side}|{execution.price}|{time_str}|{execution.quantity}|{round(execution.amount_usd,1)} {execution.client_order_id} [{execution.receive_exec_latency_ms}ms]"
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
            msg_str = f"{order.symbol[:-4]} |{side}|{order.price}|{time_str}|{order.leaves_qty}|{round(amount_usd,1)} {order.order_link_id} {order.status}"
            if order.status in ["NEW"]:
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
        if self.exchange == Exchange.bybit:
            self.authenticate_with_bybit(ws)
            time.sleep(1)  # Wait for authentication to complete
            subscription_msg = {
                "op": "subscribe",
                "args": ["execution", "order"]
            }
            # Send subscription message
            ws.send(json.dumps(subscription_msg))

    def authenticate_with_bybit(self, ws):
        # Generate expires
        expires = int((time.time() + 1) * 1000)

        # Generate signature
        signature = str(hmac.new(
            bytes(API_SECRET, "utf-8"),
            bytes(f"GET/realtime{expires}", "utf-8"), digestmod="sha256"
        ).hexdigest())

        # Authenticate with API
        ws.send(json.dumps({
            "op": "auth",
            "args": [API_KEY, expires, signature]
        }))

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
            elif PRODUCT_TYPE == "linear":
                logger.info(f"creating ws for {PRODUCT_TYPE} ")
                response = requests.post(f"{self.api_url}/fapi/v1/listenKey", headers=self.headers)
                listenKey = response.json()['listenKey']
                self.ws = websocket.WebSocketApp(f"wss://fstream.binance.com/ws/{listenKey}",
                                            on_open=self.on_open,
                                            on_message=self.on_message,
                                            on_error=self.on_error,
                                            on_close=self.on_close)
        elif self.exchange == Exchange.bybit:
            self.ws = websocket.WebSocketApp(f"ws://stream.bybit.timeresearch.biz/v5/private",
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

    def send_bybit_heartbeat(self):
        # Function to send heartbeat to Bybit's WebSocket API https://bybit-exchange.github.io/docs/v5/ws/connect#how-to-send-the-heartbeat-packet
        def run():
            while True:
                try:
                    self.ws.send(json.dumps({'op': 'ping'}))
                    time.sleep(20)
                except Exception as e:
                    logger.error(f"Error sending heartbeat to Bybit: {e}")
                    break
        heartbeat_thread = threading.Thread(target=run)
        heartbeat_thread.start()

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
                elif self.exchange == Exchange.bybit:
                    self.send_bybit_heartbeat()
                self.ws.run_forever()
            except Exception as e:
                logger.error(f"{self.exchange} WebSocket connection failed: {e}, attempting to reconnect...")
            # finally:
            #     if self.exchange == Exchange.binance and keep_alive_thread.is_alive():
            #         keep_alive_thread.join(timeout=1)  # Ensure the keep-alive thread is not left hanging
            time.sleep(10)  # Wait a bit before attempting to reconnect


if __name__ == '__main__':
    monitor = CryptoMonitor()
    monitor.start()