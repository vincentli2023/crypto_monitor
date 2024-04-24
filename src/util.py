import json, requests, traceback
from time import time
from functools import wraps
import logging
logging.basicConfig(
    format="%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y/%m/%d-%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
    
class Exchange:
    binance = 'binance'
    bybit = 'bybit'

def get_curr_time(ms = True) -> int:
    """ get current time in ms (default) or second """
    curr_time = time()
    if ms:
        return int(curr_time * 1000)
    return int(curr_time)
    
def decorator_try_catch(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
            return result # MAKE IT RETURN WHAT WAS SUPPOSED TO RETURN, AVOID RETURNING NULL
        except Exception as e:
            logger.error(f"An exception occurred in function {f.__name__}: {e}")
            logger.error(f"Trackback: {traceback.format_exc()}")
            return None
    return wrapper


def send_lark_message(channel, msg: str) -> None:
    """
    send message to lark on a given channel
    ref: https://open.feishu.cn/document/ukTMukTMukTM/ucTM5YjL3ETO24yNxkjN
    """
    payload = {
        "msg_type": "interactive",
        "card": {
            "config": {
                "wide_screen_mode": True
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "content": msg,
                        "tag": "lark_md"
                    }
                }
            ]
        }
    }
    if not channel:
        return True
    # send the POST request
    response = requests.post(
        channel,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    trials = 0
    while response.status_code != 200 and trials <= 3:
        logging.error(f"Failed to send lark message, status code: {response.status_code}, message: {response.text}")
        # send the POST request
        response = requests.post(
            channel,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload)
        )
        trials += 1
    return True


