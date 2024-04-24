import os

################# ACCOUNT ################
EXCHANGE = os.environ.get('EXCHANGE', "binance")
API_KEY = os.environ.get('API_KEY', '')
API_SECRET = os.environ.get('API_SECRET', '')
PRODUCT_TYPE = os.environ.get('PRODUCT_TYPE', "spot")

LARK_URL: str = os.environ.get('LARK_URL', "")

