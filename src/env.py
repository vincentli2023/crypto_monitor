import os

################# ACCOUNT ################
EXCHANGE = os.environ.get('EXCHANGE', "okx")
API_KEY = os.environ.get('API_KEY', '7')
API_SECRET = os.environ.get('API_SECRET', '5C7CA5F')
PASSPHRASE = os.environ.get('PASSPHRASE', 'HoS')
PRODUCT_TYPE = os.environ.get('PRODUCT_TYPE', "linear") # spot or linear
LARK_URL: str = os.environ.get('LARK_URL', "https://open.larksuite.com/open-apis/bot/v2/hook/8a026021-0743-")

