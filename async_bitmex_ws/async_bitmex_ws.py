# todo 有空提交到pypi

import asyncio
import time

import websockets
import traceback
import json
from loguru import logger

import urllib
import math
from util.api_key import generate_signature


# Naive implementation of connecting to BitMEX websocket for streaming realtime data.
# The Marketmaker still interacts with this as if it were a REST Endpoint, but now it can get
# much more realtime data without polling the hell out of the API.
#
# The Websocket offers a bunch of data as raw properties right on the object.
# On connect, it synchronously asks for a push of all this data then returns.
# Right after, the MM can start using its data. It will be updated in realtime, so the MM can
# poll really often if it wants.
class AsyncBitMEXWebsocket:
    # Don't grow a table larger than this amount. Helps cap memory usage.
    MAX_TABLE_LEN = 200

    symbolSubs = ["execution", "instrument", "order", "orderBookL2", "position", "quote", "trade"]
    genericSubs = ["margin"]

    @classmethod
    async def create_instance(cls, symbol='', api_key=None, api_secret=None, testnet=False, timeout=999999999999, ):
        instance = cls(symbol, api_key, api_secret, testnet, timeout)
        await instance.instantiation_complete.wait()
        return instance

    async def _reactivate(self):
        # close the old connection
        try:
            asyncio.create_task(self.ws.close())
            self._reactivate_task.cancel()
            self._reactivate_task = self.new_reactivate_task
        except:
            pass
        # build the new connection
        await self.__connect()
        logger.info('Activate successfully.')
        # instance instantiation complete
        self.instantiation_complete.set()

        # heartbeat
        _send_ping_after_task = asyncio.create_task(self._ping_pong())
        # handle new message
        async for message in self.ws:
            if message != 'pong':
                # new message clear heartbeat
                _send_ping_after_task.cancel()
                # activate new heartbeat
                _send_ping_after_task = asyncio.create_task(self._ping_pong())
            # self.last_msg_time = asyncio.get_running_loop().time()
            # handle new message
            asyncio.create_task(self.__on_message(message))

    async def _ping_pong(self):
        '''
        heartbeat
        '''
        while True:
            await asyncio.sleep(5)
            # timeout
            asyncio.create_task(self.ws.send('ping'))
            logger.info('ping')
            try:
                await asyncio.wait_for(self._wait_pong(), timeout=5)
            except TimeoutError:
                logger.warning('Heartbeat timeout, connection lost.Trying to reconnect.')
                self.new_reactivate_task = asyncio.create_task(self._reactivate())

        # async for news in self.new_message_watcher():
        #     # if timeout
        #     if asyncio.get_running_loop().time() - last_time > 5:
        #         # send 'ping'
        #         asyncio.create_task(self.ws.send('ping'))
        #         try:
        #             await asyncio.wait_for(self._wait_pong(), timeout=5)
        #         except:
        #             logger.warning('Heartbeat timeout, connection lost.Trying to reconnect.')
        #             asyncio.create_task(self._reconnect_remote())
        #     last_time = asyncio.get_running_loop().time()

    async def _wait_pong(self):
        async for news in self.new_message_watcher():
            if news == 'pong':
                logger.info("pong!")
                break

    def put_detect_hook(self, condition: dict):
        msg_receiver = asyncio.get_running_loop().create_future()
        self._detect_hook[msg_receiver] = condition
        return msg_receiver

    async def new_message_watcher(self, table: str = '', action: str = '', filter: dict = None):
        '''
        async for news in new_message_watcher():
            ...

        :param table:table
        :param action:action
        :param filter:For example:{'table':'orderBookL2_25','action':'update'}.For more info, refer https://testnet.bitmex.com/app/wsAPI
        '''
        if filter is None:
            filter = {}
        if bool(table):
            filter['table'] = table
        if bool(action):
            filter['action'] = action
        if 'table' in filter:
            partial = await asyncio.create_task(self._ensure_subscribed(filter['table']))
            yield partial
        hook = None
        while not hook:
            hook = self.put_detect_hook(filter)
            yield await hook
            hook = None

    def __init__(self, symbol='', api_key=None, api_secret=None, testnet=False, timeout=999999999999, ):
        '''Connect to the websocket and initialize data stores. timeout seems to have no effect.'''
        self.logger = logger
        self.logger.debug("Initializing WebSocket.")
        self.testnet = testnet
        self.symbol = symbol
        self.timeout = timeout
        self._detect_hook = {}
        self.instantiation_complete = asyncio.Event()

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self.api_key = api_key
        self.api_secret = api_secret

        self.data = {}
        self.keys = {}
        self.exited = False
        self._reactivate_task = asyncio.create_task(self._reactivate())

        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        # wsURL = self.__get_url

        # # Connected. Wait for partials
        # self.__wait_for_symbol(symbol)
        # if api_key:
        #     self.__wait_for_account()
        # self.logger.info('Got all market data. Starting.')

    async def exit(self):
        '''Call this to exit - will close websocket.'''
        self.exited = True
        await asyncio.create_task(self.ws.close())

    async def _ensure_subscribed(self, subject: str):
        '''
        Ensure the subject to be subscribed.

        :param subject:One among instrument, trade, quote, margin, position, orderBookL2, order, execution and so on
        :return: partial message
        '''
        # If subject has not been subscribed
        if subject not in self.data.keys():
            # subscribe
            asyncio.create_task(self.__send_command('subscribe', args=[
                f'{subject}:{self.symbol}'] if subject in self.symbolSubs else [f'{subject}']))
            # wait for 'partial'
            async for news in self.new_message_watcher():
                if isinstance(news, dict):
                    if news.get('table', '') == f"{subject}" and news.get("action", '') == "partial":
                        return news
                    # Not authenticated
                    # {"status": 401,
                    #  "error": "User requested an account-locked subscription but no authorization was provided.",
                    #  "meta": {
                    #      "notes": "Account-locked tables include the following: account,affiliate,execution,margin,order,position,privateNotifications,transact,wallet"},
                    #  "request": {"op": "subscribe", "args": ["margin"]}}
                    elif news.get('status', 0) == 401 and \
                            subject in news.get('request', {}).get('args', []) and \
                            subject in ['account', 'affiliate', 'execution', 'margin',
                                        'order', 'position', 'privateNotifications',
                                        'transact', 'wallet'] and \
                            news.get('error',
                                     '') == "User requested an account-locked subscription but no authorization was provided.":
                        raise ConnectionRefusedError(
                            'User requested an account-locked subscription but no authorization was provided.')

    async def get_instrument(self):
        '''Get the raw instrument data for this symbol.'''
        # Turn the 'tickSize' into 'tickLog' for use in rounding
        await self._ensure_subscribed('instrument')
        instrument = self.data['instrument'][0]
        instrument['tickLog'] = int(math.fabs(math.log10(instrument['tickSize'])))
        return instrument

    async def recent_trades(self):
        '''Get recent trades.'''
        await self._ensure_subscribed('trade')
        return self.data['trade']

    async def recent_quotes(self):
        '''Get recent quotes.'''
        await self._ensure_subscribed('quote')
        return self.data['quote']

    async def get_ticker(self):
        '''Return a ticker object. Generated from quote and trade.'''
        quote_task = asyncio.create_task(self.recent_quotes())
        trade_task = asyncio.create_task(self.recent_trades())
        instrument_task = asyncio.create_task(self.get_instrument())

        await quote_task
        await trade_task
        await instrument_task
        lastQuote = self.data['quote'][-1]
        lastTrade = self.data['trade'][-1]
        ticker = {
            "last": lastTrade['price'],
            "buy": lastQuote['bidPrice'],
            "sell": lastQuote['askPrice'],
            "mid": (float(lastQuote['bidPrice'] or 0) + float(lastQuote['askPrice'] or 0)) / 2
        }

        # The instrument has a tickSize. Use it to round values.
        instrument = self.data['instrument'][0]
        return {k: round(float(v or 0), instrument['tickLog']) for k, v in ticker.items()}

    async def margin(self):
        '''Get your margin details.'''
        await self._ensure_subscribed('margin')
        return self.data['margin'][0]

    async def positions(self):
        '''Get your positions.'''
        await self._ensure_subscribed('position')
        return self.data['position']

    async def market_depth(self):
        '''Get market depth (orderbook). Returns all levels.'''
        await self._ensure_subscribed('orderBookL2')
        return self.data['orderBookL2']

    async def open_orders(self, clOrdIDPrefix=''):
        '''Get all your open orders.'''
        await self._ensure_subscribed('order')
        orders = self.data['order']
        # Filter to only open orders and those that we actually placed
        return [o for o in orders if str(o['clOrdID']).startswith(clOrdIDPrefix) and order_leaves_quantity(o)]

    #
    # End Public Methods
    #

    async def __connect(self):
        url = self.__get_url()
        self.ws = await websockets.connect(url, extra_headers=self.__get_auth())
        self.logger.info(f'Connected to {url}.')

    def __get_auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''
        if self.api_key:
            self.logger.info("Authenticating with API Key.")
            # To auth to the WS using an API key, we generate a signature of a nonce and
            # the WS API endpoint.
            expires = self.__generate_nonce()
            return [
                ("api-expires", expires),
                ("api-signature", generate_signature(self.api_secret, 'GET', '/realtime', expires, '')),
                ("api-key", self.api_key)
            ]
        else:
            self.logger.info("Not authenticating.")
            return []

    def __generate_nonce(self):
        return str(int(round(time.time()) + self.timeout))

    def __get_url(self):
        '''
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        '''

        # You can sub to orderBookL2 for all levels, or orderBook10 for top 10 levels & save bandwidth
        # symbolSubs = ["execution", "instrument", "order", "orderBookL2", "position", "quote", "trade"]
        # genericSubs = ["margin"]

        # subscriptions = [sub + ':' + self.symbol for sub in symbolSubs]
        # subscriptions += genericSubs

        urlParts = list(urllib.parse.urlparse(
            "https://testnet.bitmex.com/api/v1" if self.testnet else 'https://www.bitmex.com/api/v1'))
        urlParts[0] = urlParts[0].replace('http', 'ws')
        urlParts[2] = "/realtime"
        return urllib.parse.urlunparse(urlParts)

    # def __wait_for_account(self):
    #     '''On subscribe, this data will come down. Wait for it.'''
    #     # Wait for the keys to show up from the ws
    #     while not {'margin', 'position', 'order', 'orderBookL2'} <= set(self.data):
    #         sleep(0.1)
    #
    # def __wait_for_symbol(self, symbol):
    #     '''On subscribe, this data will come down. Wait for it.'''
    #     while not {'instrument', 'trade', 'quote'} <= set(self.data):
    #         sleep(0.1)

    async def __send_command(self, command, args=None):
        '''Send a raw command.'''
        if args is None:
            args = []
        await self.ws.send(json.dumps({"op": command, "args": args}))

    async def __on_message(self, message):
        '''Handler for parsing WS messages.'''
        message = json.loads(message) if message != 'pong' else message
        self.logger.debug(json.dumps(message))

        try:
            to_del_items = []
            # future: asyncio.Future
            for future, condition in self._detect_hook.items():
                if future.done():
                    to_del_items.append(future)
                elif message == 'pong' or all([message.get(key, None) == value for key, value in condition.items()]):
                    future.set_result(message)

            [self._detect_hook.pop(item) for item in to_del_items]
            if isinstance(message, dict):
                table = message.get("table")
                action = message.get("action")
                if 'subscribe' in message:
                    self.logger.debug("Subscribed to %s." % message['subscribe'])
                elif action:
                    if table not in self.data:
                        self.data[table] = []

                    # There are four possible actions from the WS:
                    # 'partial' - full table image
                    # 'insert'  - new row
                    # 'update'  - update row
                    # 'delete'  - delete row
                    if action == 'partial':
                        self.logger.debug("%s: partial" % table)
                        self.data[table] = message['data']
                        # Keys are communicated on partials to let you know how to uniquely identify
                        # an item. We use it for updates.
                        self.keys[table] = message['keys']
                    elif action == 'insert':
                        self.logger.debug('%s: inserting %s' % (table, message['data']))
                        self.data[table] += message['data']

                        # Limit the max length of the table to avoid excessive memory usage.
                        # Don't trim orders because we'll lose valuable state if we do.
                        if table not in ['order', 'orderBookL2'] and len(
                                self.data[table]) > AsyncBitMEXWebsocket.MAX_TABLE_LEN:
                            self.data[table] = self.data[table][AsyncBitMEXWebsocket.MAX_TABLE_LEN // 2:]

                    elif action == 'update':
                        self.logger.debug('%s: updating %s' % (table, message['data']))
                        # Locate the item in the collection and update it.
                        for updateData in message['data']:
                            item = find_by_keys(self.keys[table], self.data[table], updateData)
                            if not item:
                                return  # No item found to update. Could happen before push
                            item.update(updateData)
                            # Remove cancelled / filled orders
                            if table == 'order' and not order_leaves_quantity(item):
                                self.data[table].remove(item)
                    elif action == 'delete':
                        self.logger.debug('%s: deleting %s' % (table, message['data']))
                        # Locate the item in the collection and remove it.
                        for deleteData in message['data']:
                            item = find_by_keys(self.keys[table], self.data[table], deleteData)
                            self.data[table].remove(item)
                    else:
                        raise Exception("Unknown action: %s" % action)
        except:
            self.logger.error(traceback.format_exc())


# Utility method for finding an item in the store.
# When an update comes through on the websocket, we need to figure out which item in the array it is
# in order to match that item.
#
# Helpfully, on a data push (or on an HTTP hit to /api/v1/schema), we have a "keys" array. These are the
# fields we can use to uniquely identify an item. Sometimes there is more than one, so we iterate through all
# provided keys.
def find_by_keys(keys, table, matchData):
    for item in table:
        if all(item[k] == matchData[k] for k in keys):
            return item


def order_leaves_quantity(o):
    if o['leavesQty'] is None:
        return True
    return o['leavesQty'] > 0
