import asyncio
import time

import websockets
import traceback
import json
from loguru import logger

import urllib
import math
from util.api_key import generate_signature

import beeprint

from NoLossAsyncGenerator import no_data_loss_async_generator_decorator


def beeprint_format(o):
    return '\n' + beeprint.pp(o, output=False, sort_keys=False, string_break_enable=False)


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

    # todo: to expand the table, expand the two lists
    symbolSubs = ["execution", "instrument", "order", "orderBookL2", "position", "quote", "trade"]
    genericSubs = ["margin"]

    @classmethod
    async def create_instance(cls, api_key=None, api_secret=None, testnet=False, timeout=999999999999, ):
        instance = cls(api_key, api_secret, testnet, timeout)
        await instance._instantiation_complete.wait()
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
        self._instantiation_complete.set()

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
            self.__on_message(message)

    async def _ping_pong(self):
        '''
        heartbeat
        '''
        while not self.exited:
            await asyncio.sleep(5)
            if self.exited:
                break
            # timeout
            asyncio.create_task(self.ws.send('ping'))
            logger.info('ping')
            try:
                await asyncio.wait_for(self._wait_pong(), timeout=5)
            except TimeoutError:
                logger.warning('Heartbeat timeout, connection lost.Trying to reconnect.')
                self.new_reactivate_task = asyncio.create_task(self._reactivate())

    async def _wait_pong(self):
        async for news in self.new_message_watcher():
            if news == 'pong':
                logger.info("pong!")
                break

    def put_detect_hook(self, symbol: str, filter: dict):
        msg_receiver = asyncio.get_running_loop().create_future()
        if symbol not in self._detect_hook.keys():
            self._detect_hook[symbol] = {}
        self._detect_hook[symbol][msg_receiver] = filter
        return msg_receiver

    @no_data_loss_async_generator_decorator
    async def new_message_watcher(self, symbol: str = '', table: str = '', action: str = '', filter: dict = None):
        '''
        async for news in new_message_watcher():
            ...

        :param symbol:symbol, '' means that the message being watched is not symbol related.
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
            assert symbol if filter['table'] in self.symbolSubs else not bool(symbol)
            partial = await asyncio.create_task(self._ensure_subscribed(filter['table'], symbol))
            if partial:
                yield partial
        hook = self.put_detect_hook(symbol, filter)
        while True:
            msg = await hook
            hook = self.put_detect_hook(symbol, filter)
            yield msg

    def __init__(self, api_key=None, api_secret=None, testnet=False, timeout=999999999999, ):
        '''Connect to the websocket and initialize data stores. timeout seems to have no effect.'''
        self.logger = logger
        self.logger.debug("Initializing WebSocket.")
        self.testnet = testnet
        self.timeout = timeout
        self._detect_hook = {}  # {'ETHUSD':{...},'XBTUSD':{...},...,'':{...}}
        self._instantiation_complete = asyncio.Event()

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self.api_key = api_key
        self.api_secret = api_secret

        self.data = {}  # {'ETHUSD':{...},'XBTUSD':{...},...,'':{...}}
        self.keys = {}
        self.exited = False
        self._reactivate_task = asyncio.create_task(self._reactivate())

    async def exit(self):
        '''Call this to exit - will close websocket.'''
        self.exited = True
        self._reactivate_task.cancel()
        await asyncio.create_task(self.ws.close())

    async def _ensure_subscribed(self, subject: str, symbol: str = ''):
        '''
        Ensure the subject to be subscribed.

        :param subject:One among instrument, trade, quote, margin, position, orderBookL2, order, execution and so on
        :return: partial message
        '''
        # ensure symbol has a slot
        if symbol not in self.data.keys():
            self.data[symbol] = {}
        # If subject has not been subscribed
        if subject not in self.data[symbol].keys():
            assert bool(symbol) if subject in self.symbolSubs else not bool(symbol)
            # subscribe
            asyncio.create_task(self.__send_command('subscribe', args=[
                f'{subject}:{symbol}'] if subject in self.symbolSubs else [f'{subject}']))
            # wait for 'partial'
            async for news in self.new_message_watcher(symbol):
                if isinstance(news, dict):
                    if news.get('table', '') == f"{subject}" and \
                            news.get("action", '') == "partial" \
                            and self._parse_symbol(news) == symbol:
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

    async def get_instrument(self, symbol: str):
        '''Get the raw instrument data for this symbol.'''
        # Turn the 'tickSize' into 'tickLog' for use in rounding
        assert symbol
        await self._ensure_subscribed('instrument', symbol)
        instrument = self.data[symbol]['instrument'][0]
        instrument['tickLog'] = int(math.fabs(math.log10(instrument['tickSize'])))
        return instrument

    async def recent_trades(self, symbol: str):
        '''Get recent trades.'''
        assert symbol
        await self._ensure_subscribed('trade', symbol)
        return self.data[symbol]['trade']

    async def recent_quotes(self, symbol: str):
        '''Get recent quotes.'''
        assert symbol
        await self._ensure_subscribed('quote', symbol)
        return self.data[symbol]['quote']

    async def get_ticker(self, symbol: str):
        '''Return a ticker object. Generated from quote and trade.'''
        assert symbol
        quote_task = asyncio.create_task(self.recent_quotes(symbol))
        trade_task = asyncio.create_task(self.recent_trades(symbol))
        instrument_task = asyncio.create_task(self.get_instrument(symbol))

        await quote_task
        await trade_task
        await instrument_task
        lastQuote = self.data[symbol]['quote'][-1]
        lastTrade = self.data[symbol]['trade'][-1]
        ticker = {
            "last": lastTrade['price'],
            "buy": lastQuote['bidPrice'],
            "sell": lastQuote['askPrice'],
            "mid": (float(lastQuote['bidPrice'] or 0) + float(lastQuote['askPrice'] or 0)) / 2
        }

        # The instrument has a tickSize. Use it to round values.
        instrument = self.data[symbol]['instrument'][0]
        return {k: round(float(v or 0), instrument['tickLog']) for k, v in ticker.items()}

    async def margin(self):
        '''Get your margin details.'''
        await self._ensure_subscribed('margin')
        return self.data['']['margin'][0]

    async def positions(self, symbol: str):
        '''Get your positions.'''
        await self._ensure_subscribed('position', symbol)
        return self.data[symbol]['position']

    async def market_depth(self, symbol: str):
        '''Get market depth (orderbook). Returns all levels.'''
        await self._ensure_subscribed('orderBookL2', symbol)
        return self.data[symbol]['orderBookL2']

    async def open_orders(self, symbol: str, clOrdIDPrefix=''):
        '''Get all your open orders.'''
        assert symbol
        await self._ensure_subscribed('order', symbol)
        orders = self.data[symbol]['order']
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

    def _parse_symbol(self, msg):
        try:
            if msg['table'] in self.symbolSubs:
                return msg['data'][0]['symbol']
        except:
            try:
                return msg['filter']['symbol']
            except:
                return ''

    def __on_message(self, message):
        '''Handler for parsing WS messages.'''
        message = json.loads(message) if message != 'pong' else message  # a dict or 'pong'
        self.logger.debug(beeprint_format(message))
        message_symbol = self._parse_symbol(message)

        try:
            to_del_items = {}
            # future: asyncio.Future
            for symbol, hook in self._detect_hook.items():
                if symbol == message_symbol:
                    for future, filter in hook.items():
                        if future.done():
                            to_del_items[symbol] = to_del_items.get(symbol, []) + [future]
                        elif all([message.get(key, None) == value for key, value in filter.items()]):
                            future.set_result(message)
                            to_del_items[symbol] = to_del_items.get(symbol, []) + [future]

            [(self._detect_hook[symbol].pop(hook) for hook in hooks) \
             for symbol, hooks in to_del_items.items()]

            if isinstance(message, dict):
                # ensure symbol in data
                if message_symbol not in self.data.keys():
                    self.data[message_symbol] = {}
                table = message.get("table")
                action = message.get("action")
                if 'subscribe' in message:
                    self.logger.debug("Subscribed to %s." % beeprint_format(message['subscribe']))
                elif action:
                    if table not in self.data[message_symbol]:
                        self.data[message_symbol][table] = []

                    # There are four possible actions from the WS:
                    # 'partial' - full table image
                    # 'insert'  - new row
                    # 'update'  - update row
                    # 'delete'  - delete row
                    if action == 'partial':
                        self.logger.debug("%s: partial" % table)
                        self.data[message_symbol][table] = message['data']
                        # Keys are communicated on partials to let you know how to uniquely identify
                        # an item. We use it for updates.
                        self.keys[table] = message['keys']
                    elif action == 'insert':
                        self.logger.debug('%s: inserting %s' % (table, beeprint_format(message['data'])))
                        self.data[message_symbol][table] += message['data']

                        # Limit the max length of the table to avoid excessive memory usage.
                        # Don't trim orders because we'll lose valuable state if we do.
                        if table not in ['order', 'orderBookL2'] and len(
                                self.data[message_symbol][table]) > AsyncBitMEXWebsocket.MAX_TABLE_LEN:
                            self.data[message_symbol][table] = self.data[message_symbol][table][
                                                               AsyncBitMEXWebsocket.MAX_TABLE_LEN // 2:]

                    elif action == 'update':
                        self.logger.debug('%s: updating %s' % (table, beeprint_format(message['data'])))
                        # Locate the item in the collection and update it.
                        for updateData in message['data']:
                            item = find_by_keys(self.keys[table], self.data[message_symbol][table], updateData)
                            if not item:
                                return  # No item found to update. Could happen before push
                            item.update(updateData)
                            # Remove cancelled / filled orders
                            if table == 'order' and not order_leaves_quantity(item):
                                self.data[message_symbol][table].remove(item)
                    elif action == 'delete':
                        self.logger.debug('%s: deleting %s' % (table, beeprint_format(message['data'])))
                        # Locate the item in the collection and remove it.
                        for deleteData in message['data']:
                            item = find_by_keys(self.keys[table], self.data[message_symbol][table], deleteData)
                            self.data[message_symbol][table].remove(item)
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
