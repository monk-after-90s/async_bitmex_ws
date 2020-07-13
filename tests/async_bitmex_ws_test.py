import asyncUnittest
from asyncUnittest import AsyncTestCase
from async_bitmex_ws import AsyncBitMEXWebsocket
import asyncio
from loguru import logger
import ccxt.async_support as ccxt

from async_bitmex_ws.async_bitmex_ws import beeprint_format


class TestPingPong(AsyncTestCase):
    enable_test = False
    aws: AsyncBitMEXWebsocket = None

    @classmethod
    async def setUpClass(cls) -> None:
        cls.aws = await AsyncBitMEXWebsocket.create_instance(testnet=True)

    async def test_ping_pong(self):
        n = 3
        last_pong_time = 0
        async for news in self.aws.new_message_watcher():
            if news == 'pong':
                if last_pong_time:
                    interval_time = round(asyncio.get_running_loop().time() - last_pong_time)
                    logger.info(interval_time)

                    self.assertEqual(interval_time, 5)
                    n -= 1
                    if n == 0:
                        break

                last_pong_time = asyncio.get_running_loop().time()

    @classmethod
    async def tearDownClass(cls) -> None:
        await cls.aws.exit()


class TestOrderThenPingPong(AsyncTestCase):
    enable_test = False
    aws: AsyncBitMEXWebsocket = None

    @classmethod
    async def setUpClass(cls) -> None:
        cls.aws = await AsyncBitMEXWebsocket.create_instance(api_key='IGMzIdy55DWYGKVSFIECBQjy',
                                                             api_secret='HB7z6vhXvu9fSJrcYnjWm6R_9JhSs6dVYwKPQryadRSG8atF',
                                                             testnet=True)

    async def setUp(self) -> None:
        self.bitmex = ccxt.bitmex({
            "apiKey": "IGMzIdy55DWYGKVSFIECBQjy",
            "secret": "HB7z6vhXvu9fSJrcYnjWm6R_9JhSs6dVYwKPQryadRSG8atF",
        })
        self.bitmex.set_sandbox_mode(True)
        self.orders = []

    async def _create_test_order(self):
        order_book = await self.bitmex.fetch_order_book('BTC/USD', limit=1)
        self.orders.append(await asyncio.create_task(
            self.bitmex.create_order('BTC/USD', 'limit', 'buy', 1, round(order_book['bids'][0][0] * 0.95))))

    async def test_create_order_then_pingpong(self):
        order_ok_time = 0

        n = 0

        async for news in self.aws.new_message_watcher(symbol='XBTUSD', table='order'):
            n += 1

            if n == 1:
                self.assertEqual(news['action'], 'partial')
                self.assertEqual(news['data'], [])
                asyncio.create_task(self._create_test_order())
            elif n == 2:
                self.assertEqual(news['action'], 'insert')
                self.assertEqual(len(news['data']), 1)
                self.assertTrue('orderID' in news['data'][0])
                self.assertTrue('account' in news['data'][0])
                self.assertTrue('symbol' in news['data'][0])
                self.assertTrue('side' in news['data'][0])
                self.assertTrue('orderQty' in news['data'][0])
                self.assertTrue('price' in news['data'][0])
                order_ok_time = asyncio.get_running_loop().time()

                break

        last_pong_time = 0
        n = 0
        async for news in self.aws.new_message_watcher():
            if news == 'pong':
                if not last_pong_time:
                    self.assertEqual(5, round(asyncio.get_running_loop().time() - order_ok_time))
                else:
                    self.assertEqual(5, round(asyncio.get_running_loop().time() - last_pong_time))
                last_pong_time = asyncio.get_running_loop().time()
                n += 1
                if n >= 3:
                    break

    async def tearDown(self) -> None:
        cancel_order_tasks = [asyncio.create_task(self.bitmex.cancel_order(id=order['id'], symbol=order['symbol'])) for
                              order in self.orders]

        [await task for task in cancel_order_tasks]

        await self.bitmex.close()

    @classmethod
    async def tearDownClass(cls) -> None:
        exit_task = asyncio.create_task(cls.aws.exit())

        try:
            await exit_task
        except:
            pass


class MultiTest(AsyncTestCase):
    # enable_test = False
    aws: AsyncBitMEXWebsocket = None

    @classmethod
    async def setUpClass(cls) -> None:
        cls.aws = await AsyncBitMEXWebsocket.create_instance(api_key='IGMzIdy55DWYGKVSFIECBQjy',
                                                             api_secret='HB7z6vhXvu9fSJrcYnjWm6R_9JhSs6dVYwKPQryadRSG8atF',
                                                             testnet=True)

    async def test_instrument(self):
        n = 0
        async for news in self.aws.new_message_watcher(symbol='XBTUSD', table='instrument', ):
            if n == 0:
                self.assertEqual(news['action'], 'partial')
                instrument = await self.aws.get_instrument('XBTUSD')
                self.assertEqual(instrument, news['data'][0])
            else:
                self.assertEqual(news['table'], 'instrument')
                self.assertTrue(news['action'] in ['update', 'insert', 'delete'])
                self.assertEqual(self.aws._parse_symbol(news), 'XBTUSD')

            n += 1
            if n >= 5:
                break

    async def test_new_message_watcher(self):
        asyncio.create_task(self._test_new_message_watcher_helper1())
        asyncio.create_task(self._test_new_message_watcher_helper2())
        asyncio.create_task(self._test_new_message_watcher_helper3())
        n = 0
        while n <= 5:
            self.data_slot = ([], asyncio.get_running_loop().create_future())
            await self.data_slot[-1]
            self.assertEqual(*self.data_slot[0])

            n += 1

    async def _test_new_message_watcher_helper1(self):
        async for news in self.aws.new_message_watcher(symbol='XBTUSD', table='instrument', ):
            self.data_slot[0].append(news)
            if len(self.data_slot[0]) >= 3 and not self.data_slot[-1].done():
                self.data_slot[-1].set_result(None)

    async def _test_new_message_watcher_helper2(self):
        async for news in self.aws.new_message_watcher(symbol='XBTUSD', table='instrument', ):
            self.data_slot[0].append(news)
            if len(self.data_slot[0]) >= 3 and not self.data_slot[-1].done():
                self.data_slot[-1].set_result(None)

    async def _test_new_message_watcher_helper3(self):
        async for news in self.aws.new_message_watcher(symbol='XBTUSD', table='instrument', ):
            self.data_slot[0].append(news)
            if len(self.data_slot[0]) >= 3 and not self.data_slot[-1].done():
                self.data_slot[-1].set_result(None)

    async def test_get_ticker(self):
        n = 0
        async for news in self.aws.new_message_watcher(symbol='XBTUSD'):
            ticker = await asyncio.create_task(self.aws.get_ticker('XBTUSD'))
            self.assertTrue('last' in ticker, 'buy' in ticker, 'sell' in ticker, 'mid' in ticker)
            lastQuote = (await self.aws.recent_quotes('XBTUSD'))[-1]
            self.assertEqual(ticker,
                             {k: round(float(v or 0), (await self.aws.get_instrument('XBTUSD'))['tickLog']) for k, v in
                              {
                                  "last": (await self.aws.recent_trades('XBTUSD'))[-1]['price'],
                                  "buy": lastQuote['bidPrice'],
                                  "sell": lastQuote['askPrice'],
                                  "mid": (float(lastQuote['bidPrice'] or 0) + float(
                                      lastQuote['askPrice'] or 0)) / 2
                              }.items()}
                             )

            n += 1
            if n >= 4:
                break

    async def test_recent_tardes(self):
        sample_trade = {
            'timestamp': '2020-07-13T06:56:15.029Z',
            'symbol': 'XBTUSD',
            'side': 'Sell',
            'size': 7,
            'price': 9256,
            'tickDirection': 'MinusTick',
            'trdMatchID': 'f34af50f-4f18-b866-4647-78aa8d7029a5',
            'grossValue': 75628,
            'homeNotional': 0.00075628,
            'foreignNotional': 7,
        }
        recent_trades = await asyncio.create_task(self.aws.recent_trades('XBTUSD'))

        for recent_trade in recent_trades:
            self.assertEqual(recent_trade['symbol'], 'XBTUSD')

            self.assertTrue(*[k in recent_trade for k in sample_trade.keys()])

        async for news in self.aws.new_message_watcher(symbol='XBTUSD', table='trade'):
            logger.info(beeprint_format(news))
            break

    async def test_positions(self):
        pass

    @classmethod
    async def tearDownClass(cls) -> None:
        await cls.aws.exit()


if __name__ == '__main__':
    asyncUnittest.run()
