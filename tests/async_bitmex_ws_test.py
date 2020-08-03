import asyncUnittest
from asyncUnittest import AsyncTestCase
from async_bitmex_ws import AsyncBitMEXWebsocket
import asyncio
from loguru import logger
import ccxt.async_support as ccxt


class TestPingPong(AsyncTestCase):
    enable_test = True
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
    enable_test = True
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
    enable_test = True
    aws: AsyncBitMEXWebsocket = None

    @classmethod
    async def setUpClass(cls) -> None:
        cls.aws = await AsyncBitMEXWebsocket.create_instance(api_key='IGMzIdy55DWYGKVSFIECBQjy',
                                                             api_secret='HB7z6vhXvu9fSJrcYnjWm6R_9JhSs6dVYwKPQryadRSG8atF',
                                                             testnet=True)
        cls.bitmex = ccxt.bitmex({
            "apiKey": "IGMzIdy55DWYGKVSFIECBQjy",
            "secret": "HB7z6vhXvu9fSJrcYnjWm6R_9JhSs6dVYwKPQryadRSG8atF",
        })
        cls.bitmex.set_sandbox_mode(True)
        order_book_task = asyncio.create_task(cls.bitmex.fetch_l2_order_book('BTC/USD'))
        await cls.bitmex.private_post_position_leverage(params={'symbol': f'XBTUSD', 'leverage': 1})
        buy_BTC_USD_task = asyncio.create_task(cls.bitmex.create_order('BTC/USD', 'limit', 'buy', 1, 1000))
        order_book = await order_book_task
        sell_BTC_USD_task = asyncio.create_task(
            cls.bitmex.create_order('BTC/USD', 'limit', 'sell', 1, order_book['bids'][0][0]))

        await sell_BTC_USD_task
        cls.tmp_BTC_USD_buy_order = await buy_BTC_USD_task

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
            # logger.info(beeprint_format(news))
            break

    async def test_positions(self):
        position_sample = {
            'account': 308110,
            'symbol': 'XBTUSD',
            'currency': 'XBt',
            'underlying': 'XBT',
            'quoteCurrency': 'USD',
            'commission': 0.00075,
            'initMarginReq': 0.01,
            'maintMarginReq': 0.0035,
            'riskLimit': 20000000000,
            'leverage': 100,
            'crossMargin': True,
            'deleveragePercentile': 1,
            'rebalancedPnl': 59,
            'prevRealisedPnl': -12,
            'prevUnrealisedPnl': 0,
            'prevClosePrice': 10268.24,
            'openingTimestamp': '2020-07-28T03:00:00.000Z',
            'openingQty': 0,
            'openingCost': 0,
            'openingComm': 0,
            'openOrderBuyQty': 0,
            'openOrderBuyCost': 0,
            'openOrderBuyPremium': 0,
            'openOrderSellQty': 0,
            'openOrderSellCost': 0,
            'openOrderSellPremium': 0,
            'execBuyQty': 5,
            'execBuyCost': 45660,
            'execSellQty': 7,
            'execSellCost': 63915,
            'execQty': -2,
            'execCost': 18255,
            'execComm': 73,
            'currentTimestamp': '2020-07-28T03:55:41.263Z',
            'currentQty': -2,
            'currentCost': 18255,
            'currentComm': 73,
            'realisedCost': -2,
            'unrealisedCost': 18257,
            'grossOpenCost': 0,
            'grossOpenPremium': 0,
            'grossExecCost': 18262,
            'isOpen': True,
            'markPrice': 10975.4,
            'markValue': 18222,
            'riskValue': 18222,
            'homeNotional': -0.00018222,
            'foreignNotional': 2,
            'posState': '',
            'posCost': 18257,
            'posCost2': 18257,
            'posCross': 21,
            'posInit': 183,
            'posComm': 14,
            'posLoss': 0,
            'posMargin': 218,
            'posMaint': 161,
            'posAllowance': 0,
            'taxableMargin': 0,
            'initMargin': 0,
            'maintMargin': 183,
            'sessionMargin': 0,
            'targetExcessMargin': 0,
            'varMargin': 0,
            'realisedGrossPnl': 2,
            'realisedTax': 0,
            'realisedPnl': -71,
            'unrealisedGrossPnl': -35,
            'longBankrupt': 0,
            'shortBankrupt': 0,
            'taxBase': 0,
            'indicativeTaxRate': None,
            'indicativeTax': 0,
            'unrealisedTax': 0,
            'unrealisedPnl': -35,
            'unrealisedPnlPcnt': -0.0019,
            'unrealisedRoePcnt': -0.1917,
            'simpleQty': None,
            'simpleCost': None,
            'simpleValue': None,
            'simplePnl': None,
            'simplePnlPcnt': None,
            'avgCostPrice': 10954.1023,
            'avgEntryPrice': 10954.1023,
            'breakEvenPrice': 10946.5,
            'marginCallPrice': 100000000,
            'liquidationPrice': 100000000,
            'bankruptPrice': 100000000,
            'timestamp': '2020-07-28T03:55:41.263Z',
            'lastPrice': 10975.4,
            'lastValue': 18222,
        }
        n = 0
        async for news in self.aws.new_message_watcher(symbol='XBTUSD', table='position', ):
            positions = await self.aws.positions('XBTUSD')

            if n == 0:
                self.assertEqual(news['action'], 'partial')
                self.assertEqual(len(positions), 1)
                self.assertEqual(positions[0], news['data'][0])
                for key in position_sample.keys():
                    self.assertTrue(key in positions[0].keys())
            else:
                self.assertEqual(news['table'], 'position')
                self.assertTrue(news['action'] in ['update', 'insert', 'delete'])
                self.assertEqual(self.aws._parse_symbol(news), 'XBTUSD')
                if news['action'] == 'update':
                    for key, value in news['data'][0].items():
                        self.assertEqual(value, positions[0][key])

            n += 1
            if n >= 5:
                break

    async def test_open_orders(self):
        order_sample = {
            'orderID': '96796a8a-6204-4a98-054c-6e64c4200667',
            'clOrdID': '',
            'clOrdLinkID': '',
            'account': 308110,
            'symbol': 'XBTUSD',
            'side': 'Buy',
            'simpleOrderQty': None,
            'orderQty': 1,
            'price': 1000,
            'displayQty': None,
            'stopPx': None,
            'pegOffsetValue': None,
            'pegPriceType': '',
            'currency': 'USD',
            'settlCurrency': 'XBt',
            'ordType': 'Limit',
            'timeInForce': 'GoodTillCancel',
            'execInst': '',
            'contingencyType': '',
            'exDestination': 'XBME',
            'ordStatus': 'New',
            'triggered': '',
            'workingIndicator': True,
            'ordRejReason': '',
            'simpleLeavesQty': None,
            'leavesQty': 1,
            'simpleCumQty': None,
            'cumQty': 0,
            'avgPx': None,
            'multiLegReportingType': 'SingleSecurity',
            'text': 'Submitted via API.',
            'transactTime': '2020-07-29T06:43:38.521Z',
            'timestamp': '2020-07-29T06:43:38.521Z',
        }
        async for news in self.aws.new_message_watcher(table='order', symbol='XBTUSD'):
            self.assertEqual(news['action'], 'partial')
            open_orders = await self.aws.open_orders(symbol='XBTUSD')
            self.assertTrue(
                any([self.tmp_BTC_USD_buy_order['id'] == open_order["orderID"] for open_order in open_orders]))
            self.assertTrue(*[order in open_orders for order in news['data']])
            for open_order in open_orders:
                self.assertTrue(*[key in open_order for key in order_sample.keys()])
            await asyncio.sleep(1)
            break

    @classmethod
    async def tearDownClass(cls) -> None:
        aws_exit_task = asyncio.create_task(cls.aws.exit())
        cancle_order_task = asyncio.create_task(
            cls.bitmex.cancel_order(cls.tmp_BTC_USD_buy_order['id'], cls.tmp_BTC_USD_buy_order['symbol']))
        order_book_task = asyncio.create_task(cls.bitmex.fetch_l2_order_book('BTC/USD'))

        await cls.bitmex.private_post_position_leverage(params={'symbol': f'XBTUSD', 'leverage': 1})
        BTC_USD_buy_task = asyncio.create_task(
            cls.bitmex.create_order('BTC/USD', 'limit', 'buy', 1, (await order_book_task)['asks'][0][0]))
        try:
            await BTC_USD_buy_task
            await cancle_order_task
        except:
            pass
        finally:
            bitmex_close_task = asyncio.create_task(cls.bitmex.close())

            await bitmex_close_task
        await aws_exit_task


if __name__ == '__main__':
    asyncUnittest.run()
