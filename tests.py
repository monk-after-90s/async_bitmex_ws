'''
todo 更改为自定测试，找异步测试框架，没有的话就自己写
'''
import signal
import asyncio
import sys
from async_bitmex_ws import AsyncBitMEXWebsocket
import beeprint

"""
信号值      符号      行为
2          SIGINT    进程终端，CTRL+C
9          SIGKILL   强制终端
15         SIGTEM    请求中断
20         SIGTOP    停止（挂起）进程 CRTL+D
"""
loop = asyncio.get_event_loop()


def safely_exit():
    sys.exit(0)


loop.add_signal_handler(signal.SIGTERM, safely_exit)
loop.add_signal_handler(signal.SIGINT, safely_exit)


async def safely_exit_management():
    global ws
    if ws and isinstance(ws, AsyncBitMEXWebsocket):
        await ws.exit()
    print('Safely exit.')


async def test_instrument():
    global ws
    ws = await AsyncBitMEXWebsocket.create_instance(symbol='XBTUSD')

    print(f'当前instrument:{await ws.get_instrument()}')


async def test_new_message():
    global ws
    ws = await AsyncBitMEXWebsocket.create_instance(symbol='XBTUSD')

    await ws.get_instrument()

    async def show_news(number: int):
        async for news in ws.new_message_watcher():
            print(f'{number}号:{news}')

    asyncio.create_task(show_news(1))
    asyncio.create_task(show_news(2))


async def test_get_ticker():
    global ws
    ws = await AsyncBitMEXWebsocket.create_instance(symbol='XBTUSD')
    async for news in ws.new_message_watcher():
        print(await ws.get_ticker())


async def test_recent_tardes():
    global ws
    ws = await AsyncBitMEXWebsocket.create_instance(symbol='XBTUSD')

    async for news in ws.new_message_watcher(table='trade'):
        # print(await ws.recent_trades())
        beeprint.pp(news, sort_keys=False)


async def test_recent_quotes():
    global ws
    ws = await AsyncBitMEXWebsocket.create_instance(symbol='XBTUSD')

    async for news in ws.new_message_watcher({'table': 'quote'}):
        beeprint.pp(news)
        # beeprint.pp(await ws.recent_quotes(), sort_keys=False)


async def test_market_depth():
    global ws
    ws = await AsyncBitMEXWebsocket.create_instance(symbol='XBTUSD')

    # async for news in ws.new_message_watcher():
    async for news in ws.new_message_watcher(table='orderBookL2'):
        beeprint.pp(news, sort_keys=False)
        # beeprint.pp(await ws.market_depth())


async def test_margin():
    global ws
    ws = await AsyncBitMEXWebsocket.create_instance(symbol='XBTUSD',
                                                    api_key='IGMzIdy55DWYGKVSFIECBQjy',
                                                    api_secret='HB7z6vhXvu9fSJrcYnjWm6R_9JhSs6dVYwKPQryadRSG8atF',
                                                    testnet=True)
    # beeprint.pp(await ws.margin())

    async for news in ws.new_message_watcher(table='margin'):
        beeprint.pp(news, sort_keys=False)


async def test_positions():
    global ws
    ws = await AsyncBitMEXWebsocket.create_instance(symbol='XBTUSD',
                                                    api_key='IGMzIdy55DWYGKVSFIECBQjy',
                                                    api_secret='HB7z6vhXvu9fSJrcYnjWm6R_9JhSs6dVYwKPQryadRSG8atF',
                                                    testnet=True)
    # beeprint.pp(await ws.positions())

    async for news in ws.new_message_watcher(table='position'):
        beeprint.pp(news, sort_keys=False)


async def test_open_orders():
    global ws
    ws = await AsyncBitMEXWebsocket.create_instance(symbol='XBTUSD',
                                                    api_key='IGMzIdy55DWYGKVSFIECBQjy',
                                                    api_secret='HB7z6vhXvu9fSJrcYnjWm6R_9JhSs6dVYwKPQryadRSG8atF',
                                                    testnet=True)
    # beeprint.pp(await ws.open_orders())

    async for news in ws.new_message_watcher(table='order'):
        beeprint.pp(news, sort_keys=False)


async def test_ping_pong():
    global ws
    ws = await AsyncBitMEXWebsocket.create_instance(symbol='XBTUSD',
                                                    api_key='IGMzIdy55DWYGKVSFIECBQjy',
                                                    api_secret='HB7z6vhXvu9fSJrcYnjWm6R_9JhSs6dVYwKPQryadRSG8atF',
                                                    testnet=True)
    beeprint.pp(await ws.get_instrument(), sort_keys=False)


loop.create_task(test_margin())

try:
    loop.run_forever()
except:
    print('Loop detects a error')
finally:
    loop.run_until_complete(safely_exit_management())
exit()
