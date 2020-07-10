import asyncUnittest
from asyncUnittest import AsyncTestCase
from async_bitmex_ws import AsyncBitMEXWebsocket
import asyncio
from loguru import logger


class TestPingPong(AsyncTestCase):
    ws: AsyncBitMEXWebsocket = None

    @classmethod
    async def setUpClass(cls) -> None:
        cls.ws = await AsyncBitMEXWebsocket.create_instance(api_key='IGMzIdy55DWYGKVSFIECBQjy',
                                                            api_secret='HB7z6vhXvu9fSJrcYnjWm6R_9JhSs6dVYwKPQryadRSG8atF',
                                                            testnet=True)

    async def test_ping_pong(self):
        n = 3
        last_pong_time = 0
        async for news in self.ws.new_message_watcher():
            if news == 'pong':
                if last_pong_time:
                    interval_time = round(asyncio.get_running_loop().time() - last_pong_time)
                    logger.info(interval_time)

                    assert interval_time == 5
                    n -= 1
                    if n == 0:
                        break

                last_pong_time = asyncio.get_running_loop().time()

    @classmethod
    async def tearDownClass(cls) -> None:
        await cls.ws.exit()


if __name__ == '__main__':
    asyncUnittest.run()
