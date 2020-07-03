'''import asyncio

import pytest


async def say(what, when):
    await asyncio.sleep(when)
    return what


@pytest.mark.asyncio
async def test_say():
    assert 'Hello!' == await say('Hello!', 0)
'''
import asyncio

import sys
import os
from aiounittest import AsyncTestCase
import unittest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from async_bitmex_ws import AsyncBitMEXWebsocket


class MyTest(AsyncTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.a = 10

    def setUp(self) -> None:
        self.b = 3

    async def test_sample(self):
        await asyncio.sleep(0)
        print(type(self).a)
        print(self.b)
        type(self).a += 1
        self.b += 1
        print(type(self).a)
        print(self.b)

    async def test_sample2(self):
        await asyncio.sleep(0)
        print(type(self).a)
        print(self.b)
        type(self).a += 1
        self.b += 1
        print(type(self).a)
        print(self.b)


if __name__ == '__main__':
    unittest.main()
