import asyncio
import aiohttp


__all__ = ('State',)


class Heart:

    __slots__ = ('_ack', '_kill', '_beated', '_closed', '_task', '_loop')

    def __init__(self, ack, kill, loop):

        self._ack = ack

        self._kill = kill

        self._beated = None

        self._closed = None

        self._task = None

        self._loop = loop

    async def ack(self):

        self._beated = True

        await self._ack()

    async def pulse(self, interval):

        while not self._closed:

            self._beated = False

            await asyncio.sleep(interval)

            if self._beated:

                continue

            await self._kill()

            self.close()

    def start(self, interval):

        if not self._task is None:

            self._task.cancel()

        self._closed = False

        coroutine = self.pulse(interval)

        self._task = self._loop.create_task(coroutine)

    def close(self):

        self._closed = True

        if self._task is None:

            return

        self._task.cancel()


class Codes:

    dispatch = 0
    hello = 1
    beat = 2
    ack = 3


class State:

    __slots__ = ('_websocket', '_heart')

    def __init__(self, websocket, loop = None):

        if not loop:

            loop = asyncio.get_event_loop()

        self._websocket = websocket

        self._heart = Heart(self._ack, self.close, loop)

    async def _push(self, code, payload):

        data = (code, payload)

        await self._websocket.send_json(data)

    async def _ack(self):

        await self._push(Codes.ack, None)

    async def hello(self, interval, information):

        data = (interval, information)

        pulsing = self._heart.start(interval)

        await self._push(Codes.hello, data)

    async def dispatch(self, data):

        await self._push(Codes.dispatch, data)

    async def _pull(self, data):

        (code, payload) = data

        if code == Codes.beat:

            await self._heart.ack()

            return

    async def stream(self):

        while not self._websocket.closed:

            try:

                data = await self._websocket.receive_json()

            except (asyncio.CancelledError, TypeError):

                break

            try:

                await self._pull(data)

            except:

                continue

    async def close(self):

        await self._websocket.close()

        self._heart.close()

    async def start(self):

        try:

            await self.stream()

        finally:

            await self.close()
