import asyncio
import aiohttp


__all__ = ('State',)


class Heart:

    __slots__ = ('_beat', '_kill', '_acked', '_closed', '_task', '_loop')

    def __init__(self, beat, kill, loop):

        self._beat = beat

        self._kill = kill

        self._acked = None

        self._closed = None

        self._task = None

        self._loop = loop

    @property
    def loop(self):

        return self._loop

    def ack(self):

        self._acked = True

    async def pulse(self, interval):

        while not self._closed:

            self._acked = False

            await self._beat()

            await asyncio.sleep(interval)

            if self._acked:

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

    __slots__ = ('_websocket', '_heart', '_callback')

    def __init__(self, websocket, callback, loop = None):

        if not loop:

            loop = asyncio.get_event_loop()

        self._websocket = websocket

        self._heart = Heart(self._beat, self.close, loop)

        self._callback = callback

    async def _push(self, code, payload):

        data = (code, payload)

        await self._websocket.send_json(data)

    async def _beat(self):

        await self._push(Codes.beat, None)

    async def _pull(self, data):

        (code, payload) = data

        if code == Codes.dispatch:

            self._callback(payload)

        if code == Codes.ack:

            self._heart.ack()

            return

        if code == Codes.hello:

            (interval, information) = payload

            self._heart.start(interval)

            self._callback(information)

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
