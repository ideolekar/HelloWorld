import asyncio
import aiohttp.web
import collections
import functools

from . import state


__all__ = ('Client',)


class Client:

    __slots__ = ('_authorize', '_infos', '_interval', '_states', '_loop')

    __marker = object()

    def __init__(self, authorize, interval, loop = None):

        if not loop:

            loop = asyncio.get_event_loop()

        self._authorize = authorize

        self._infos = []

        self._interval = interval

        self._states = []

        self._loop = loop

    def inform(self, name, keys):

        info = (name, keys)

        self._infos.append(info)

    async def dispatch(self, action, name, payload):

        data = (action, name, payload)

        coroutines = (state.dispatch(data) for state in self._states)

        await asyncio.gather(*coroutines, loop = self._loop)

    async def connect(self, request):

        self._authorize(request)

        websocket = aiohttp.web.WebSocketResponse()

        await websocket.prepare(request)

        value = state.State(websocket, loop = self._loop)

        self._states.append(value)

        await value.hello(self._interval, self._infos)

        try:

            await value.start()

        finally:

            self._states.remove(value)

        return websocket

    async def close(self):

        for state in self._states:

            await state.close()
