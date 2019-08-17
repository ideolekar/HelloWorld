import asyncio
import aiohttp
import yarl
import collections
import functools
import copy
import aiocogs

from . import errors
from . import state
from . import cache
from . import helpers


__all__ = ('Client',)


class Client:

    __slots__ = ('_session', '_url', '_authorize')

    _errors = {
        401: errors.AuthorizationError,
        400: errors.BadRequestError
    }

    _valid = (*_errors.keys(), 200)

    def __init__(self, session, url, authorize):

        self._session = session

        self._url = yarl.URL(url)

        self._authorize = authorize

    async def request(self, method, path, **kwargs):

        self._authorize(kwargs)

        clean = self._url.path.rstrip('/')

        url = self._url.with_path(clean + path)

        response = await self._session.request(method, url, **kwargs)

        data = await response.json() if response.status in self._valid else None

        if response.status < 400:

            return data

        try:

            error = self._errors[response.status]

        except KeyError:

            error = errors.RequestError

        if data:

            code, *info = data

        else:

            code, info = (None,) * 2

        raise error(response, code, info)


class Client(Client):

    __slots__ = ('_cache', '_ignore', '_tracks', '_state', '_ready', '_closed',
                 '_loop')

    def __init__(self, *args, ignore = ().__contains__, loop = None):

        super().__init__(*args)

        if not loop:

            loop = asyncio.get_event_loop()

        self._cache = None

        self._ignore = ignore

        self._tracks = collections.defaultdict(
            functools.partial(
                collections.defaultdict,
                list
            )
        )

        self._state = None

        self._ready = asyncio.Event(loop = loop)

        self._closed = False

        self._loop = loop

    @property
    def cache(self):

        return self._cache

    async def request(self, method, name, *data):

        path = '/' + name

        data = await super().request(method, path, json = data)

        return tuple(map(cache.Entry, data))

    def get(self, name, *keys, names = ()):

        return self.request('GET', name, keys, names)

    def create(self, name, *keys, **data):

        return self.request('POST', name, keys, data)

    def update(self, name, *keys, **data):

        return self.request('PATCH', name, keys, data)

    def delete(self, name, *keys):

        return self.request('DELETE', name, keys)

    _methods = {
        'create': 'POST',
        'update': 'PATCH',
        'delete': 'DELETE'
    }

    def track(self, action, name):

        method = self._methods[action]

        def wrapper(function):

            self._tracks[method][name].append(function)

            return function

        return wrapper

    def check(self, action, name):

        method = self._methods[action]

        value = asyncio.Event(loop = self._loop)

        def decorator(function):

            callbacks = self._tracks[method][name]

            async def callback(*args):

                if not await function(*args):

                    return

                value.set()

            callbacks.append(callback)

            async def observe():

                await value.wait()

                callbacks.remove(callback)

            coroutine = observe()

            self._loop.create_task(coroutine)

            return value

        return decorator

    def _ePOST(self, name, data):

        entries = self._cache[name]

        query = entries.query(data)

        entry = entries.create(query, data)

        return entry

    def _ePATCH(self, name, data):

        entries = self._cache[name]

        query = entries.query(data)

        entry = entries.get(*query)

        fake = copy.copy(entry)

        helpers.update_value(entry, data)

        return (fake, entry)

    def _eDELETE(self, name, data):

        entries = self._cache[name]

        query = entries.query(data)

        entry = entries.destroy(query)

        return entry

    async def handle(self, method, name, data, prefix = '_e'):

        await self._ready.wait()

        if self._ignore(name):

            entries = map(cache.Entry, data)

        else:

            handle = getattr(self, prefix + method)

            entries = (handle(name, value) for value in data)

        entries = tuple(entries)

        callbacks = self._tracks[method][name]

        coroutines = (callback(entries) for callback in callbacks)

        await asyncio.gather(*coroutines, loop = self._loop)

    async def _build(self, names):

        names = tuple(names)

        coroutines = map(self.get, names)

        tasks = tuple(map(self._loop.create_task, coroutines))

        async for task in aiocogs.ready(*tasks, loop = self._loop):

            values = task.result()

            index = tasks.index(task)

            name = names[index]

            store = self._cache[name]

            for value in values:

                data = value.__dict__

                query = store.query(data)

                store.create(query, data)

        self._ready.set()

    def _setup(self, data):

        store = {}

        for (name, key) in data:

            if self._ignore(name):

                continue

            store[name] = cache.Entries(key)

        self._cache = cache.Entry(store)

        return self._build(store.keys())

    def _callback(self, data, prefix = '_e'):

        if self._cache:

            (method, name, payload) = data

            coroutine = self.handle(method, name, payload)

        else:

            coroutine = self._setup(data)

        self._loop.create_task(coroutine)

    async def connect(self, path):

        (params, headers) = ({}, {})

        options = {'params': params, 'headers': headers}

        self._authorize(options)

        path = self._url.path.rstrip('/') + path

        url = self._url.with_path(path).with_query(params)

        websocket = await self._session.ws_connect(url, headers = headers)

        return state.State(websocket, self._callback, loop = self._loop)

    async def manage(self, execute, retry):

        while not self._closed:

            while not self._closed:

                try:

                    state = await execute()

                except aiohttp.ClientConnectionError:

                    await asyncio.sleep(retry)

                else:

                    break

            else:

                break

            self._state = state

            await self._state.start()

    async def start(self, path, retry = 0.5):

        connect = functools.partial(self.connect, path)

        coroutine = self.manage(connect, retry)

        self._loop.create_task(coroutine)

        await self._ready.wait()

    async def close(self):

        self._closed = True

        await self._state.close()
