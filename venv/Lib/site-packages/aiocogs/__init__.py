import sys, os
import asyncio
import threading
import operator
import inspect
import importlib

from . import helpers


__all__ = ('sort', 'thread', 'ready', 'cache', 'reduce', 'flatten', 'infinite',
           'Stream', 'Valve', 'throttle', 'Sling')


marker = object()


async def sort(generator, key = None, reverse = False):

    """
    Get a list if values sorted as yielded.
    """

    values = []

    compare = operator.lt if reverse else  operator.gt

    async for value in generator:

        index = helpers.rank(compare, values, value, key = key)

        values.insert(index, value)

    return values


async def thread(function, loop = None, factory = threading.Thread):

    """
    Execute a function in a thread, wait for it to complete and get the result.
    """

    loop = asyncio.get_event_loop()

    event = asyncio.Event(loop = loop)

    result = NotImplemented

    def observe():

        nonlocal result

        result = function()

        loop.call_soon_threadsafe(event.set)

    thread = factory(target = observe)

    thread.start()

    await event.wait()

    return result


async def ready(*tasks, loop = None):

    """
    Yield tasks as they complete.
    """

    if not loop:

        loop = asyncio.get_event_loop()

    left = len(tasks)

    queue = asyncio.Queue(loop = loop)

    for task in tasks:

        task.add_done_callback(queue.put_nowait)

    while left:

        yield await queue.get()

        left -= 1


@helpers.decorate(0)
def cache(function, determine, maxsize = float('inf'), loop = None):

    """
    Similar to an LRU cache except it can wait before deciding whether to cache
    results from the same inputs with a timeout; can be limited to a max size.
    `determine` returns seconds, called with state arguments.
    """

    if not loop:

        loop = asyncio.get_event_loop()

    states = {}

    events = {}

    async def wrapper(*args, **kwargs):

        state = (*args, *kwargs.items())

        try:

            event = events[state]

        except KeyError:

            pass

        else:

            timeout = await determine(*state)

            try:

                await asyncio.wait_for(event.wait(), timeout, loop = loop)

            except asyncio.TimeoutError:

                pass

        try:

            result = states[state]

        except KeyError:

            events[state] = asyncio.Event(loop = loop)

            try:

                result = await function(*args, **kwargs)

            finally:

                events.pop(state).set()

            if len(states) < maxsize:

                states[state] = result

        return result

    return wrapper


async def reduce(function, iterable, first = marker):

    """
    Similar to an async version of functools.reduce except it's a generator
    yielding all results as they are computed. Iterate through for the final.
    """

    if first is marker:

        result = await iterable.__anext__()

    else:

        result = first

    async for value in iterable:

        result = await function(result, value)

        yield result


async def flatten(generator,
                  apply = lambda value: value,
                  predicate = lambda value: True):

    """
    Convenience async version of builtins.list with apply and predicate.
    """

    return [apply(value) async for value in generator if predicate(value)]


@helpers.decorate(0)
def infinite(execute, wait = None, signal = marker, loop = None):

    """
    Crate an infintely looping task calling the function after signal completes.
    """

    if not loop:

        loop = asyncio.get_event_loop()

    async def wrapper():

        if wait:

            await wait

        while True:

            result = await execute()

            if not result is marker:

                continue

            break

    coroutine = wrapper()

    task = loop.create_task(coroutine)

    return task


class Stream:

    """
    Progressively call functions until prompted to stop by returning signal.
    Specify which functions be called together or in order with bool on sub.
    Arguments passed in start will be used for all subsequent calls.
    Last function called before ending loop in start returns.
    """

    __slots__ = ('_loop', '_stores')

    signal = object()

    def __init__(self, loop = None):

        self._loop = loop or asyncio.get_event_loop()

        self._stores = ([], [])

    @helpers.decorate(1)
    def sub(self, function, group):

        self._stores[group].append(function)

    async def start(self, *args, **kwargs):

        bundle, single = self._stores

        coroutines = (function(*args, **kwargs) for function in bundle)

        future = asyncio.gather(*coroutines, loop = self._loop)

        asyncio.ensure_future(future, loop = self._loop)

        for function in single:

            signal = function(*args, **kwargs)

            if inspect.isawaitable(signal):

                signal = await signal

            if not signal is self.signal:

                continue

            break

        return function


class Valve:

    __slots__  = ('_loop', '_state')

    def __init__(self, state = None, loop = None):

        if not loop:

            loop = asyncio.get_event_loop()

        self._loop = loop

        self._state = state or []

    @property
    def state(self):

        """
        Get the state.
        """

        return self._state

    def count(self, key):

        """
        Get the number of values adhering to the key.
        """

        return len(tuple(filter(key, self._state)))

    def left(self, key, limit):

        """
        Get the number of room left according to the limit.
        """

        return limit - self.count(key)

    def observe(self, value, period):

        """
        Track value, wait for period and discard it.
        """

        manage = lambda task: self._state.remove(value)

        coroutine = asyncio.sleep(period)

        self._state.append(value)

        task = self._loop.create_task(coroutine)

        task.add_done_callback(manage)

        return task

    def check(self, value, period, limit, key, bypass = False):

        """
        Check if the valve is open. If it is, track value.
        Returns the number of spaces left before adding value.
        """

        left = self.left(key, limit)

        if bypass or left:

            self.observe(value, period)

        return left


@helpers.decorate(0)
def throttle(function, period, limit = 1, loop = None, signal = None):

    """
    Disallow a function's execution during a period.
    """

    valve = Valve(loop = loop)

    key = lambda value: value is function

    def observe(*args, **kwargs):

        if not valve.check(function, period, limit, key):

            return signal

        return function(*args, **kwargs)

    return observe


class Sling(Valve):

    __all__ = ()

    def check(self, value, period, limit, trail, rate, key):

        """
        Check if the valve is open. Calculate period and track state.
        """

        left = self.left(key, limit)

        period *= ((left + trail) / limit) * rate

        self.observe(value, period)

        return max(left, 0)


async def interload(path, callback):

    """
    Import module upon changes in path and pass to callback.
    Storage in sys.modules allows for relative imports within the package.
    Note that `watchgod` must be manually installed.
    """

    import watchgod # install

    watching = watchgod.awatch(path)

    (lead, name) = os.path.split(path)

    origin = os.path.join(path, '__init__.py')

    while True:

        spec = importlib.util.spec_from_file_location(name, origin)

        module = importlib.util.module_from_spec(spec)

        sys.modules[name] = module

        spec.loader.exec_module(module)

        callback(module)

        await watching.__anext__()

        for trail in tuple(sys.modules):

            if not trail.startswith(name):

                continue

            del sys.modules[trail]
