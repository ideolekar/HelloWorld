import asyncio
import aiohttp.web
import functools

from . import access
from . import seed
from . import handle


__all__ = ()


def authorization(error, key, value, store):

    try:

        provide = store[key]

    except KeyError:

        raise error('missing')

    if provide == value:

        return

    raise error('invalid')


def fail(cls, *data):

    response = aiohttp.web.json_response(data)

    return cls(body = response.body, content_type = response.content_type)


authorization_header = 'Authorization'


authorization_error = aiohttp.web.HTTPUnauthorized


authorization_fail = functools.partial(fail, authorization_error)


authorization_parts = (authorization_fail, authorization_header)


specific_authorize = functools.partial(authorization, *authorization_parts)


def specific_authorization(spot, value, request):

    store = getattr(request, spot)

    specific_authorize(value, store)


authorization_spot = 'headers'


authorize = functools.partial(specific_authorization, authorization_spot)


authorizer = lambda value: functools.partial(authorize, value)


handle_error = aiohttp.web.HTTPBadRequest


handle_fail = functools.partial(fail, authorization_error)


ignore = ('GET',)


methods = (*ignore, 'POST', 'PATCH', 'DELETE')


schemas = ((list, list), (list, dict), (list, dict), (list,))


accept = dict(zip(methods, schemas))


async def parse(error, method, data):

    schema = accept[method]

    if not len(data) == len(schema):

        raise error('invalid data length')

    values = []

    for (data, cls) in zip(data, schema):

        if not isinstance(data, cls):

            raise error('invalid data type')

        values.append(data)

    return values


def create(client, objects, model, authorize, fail, parse = parse):

    access_ = access.Access(objects, model)

    client.inform(access_.name, access_.primary)

    seed_ = seed.Seed(access_, tuple)

    def _parse(method, data):

        return parse(fail, method, data)

    handle_ = handle.Handle(seed_, authorize, fail, _parse)

    return handle_


def listen(action, alert):

    async def wrapper(request):

        data = await action(request)

        if alert:

            await alert(data)

        return aiohttp.web.json_response(data)

    return wrapper


def funnel(client, handle):

    name = handle.seed.access.name

    actions = (handle.select, handle.create, handle.update, handle.delete)

    path = '/' + name

    def vocal(*args):

        def wrapper(data):

            return client.dispatch(*args, data)

        return wrapper

    for (method, action) in zip(methods, actions):

        dispatch = None if method in ignore else vocal(method, name)

        action = listen(action, dispatch)

        yield (method, path, action)


def setup(client, *args, **kwargs):

    handle = create(client, *args, **kwargs)

    yield from funnel(client, handle)
