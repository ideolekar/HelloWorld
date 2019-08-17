import functools


__all__ = ()


def authorization(spot, key, value, options):

    store = options.setdefault(spot, {})

    store[key] = value


authorization_spot = 'headers'


authorization_header = 'Authorization'


authorization_parts = (authorization_spot, authorization_header)


authorize = functools.partial(authorization, *authorization_parts)


authorizer = lambda value: functools.partial(authorize, value)
