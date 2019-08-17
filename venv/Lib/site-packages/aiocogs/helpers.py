

__all__ = ()


def rank(compare, iterable, root, key = None):

    """
    Determine the root's rank in an iterable.
    """

    index = 0

    for value in iterable:

        args = (root, value)

        if key:

            args = map(key, args)

        if not compare(*args):

            break

        index += 1

    return index


def decorate(spot):

    """
    Turn a function into a decorator.
    Inserts callable argument to spot.
    """

    def wrapper(value):

        def decorator(*args, **kwargs):

            args = list(args)

            def wrapper(function):

                args.insert(spot, function)

                return value(*args, **kwargs)

            return wrapper

        return decorator

    return wrapper
