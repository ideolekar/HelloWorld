

__all__ = ('RequestError', 'AuthorizationError', 'BadRequestError')


class RequestError(Exception):

    __slots__ = ('response', 'code', 'info')

    def __init__(self, response, code, info):

        self.response = response

        self.code = code

        self.info = info

    def __repr__(self):

        return f'{self.__class__.__name__}: {self.code} {self.info}'


class AuthorizationError(RequestError):

    __slots__ = ()


class BadRequestError(RequestError):

    __slots__ = ()
