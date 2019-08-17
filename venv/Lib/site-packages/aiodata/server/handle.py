import peewee


__all__ = ('Handle',)


class Handle:

    __slots__ = ('_seed', '_authorize', '_error', '_parse')

    def __init__(self, seed, authorize, error, parse):

        self._seed = seed

        self._authorize = authorize

        self._error = error

        self._parse = parse

    @property
    def seed(self):

        return self._seed

    async def _execute(self, action, request, accept):

        self._authorize(request)

        try:

            data = await request.json()

        except:

            raise self._error('invalid data')

        method = request.method

        path = request.url.path

        (keys, *rest) = await self._parse(method, data)

        try:

            data = await action(keys, *rest)

        except peewee.DatabaseError as error:

            raise self._error('database fail', str(error))

        return data

    def select(self, request):

        accept = (list, list) # [keys, names]

        return self._execute(self._seed.select, request, accept)

    def create(self, request):

        accept = (list, dict) # [keys, data]

        return self._execute(self._seed.create, request, accept)

    def update(self, request):

        accept = (list, dict) # [keys, data]

        return self._execute(self._seed.update, request, accept)

    def delete(self, request):

        accept = (list,) # [keys]

        return self._execute(self._seed.delete, request, accept)
