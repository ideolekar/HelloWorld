import operator
import functools


__all__ = ('Access',)


_containers = (tuple, list, set, frozenset)


class Access:

    __slots__ = ('_objects', '_model')

    def __init__(self, objects, model):

        self._objects = objects

        self._model = model

    @property
    def name(self):

        return self._model._meta.name

    @property
    def primary(self):

        keys = self._model._meta.get_primary_keys()

        return tuple(key.column_name for key in keys)

    @staticmethod
    def _compatible(keys):

        (*leads, finals) = keys

        if isinstance(finals, _containers):

            finals = tuple(finals)

        else:

            finals = (finals,)

        return (leads, finals)

    def query(self, source, keys):

        (leads, finals) = self._compatible(keys)

        fields = (getattr(self._model, key) for key in self.primary)

        if leads:

            associations = (field == key for (field, key) in zip(leads, fields))

            action = functools.reduce(operator.and_, associations)

            source = source.where(action)

        try:

            field = next(fields)

        except StopIteration:

            pass

        else:

            source = source.where(field << finals)

        return source

    def _revert(self, data):

        for (key, value) in data.items():

            if not value is None:

                continue

            data[key] = getattr(self._model, key).default

    def select(self, keys, names):

        columns = (getattr(self._model, name) for name in names)

        action = self._model.select(*columns).dicts()

        if keys:

            action = self.query(action, keys)

        return self._objects.execute(action)

    async def create(self, keys, data):

        self._revert(data)

        (leads, finals) = self._compatible(keys)

        series = ((*leads, final) for final in finals)

        extras = (zip(self.primary, keys) for keys in series)

        async with self._objects.atomic():

            for extra in extras:

                data.update(extra)

                action = self._model.insert(**data)

                await self._objects.execute(action)

    async def update(self, keys, data):

        self._revert(data)

        action = self._model.update(**data)

        if keys:

            action = self.query(action, keys)

        await self._objects.execute(action)

    async def delete(self, keys):

        action = self._model.delete()

        if keys:

            action = self.query(action, keys)

        await self._objects.execute(action)
