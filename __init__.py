# flake8: noqa

import pandas
import numpy
from dynamics.objects.Connection import Connection


@pandas.api.extensions.register_dataframe_accessor("dynamics")
class DynamicsAccessor:
    def __init__(self, obj: pandas.DataFrame) -> None:
        self.data = obj

    def connect(self, resource: str, login: str, password: str):
        return Connection(self.data, resource, login, password)
