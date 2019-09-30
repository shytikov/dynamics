from dynamics.objects import Connection


def connect(resource: str, login: str, password: str):
    return Connection(resource, login, password)
