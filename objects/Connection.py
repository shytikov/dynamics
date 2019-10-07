# flake8: noqa

import pandas
from dynamics.objects.Entity import Entity

class Connection:
    """
    Class to hold required information about MS Dynamics connection 
    """

    def __init__(self, obj: pandas.DataFrame, resource: str, login: str, password: str):
        """
        Initiating new connection object by storing important information
        """

        self.data = obj

        # Making sure that trailing slash in not included to resource
        index = len(resource)-1
        if resource[index] == '/':
            resource = resource[0:index]

        self.resource = resource
        self.login = login
        self.password = password

    def entity(self, name: str) -> Entity:
        return Entity(self, name)
