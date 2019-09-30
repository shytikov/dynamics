import pandas
import dynamics.utils


class Entity:

    def __init__(self, connection, name: str, prefer_annotations: bool = False):
        self.endpoint = f'{connection.resource}/api/data/v9.1/'
        self.name = name
        self.data = None
        self.metadata = None

        self.session = dynamics.utils.get_session(
            connection.resource,
            connection.login,
            connection.password,
            prefer_annotations
        )

        query = "$select=EntitySetName&$expand=Attributes($select=LogicalName,AttributeType,IsValidForUpdate)"

        result = self.session.get(f"{self.endpoint}EntityDefinitions(LogicalName='{self.name}')?{query}")

        if result.ok:
            result = result.json()
            self.metadata = pandas.DataFrame(result['Attributes'])
            self.metadata = self.metadata[['LogicalName', 'AttributeType', 'IsValidForUpdate']]

            self.base = f"{self.endpoint}{result['EntitySetName']}"
        else:
            raise Exception(f"Metadata request has failed. Check if name of the entity is correct: '{self.name}'.")

    def read(self, data: pandas.DataFrame = None) -> pandas.DataFrame:
        """
        Reads data to the entity instance. Either from MS Dynamics or from DataFrame supplied.
        Uses metadata information to ensure that integers will be represented as correct integers,
        and date values — as correct datatype.
        """

        if data is None:
            result = self.session.get(f'{self.base}')
            result = result.json()
            result = result['value']

            self.data = pandas.DataFrame(result)
            self.data.pop('@odata.etag')
        else:
            self.data = pandas.DataFrame(data)

        columns = []
        columns.extend(self.metadata[self.metadata['AttributeType'] == 'Integer']['LogicalName'].tolist())
        columns.extend(self.metadata[self.metadata['AttributeType'] == 'Picklist']['LogicalName'].tolist())
        dynamics.utils.any_to_int(self.data, columns)

        columns = self.metadata[self.metadata['AttributeType'] == 'DateTime']['LogicalName'].tolist()
        dynamics.utils.any_to_dt(self.data, columns)

        return self.data

    def write(self, data: pandas.DataFrame):
        pass


class Connection:
    """
    Class to hold required information about MS Dynamics connection 
    """

    def __init__(self, resource: str, login: str, password: str):
        """
        Initiating new connection object by storing important information
        """

        # Making sure that trailing slash in not included to resource
        index = len(resource)-1
        if resource[index] == '/':
            resource = resource[0:index]

        self.resource = resource
        self.login = login
        self.password = password

    def entity(self, name: str, prefer_annotations: bool = False) -> Entity:
        return Entity(self, name, prefer_annotations)
