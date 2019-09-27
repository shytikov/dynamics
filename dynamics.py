import adal
import pandas
import requests


class Utils:

    def any_to_int(data, columns):
        for column in columns:
            loc = data.columns.get_loc(column)

            value = pandas.to_numeric(data[column], downcast='integer')
            value = value.fillna(0.0).astype(int)

            # Deleting initial data from the dataframe
            data.pop(column)
            data.insert(loc=loc, column=column, value=value)

    def any_to_dt(data, columns):
        for column in columns:
            data[column] = pandas.to_datetime(data[column])


class Entity:

    def __init__(self, connection, name, prefer_annotations=False):
        self.endpoint = f'{connection.resource}/api/data/v9.1/'
        self.name = name
        self.data = None
        self.metadata = None

        context = adal.AuthenticationContext("https://login.microsoftonline.com/common/")

        result = context.acquire_token_with_username_password(
            connection.resource,
            connection.login,
            connection.password,
            '2ad88395-b77d-4561-9441-d0e40824f9bc')

        self.session = requests.Session()
        self.session.headers = {
            'Authorization': 'Bearer ' + result['accessToken'],
            'OData-MaxVersion': '4.0',
            'OData-Version': '4.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json; charset=utf-8',
        }

        if prefer_annotations:
            self.session.headers['Prefer'] = 'odata.include-annotations=OData.Community.Display.V1.FormattedValue'

        query = "$select=EntitySetName&$expand=Attributes($select=LogicalName,AttributeType,IsValidForUpdate)"

        result = self.session.get(f"{self.endpoint}EntityDefinitions(LogicalName='{self.name}')?{query}")

        if result.ok:
            result = result.json()
            self.metadata = pandas.DataFrame(result['Attributes'])
            self.metadata = self.metadata[['LogicalName', 'AttributeType', 'IsValidForUpdate']]

            self.base = f"{self.endpoint}{result['EntitySetName']}"
        else:
            raise Exception(f"Metadata request has failed. Check name of the entity: '{self.name}'.")

    def read(self) -> pandas.DataFrame:
        result = self.session.get(f'{self.base}')
        result = result.json()
        result = result['value']

        self.data = pandas.DataFrame(result)
        self.data.pop('@odata.etag')

        columns = []
        columns.extend(self.metadata[self.metadata['AttributeType'] == 'Integer']['LogicalName'].tolist())
        columns.extend(self.metadata[self.metadata['AttributeType'] == 'Picklist']['LogicalName'].tolist())
        Utils.any_to_int(self.data, columns)

        columns = self.metadata[self.metadata['AttributeType'] == 'DateTime']['LogicalName'].tolist()
        Utils.any_to_dt(self.data, columns)

        return self.data

    def write(self, data: pandas.DataFrame):
        pass


class Connection:
    """
    Class to hold required information about MS Dynamics connection 
    """
    def __init__(self, resource, login, password):
        self.resource = resource
        self.login = login
        self.password = password

    def entity(self, name, prefer_annotations=False) -> Entity:
        return Entity(self, name, prefer_annotations)
