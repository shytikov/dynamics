# flake8: noqa

import pandas
import dynamics.utils


class Entity:

    def __init__(self, connection, name: str, prefer_annotations: bool = False):
        self.endpoint = f'{connection.resource}/api/data/v9.1/'
        self.name = name
        self.base = None
        self.data = None
        self.metadata = None
        self.intersection = False

        self.session = dynamics.utils.get_session(
            connection.resource,
            connection.login,
            connection.password,
            prefer_annotations
        )

        url = f"{self.endpoint}EntityDefinitions(LogicalName='{self.name}')?$select=IsIntersect,EntitySetName&$expand=Attributes"

        result = self.session.get(url)

        if result.ok:
            result = result.json()
            self.metadata = pandas.DataFrame(result['Attributes'])
            self.metadata = self.metadata[self.metadata['LogicalName'] != 'versionnumber']
            self.metadata.pop('@odata.type')

            self.base = f"{self.endpoint}{result['EntitySetName']}"
            self.intersection = result['IsIntersect']

            if self.intersection:
                condition = self.metadata['IsPrimaryId'] == False
                self.metadata['EntitySetName'] = self.metadata[condition]['LogicalName'].str[:-2]
                # Finding out names of collections referenced in the relationship
                for row in self.metadata[condition].iterrows():
                    url = f"{self.endpoint}EntityDefinitions(LogicalName='{row[1]['EntitySetName']}')?$select=EntitySetName"
                    result = self.session.get(url)
                    if result.ok:
                        self.metadata.loc[row[0], 'EntitySetName'] = result.json()['EntitySetName']
        else:
            raise Exception(f"Metadata request has failed. Check request url:\n{url}")

    def delete(self) -> None:
        if self.data is not None:
            urls = []
            if self.intersection:
                condition = self.metadata['IsPrimaryId'] == False

                name0 = self.metadata[condition].iloc[0]['EntitySetName']
                name1 = self.metadata[condition].iloc[1]['EntitySetName']

                column0 = self.metadata[condition].iloc[0]['LogicalName']
                column1 = self.metadata[condition].iloc[1]['LogicalName']

                # Composing disassociate URL all rows in the DataFrame
                for row in self.data.iterrows():
                    url = f"{self.endpoint}{name0}({row[1][column0]})/{self.name}/$ref?$id={self.endpoint}{name1}({row[1][column1]})"
                    urls.append(url)
            else:
                condition = self.metadata['IsPrimaryId'] == True
                key = self.metadata[condition].iloc[0]['LogicalName']

                # Composing disassociate URL all rows in the DataFrame
                for row in self.data.iterrows():
                    url = f"{self.endpoint}{name}({row[1][key]})"
                    urls.append(url)

            for url in urls:
                self.session.delete(url)

    def read(self, data: pandas.DataFrame = None) -> None:
        """
        Reads data to the entity instance. Either from MS Dynamics or from DataFrame supplied.
        Uses metadata information to ensure that integers will be represented as correct integers,
        and date values — as correct datatype.
        """

        if data is None:
            data = []
            key = '@odata.nextLink'
            url = self.base
            
            while True:
                result = self.session.get(url)
                result = result.json()
                data.extend(result['value'])

                if key in result:
                    url = result[key]
                else:
                    break
            
            self.data = pandas.DataFrame(data)
            self.data.pop('@odata.etag')
        else:
            self.data = pandas.DataFrame(data)

        columns = []
        columns.extend(self.metadata[self.metadata['AttributeType'] == 'Integer']['LogicalName'].tolist())
        columns.extend(self.metadata[self.metadata['AttributeType'] == 'Picklist']['LogicalName'].tolist())
        dynamics.utils.any_to_int(self.data, columns)

        columns = self.metadata[self.metadata['AttributeType'] == 'DateTime']['LogicalName'].tolist()
        dynamics.utils.any_to_dt(self.data, columns)

    def write(self, data: pandas.DataFrame) -> None:
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
