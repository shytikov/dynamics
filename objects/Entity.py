# flake8: noqa

from dynamics.objects.Utils import Utils
import pandas


class Entity:

    def __init__(self, connection, name: str):
        self.endpoint = f'{connection.resource}/api/data/v9.1/'
        self.name = name
        self.base = None
        self.data = connection.data
        self.metadata = None
        self.intersection = False

        self.session = Utils.get_session(
            connection.resource,
            connection.login,
            connection.password
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
                    part1 = f"{self.endpoint}{name0}({row[1][column0]})"
                    part2 = f"{self.endpoint}{name1}({row[1][column1]})"
                    url = f"{part1}/{self.name}/$ref?$id={part2}"
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

    def read(self) -> None:
        """
        Reads data to the entity instance. Either from MS Dynamics or from DataFrame supplied.
        Uses metadata information to ensure that integers will be represented as correct integers,
        and date values — as correct datatype.
        """

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

        self.data.drop(self.data.index, axis=0, inplace=True)
        self.data.drop(self.data.columns, axis=1, inplace=True)

        data = pandas.DataFrame(data)
        self.data[data.columns] = data

        columns = []
        columns.extend(self.metadata[self.metadata['AttributeType'] == 'Integer']['LogicalName'].tolist())
        columns.extend(self.metadata[self.metadata['AttributeType'] == 'Picklist']['LogicalName'].tolist())
        Utils.any_to_int(self.data, columns)

        columns = self.metadata[self.metadata['AttributeType'] == 'DateTime']['LogicalName'].tolist()
        Utils.any_to_dt(self.data, columns)

        columns = self.data.columns.to_list()

        lookups = list(filter(lambda x: x.startswith('_') and x.endswith('_value'), columns))
        annotations = list(filter(lambda x: '@' in x, columns))
        targets = list(filter(lambda x: '.lookuplogicalname' in x, annotations))
        properties = list(filter(lambda x: '.associatednavigationproperty' in x, annotations))

        collections = []

        for item in targets:
            collections.extend(self.data[item].unique().tolist())

        collections = dict.fromkeys(collections)
        collections = dict.fromkeys([x for x in collections if str(x) != 'nan'])

        for item in collections:
            response = self.session.get(f"{self.endpoint}EntityDefinitions(LogicalName='{item}')?$select=EntitySetName")
            response = response.json()
            collections[item] = response['EntitySetName']

        for item in targets:
            self.data[item].replace(collections, inplace=True)
            attribute = item.split('@')[0]
            self.data[attribute] = '/' + self.data[item].astype(str) + '/(' + self.data[attribute] + ')'

        keys = []
        values = []

        for item in properties:
            value = [x for x in self.data[item].unique() if str(x) != 'nan']
            if len(value) > 0:
                values.append(f"{value[0]}@odata.bind")
                keys.append(item.split('@')[0])

        self.data.drop(annotations, axis=1, inplace=True)

        self.data.rename(dict(zip(keys, values)), axis=1, inplace=True)

    def write(self, data: pandas.DataFrame = None) -> None:
        """
        Writes to the entity. Either from internal instance data or from dataframe supplied.
        """
        pass
