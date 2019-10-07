# flake8: noqa

import adal
import pandas
import requests


class Utils:
    def any_to_int(data: pandas.DataFrame, columns: list) -> None:
        for column in columns:
            loc = data.columns.get_loc(column)

            value = pandas.to_numeric(data[column], downcast='integer')
            value = value.fillna(0.0).astype(int)

            # Deleting initial data from the dataframe
            data.pop(column)
            data.insert(loc=loc, column=column, value=value)

    def any_to_dt(data: pandas.DataFrame, columns: list) -> None:
        for column in columns:
            data[column] = pandas.to_datetime(data[column])

    def get_session(resource: str, login: str, password: str) -> requests.Session:
        context = adal.AuthenticationContext("https://login.microsoftonline.com/common/")

        result = context.acquire_token_with_username_password(
            resource,
            login,
            password,
            '2ad88395-b77d-4561-9441-d0e40824f9bc')

        session = requests.Session()
        session.headers = {
            'Authorization': 'Bearer ' + result['accessToken'],
            'OData-MaxVersion': '4.0',
            'OData-Version': '4.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json; charset=utf-8',
            'Prefer': 'odata.include-annotations="*"'
        }

        return session
