from typing import Dict, List, Optional

from datetime import datetime
import random
import string

from faker import Faker
import numpy as np
import pandas as pd

from helperpy.core.date_ops import get_random_timestamp
from helperpy.core.exceptions import raise_exception_if_invalid_option


def generate_random_hex_code() -> str:
    """Generates random 6-digit hexadecimal code"""
    choices = '0123456789ABCDEF'
    random_hex_code = '#'
    for _ in range(6):
        random_hex_code += random.choice(choices)
    return random_hex_code


def generate_random_hex_codes(how_many: int) -> List[str]:
    """Returns list of random 6-digit hexadecimal codes"""
    return [generate_random_hex_code() for _ in range(how_many)]


def generate_random_string(
        length: Optional[int] = 15,
        include_lowercase: Optional[bool] = True,
        include_uppercase: Optional[bool] = True,
        include_digits: Optional[bool] = True,
        include_punctuations: Optional[bool] = True,
    ) -> str:
    character_set = ""
    if include_lowercase:
        character_set += string.ascii_lowercase
    if include_uppercase:
        character_set += string.ascii_uppercase
    if include_digits:
        character_set += string.digits
    if include_punctuations:
        character_set += string.punctuation
    return "".join((random.choice(character_set) for _ in range(length)))


def generate_random_data(
        num_records: int,
        column_to_datatype_mapper: Dict[str, str],
        insert_random_nulls: Optional[bool] = False,
    ) -> pd.DataFrame:
    """
    Returns DataFrame having randomly generated fake data.

    Accepted data-type options for the columns in the DataFrame:
        - integer
        - float
        - string
        - date
        - timestamp
        - boolean
    
    >>> generate_random_data(
            num_records=1000,
            column_to_datatype_mapper={
                'name': 'string',
                'age': 'integer',
                'date_of_birth': 'date',
                'joined_at': 'timestamp',
                'salary': 'float',
                'is_recent_recruit': 'boolean',
            },
            insert_random_nulls=False,
        )
    """
    datatypes = list(column_to_datatype_mapper.values())
    for datatype in datatypes:
        raise_exception_if_invalid_option(
            option_name='datatype',
            option_value=datatype,
            valid_option_values=['integer', 'float', 'string', 'date', 'timestamp', 'boolean'],
        )
    dict_obj = {}
    for column, datatype in column_to_datatype_mapper.items():
        if datatype == 'integer':
            dict_obj[column] = (random.randint(-99999, 99999) for _ in range(num_records))
        elif datatype == 'float':
            dict_obj[column] = (random.random() * random.choice([10, 100, 1000, 10000, 100000]) * random.choice([-1, 1]) for _ in range(num_records))
        elif datatype == 'string':
            dict_obj[column] = (generate_random_string(include_punctuations=False) for _ in range(num_records))
        elif datatype == 'date':
            dict_obj[column] = (get_random_timestamp().date() for _ in range(num_records))
        elif datatype == 'timestamp':
            dict_obj[column] = (get_random_timestamp() for _ in range(num_records))
        elif datatype == 'boolean':
            dict_obj[column] = (random.choice([True, False]) for _ in range(num_records))
    df = pd.DataFrame(data=dict_obj)
    if insert_random_nulls:
        df.mask(cond=np.random.choice([True, False], size=df.shape, p=[0.2, 0.8]), inplace=True)
    return df


def generate_random_profile_data(num_records: int) -> pd.DataFrame:
    """
    Returns DataFrame having randomly generated fake data of people's profiles.
    Columns to be returned: ['job', 'company', 'ssn', 'residence', 'current_location', 'blood_group', 'website', 'username', 'name', 'sex', 'address', 'mail', 'birthdate', 'birthdatetime']

    Reference:
        - https://towardsdatascience.com/generating-fake-data-with-python-c7a32c631b2a
        - https://www.caktusgroup.com/blog/2020/04/15/quick-guide-generating-fake-data-with-pandas/
    """
    faker = Faker()
    df = pd.DataFrame(data=(faker.profile() for _ in range(num_records)))
    df['birthdatetime'] = df['birthdate'].apply(
        lambda date_obj: datetime(
            year=date_obj.year,
            month=date_obj.month,
            day=date_obj.day,
            hour=random.choice(range(0, 23+1)),
            minute=random.choice(range(0, 59+1)),
            second=random.choice(range(0, 59+1)),
        )
    )
    return df