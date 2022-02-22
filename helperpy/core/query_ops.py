from typing import Any, Dict, List, Union

import pandas as pd

from helperpy.core.string_ops import remove_last_n_characters
from helperpy.core.utils import is_none_or_nan
from helperpy.data_wrangler.explore import get_column_availability_info


def wrap_string_with_appropriate_quotes(text: str) -> str:
    """Wraps given string with appropriate quotes (for SQL insert query)"""
    has_single_quote = "'" in text
    has_double_quote = '"' in text
    if not has_single_quote and not has_double_quote:
        return f"'{text}'"
    if has_single_quote and not has_double_quote:
        return f'"{text}"'
    if not has_single_quote and has_double_quote:
        return f"'{text}'"
    return f'"{text}"'


def get_in_query_for_numbers(numbers: List[Union[int, float]]) -> str:
    comma_separated_numbers = ", ".join(map(str, numbers))
    return f"({comma_separated_numbers})"


def get_in_query_for_strings(strings: List[str]) -> str:
    comma_separated_strings = ", ".join(map(lambda string: wrap_string_with_appropriate_quotes(text=string), strings))
    return f"({comma_separated_strings})"


def __wrap_boolean_value(value: Any) -> str:
    """
    If `value` is Python's native True, returns 'true'.
    If `value` is Python's native False, returns 'false'.
    Otherwise returns 'null'.
    """
    if value is True:
        return 'true'
    if value is False:
        return 'false'
    return 'null'


def __wrap_value_by_sql_datatype(
        value: Union[int, float, str, None],
        datatype: str,
    ) -> Union[int, float, str]:
    # Handle nulls
    if is_none_or_nan(value=value):
        return 'null'
    
    # Handle non-nulls
    if datatype == 'integer':
        return int(value)
    elif datatype == 'float':
        return float(value)
    elif datatype == 'string':
        return wrap_string_with_appropriate_quotes(text=str(value))
    elif datatype == 'date':
        return f"DATE('{value}')"
    elif datatype == 'timestamp':
        return f"TIMESTAMP('{value}')"
    elif datatype == 'boolean':
        return __wrap_boolean_value(value=value)
    raise ValueError(f"Got unexpected option for `datatype`: '{datatype}'")


def __get_query_string_of_records(
        records: List[Dict],
        columns: List[str],
        datatypes: List[str],
    ) -> str:
    query = ""
    for record in records:
        values_per_record = []
        for column, datatype in zip(columns, datatypes):
            value = __wrap_value_by_sql_datatype(
                value=record[column],
                datatype=datatype,
            )
            values_per_record.append(value)
        comma_separated_values = ', '.join(map(str, values_per_record))
        query += f"({comma_separated_values}),\n"
    query = remove_last_n_characters(text=query, num_chars=2)
    return query


def generate_insert_query(
        data: pd.DataFrame,
        table_name: str,
        column_to_datatype_mapper: Dict[str, str],
    ) -> str:
    """
    Takes in DataFrame having the data to be inserted into a table.
    Returns an INSERT query (string) for the given DataFrame.
    Evaluates Python's None and Numpy's NaN to be null values.
    Expects column names of the DataFrame to match the column names of the table
    where the records will be inserted.

    Accepted data-type options for the columns in the DataFrame:
        - integer
        - float
        - string
        - date: Values must be strings of format "yyyy-mm-dd". Eg: "2020-03-15"
        - timestamp: Values must be strings of format: "yyyy-mm-dd hh:mm:ss". Eg: "2020-03-15 19:30:55"
        - boolean: Values must be Python's native True/False. Anything else will be considered as a null value.
    
    >>> generate_insert_query(
            data=data, # Any DataFrame
            table_name='employee',
            column_to_datatype_mapper={
                'name': 'string',
                'age': 'integer',
                'date_of_birth': 'date',
                'joined_at': 'timestamp',
                'salary': 'float',
                'is_recent_recruit': 'boolean',
            },
        )
    """
    records = data.to_dict(orient='records')
    columns = list(column_to_datatype_mapper.keys())
    datatypes = list(column_to_datatype_mapper.values())

    # Data validation
    columns_missing = get_column_availability_info(data=data, expected_columns=columns)['columns_missing']
    if columns_missing:
        raise KeyError(f"The following columns from `column_to_datatype_mapper` are missing in the given DataFrame: {columns_missing}")
    valid_datatype_options = ['integer', 'float', 'string', 'date', 'timestamp', 'boolean']
    invalid_datatypes = list(set(datatypes).difference(set(valid_datatype_options)))
    if invalid_datatypes:
        raise ValueError(f"The following datatypes from `column_to_datatype_mapper` are invalid: {invalid_datatypes}. Valid datatype options are: {valid_datatype_options}")
    
    # Actual logic
    comma_separated_column_names = ', '.join(map(str, columns))
    query_string_of_records = __get_query_string_of_records(records=records, columns=columns, datatypes=datatypes)
    query = f"""INSERT INTO {table_name} ({comma_separated_column_names})\nVALUES\n{query_string_of_records}"""
    return query