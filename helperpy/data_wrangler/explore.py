from typing import Dict, List, Optional, Union

import pandas as pd

from helperpy.core.exceptions import InvalidDataFrameError
from helperpy.core.type_annotations import NumberOrString


def is_dataframe_full(data: pd.DataFrame) -> bool:
    """Returns True if there are no missing values in a non-empty DataFrame"""
    is_not_empty = (not data.empty)
    has_no_missing_values = (data.isnull().sum().sum() == 0)
    return (is_not_empty & has_no_missing_values)


def get_column_count(dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Returns DataFrame having count of all columns present in the given DataFrames.
    The returned DataFrame will have the following columns: ['Column', 'Count'].
    """
    all_columns = []
    for df in dataframes:
        all_columns.extend(df.columns.tolist())
    df_column_count = pd.Series(data=all_columns).value_counts().rename('Count').reset_index().rename(mapper={'index': 'Column'}, axis=1)
    return df_column_count


def get_common_columns(dataframes: List[pd.DataFrame]) -> Union[List[NumberOrString], List]:
    """Returns list of the common columns present in all the given DataFrames"""
    num_dataframes = len(dataframes)
    df_column_count = get_column_count(dataframes=dataframes)
    common_columns = df_column_count[df_column_count['Count'] == num_dataframes]['Column'].tolist()
    return common_columns


def get_column_availability_info(
        data: pd.DataFrame,
        expected_columns: List[NumberOrString],
    ) -> Dict[str, List[str]]:
    """
    Takes in non-empty DataFrame, and list of columns expected to be in said DataFrame.
    Returns dictionary having 2 keys: ['columns_available', 'columns_missing'], wherein the value for
    each key will be a list of columns that are available/missing.
    """
    if data.empty:
        raise InvalidDataFrameError("Expected a non-empty DataFrame, but got an empty DataFrame")
    all_columns = data.columns.tolist()
    expected_columns = list(expected_columns)
    columns_available = list(
        set(expected_columns).intersection(set(all_columns))
    )
    columns_missing = list(
        set(expected_columns).difference(set(all_columns))
    )
    dict_obj = {
        'columns_available': columns_available,
        'columns_missing': columns_missing,
    }
    return dict_obj


def has_all_expected_columns(
        data: pd.DataFrame,
        expected_columns: List[NumberOrString],
    ) -> bool:
    """Returns True if all expected columns are available in given DataFrame; otherwise returns False"""
    dict_column_availability_info = get_column_availability_info(
        data=data,
        expected_columns=expected_columns,
    )
    return len(dict_column_availability_info['columns_missing']) == 0


def describe_missing_data(
        data: pd.DataFrame,
        show_all_columns: Optional[bool] = True,
    ) -> pd.DataFrame:
    """
    Returns DataFrame having information about missing values (if any) present in a non-empty DataFrame.
    The DataFrame returned will have the following columns: ['Column', 'NumMissingValues', 'PercentMissingValues', 'DataType'].
    """
    if data.empty:
        raise InvalidDataFrameError("Expected a non-empty DataFrame, but got an empty DataFrame")
    columns = data.columns.tolist()
    num_missing_values = data.isnull().sum().values
    percent_missing_values = pd.Series(data=num_missing_values * 100 / len(data)).apply(round, args=[2])
    datatypes = data.dtypes.values
    df_missing_data_info = pd.DataFrame(data={
        'Column': columns,
        'NumMissingValues': num_missing_values,
        'PercentMissingValues': percent_missing_values,
        'DataType': datatypes,
    })
    df_missing_data_info.sort_values(
        by=['PercentMissingValues', 'Column'],
        ascending=[False, True],
        ignore_index=True,
        inplace=True,
    )
    if not show_all_columns:
        df_missing_data_info = df_missing_data_info[df_missing_data_info['PercentMissingValues'] > 0]
    return df_missing_data_info