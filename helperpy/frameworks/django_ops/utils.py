from typing import Any, Dict, List, Optional, Union
import mimetypes

from django.db.models import QuerySet
from django.http import HttpResponse
import pandas as pd
from rest_framework.request import Request

from helperpy.core.exceptions import MissingRequiredParameterError
from helperpy.core.file_io import get_basename_from_filepath
from helperpy.data_wrangler.transform import (
    dataframe_to_list,
    drop_columns_if_exists,
)


def queryset_to_dataframe(
        qs: QuerySet,
        fields_to_drop: Optional[List[str]] = None,
    ) -> pd.DataFrame:
    df = pd.DataFrame(data=list(qs.values()))
    if fields_to_drop:
        df = drop_columns_if_exists(data=df, columns=fields_to_drop)
    return df


def queryset_to_list(
        qs: QuerySet,
        fields_to_drop: Optional[List[str]] = None,
    ) -> Union[List[Dict], List]:
    df = queryset_to_dataframe(qs=qs, fields_to_drop=fields_to_drop)
    list_of_dicts = dataframe_to_list(data=df)
    return list_of_dicts


def file_to_django_http_response(filepath: str) -> HttpResponse:
    """
    Converts the given file to Django HTTP Response object.
    Works for the following file extensions: ['csv', 'docx', 'flv', 'jpg', 'm4a', 'mp3', 'mp4', 'pdf', 'png', 'txt', 'xls', 'xlsx', 'zip'].
    """
    type_, encoding = mimetypes.guess_type(url=filepath)
    filename = get_basename_from_filepath(filepath=filepath)
    if type_ is None or encoding is not None:
        type_ = 'application/octet-stream'
    with open(file=filepath, mode='rb') as fp:
        http_response = HttpResponse(content=fp, content_type=type_)
    http_response['Content-Disposition'] = f'attachment; filename="{filename}"'
    http_response['X-Sendfile'] = filename
    return http_response


def get_params_from_request_payload(
        request: Request,
        required_params: Optional[List[str]] = None,
        optional_params: Optional[List[str]] = None,
    ) -> Dict[str, Union[Any, None]]:
    """
    Gets parameters from the request's payload (the request object must be of type `rest_framework.request.Request`).
    Raises an exception if any of the `required_params` are missing.
    Otherwise returns a dictionary having keys = parameter name, and values = parameter value.
    """
    required_params = [] if required_params is None else required_params
    optional_params = [] if optional_params is None else optional_params
    all_params = required_params + optional_params
    all_unique_params = list(set(all_params))
    if len(all_params) != len(all_unique_params):
        raise ValueError(
            f"The given parameters (`required_params` + `optional_params`) must be unique. All parameters received: {all_params}"
        )
    
    dict_parsed_payload = {}
    for required_param in required_params:
        dict_parsed_payload[required_param] = request.data.get(required_param, None)
        if dict_parsed_payload[required_param] is None:
            raise MissingRequiredParameterError(
                f"The parameter '{required_param}' is required in the payload, but is not passed in"
            )
    for optional_param in optional_params:
        dict_parsed_payload[optional_param] = request.data.get(optional_param, None)
    return dict_parsed_payload


def get_query_params_from_request(
        request: Request,
        required_params: Optional[List[str]] = None,
        optional_params: Optional[List[str]] = None,
    ) -> Dict[str, Union[str, None]]:
    """
    Gets query parameters from the request (the request object must be of type `rest_framework.request.Request`).
    Raises an exception if any of the `required_params` are missing.
    Otherwise returns a dictionary having keys = parameter name, and values = parameter value.
    """
    required_params = [] if required_params is None else required_params
    optional_params = [] if optional_params is None else optional_params
    all_params = required_params + optional_params
    all_unique_params = list(set(all_params))
    if len(all_params) != len(all_unique_params):
        raise ValueError(
            f"The given parameters (`required_params` + `optional_params`) must be unique. All parameters received: {all_params}"
        )
    
    dict_query_params = {}
    for required_param in required_params:
        dict_query_params[required_param] = request.query_params.get(required_param, None)
        if dict_query_params[required_param] is None:
            raise MissingRequiredParameterError(
                f"The query-parameter '{required_param}' is required, but is not passed in"
            )
    for optional_param in optional_params:
        dict_query_params[optional_param] = request.query_params.get(optional_param, None)
    return dict_query_params


def get_django_request_info(request: Request) -> Dict[str, Any]:
    """
    Takes in DRF request object of type `rest_framework.request.Request` and returns
    dictionary having certain information about the request.
    Keys returned: ['content_type', 'method', 'session_info', 'stream', 'uri_absolute', 'uri_with_query_params', 'uri_without_query_params']
    """
    request_info = {
        'content_type': request.content_type,
        'method': request.method,
        'session_info': dict(request.session),
        'stream': request.stream,
        'uri_absolute': request.build_absolute_uri(),
        'uri_with_query_params': request.get_full_path(),
        'uri_without_query_params': request.path,
    }
    return request_info


def get_django_request_info_from_args(*args) -> Dict[str, Any]:
    """
    Searches for DRF request object of type `rest_framework.request.Request` (if any) from the given arguments.
    If it exists, returns dictionary having certain information about the request. Otherwise, returns an empty dictionary.
    """
    dict_request_info = {}
    for arg in args:
        if isinstance(arg, Request):
            dict_request_info = get_django_request_info(request=arg)
            break
    return dict_request_info