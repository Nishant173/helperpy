"""
Description: Module for communicating with API endpoints asynchronously.

Docs:
    - https://docs.aiohttp.org/en/stable/client_reference.html

Functions exposed:
    - async_requests.get()
    - async_requests.post()
    - async_requests.put()
    - async_requests.patch()
    - async_requests.delete()

Parameters:
    - successful_status_codes (list): List of status codes that are considered successful for the requests made
    - urls (list): List of URLs (API endpoints) to call
    - data_items (list): List of data items. Each item must correspond to the URLs in `urls` parameter
    (pass in `data_items` only for POST, PUT, PATCH methods)
    - **request_kwargs: Kwargs related to the actual requests made (eg. headers). See `aiohttp` docs

Usage:
>>> num_api_calls = 1200
>>> results_for_get = async_requests.get(
        successful_status_codes=[200],
        urls=[f"https://pokeapi.co/api/v2/pokemon/{number}" for number in range(1, num_api_calls+1)],
        headers={},
    )
>>> results_for_post = async_requests.post(
        successful_status_codes=[201],
        urls=["https://reqres.in/api/users"] * num_api_calls,
        data_items=[{"name": f"MyName{number}", "age": number} for number in range(1, num_api_calls+1)],
        headers={},
    )
"""


from asyncio import (
    ensure_future,
    gather,
    get_event_loop,
)
from typing import Any, Callable, Dict, List, Optional

from aiohttp import ClientSession

ParsedResponse = Dict[str, Any]
ParsedResponses = List[ParsedResponse]


class _HTTP_METHOD_NAME:
    """Exposes class variables having the various HTTP method names"""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


def __choose_session_method(
        *,
        http_method: str,
        session_obj: ClientSession,
    ) -> Callable:
    """Returns callable that is used to make the asynchronous API requests"""
    session_method_mapper = {
        _HTTP_METHOD_NAME.GET: session_obj.get,
        _HTTP_METHOD_NAME.POST: session_obj.post,
        _HTTP_METHOD_NAME.PUT: session_obj.put,
        _HTTP_METHOD_NAME.PATCH: session_obj.patch,
        _HTTP_METHOD_NAME.DELETE: session_obj.delete,
    }
    if http_method not in session_method_mapper.keys():
        raise ValueError(f"Expected `http_method` to be in {list(session_method_mapper.keys())}, but got '{http_method}'")
    return session_method_mapper[http_method]


async def __make_api_call(
        *,
        successful_status_codes: List[int],
        method_to_call: Callable,
        url: str,
        data: Optional[Any] = None,
        **request_kwargs: Any,
    ) -> ParsedResponse:
    """
    Makes an API call, and returns parsed response in a dictionary having the keys: ['url', 'status_code', 'reason', 'method', 'data', 'error_details', 'ok']
    """
    data_as_kwargs = {} if data is None else {'data': data}
    async with method_to_call(url, **data_as_kwargs, **request_kwargs) as response:
        response_json_parser_kwargs = {"content_type": None} # For cases where there is no data in the response (usually for DELETE method)
        if response.status in successful_status_codes:
            data = await response.json(**response_json_parser_kwargs)
            error_details = {}
            ok = True
        else:
            data = None
            error_details = {
                "response_text": await response.text(),
            }
            ok = False
    return {
        "url": response.url,
        "status_code": response.status,
        "reason": response.reason,
        "method": response.method,
        "data": data,
        "error_details": error_details,
        "ok": ok,
    }


async def __make_api_calls_for_urls(
        *,
        http_method: str,
        successful_status_codes: List[int],
        urls: List[str],
        **request_kwargs: Any,
    ) -> ParsedResponses:
    """Helper function used to make asynchronous GET, DELETE requests"""
    parsed_responses = []
    actions = []
    async with ClientSession() as session:
        method_to_call = __choose_session_method(
            http_method=http_method,
            session_obj=session,
        )
        for url in urls:
            actions.append(
                ensure_future(
                    __make_api_call(
                        successful_status_codes=successful_status_codes,
                        method_to_call=method_to_call,
                        url=url,
                        **request_kwargs,
                    )
                )
            )
        parsed_responses = await gather(*actions)
    return parsed_responses


async def __make_api_calls_for_urls_and_data_items(
        *,
        http_method: str,
        successful_status_codes: List[int],
        urls: List[str],
        data_items: List[Any],
        **request_kwargs: Any,
    ) -> ParsedResponses:
    """Helper function used to make asynchronous POST, PUT, PATCH requests"""
    if len(urls) != len(data_items):
        raise ValueError(
            "Expected `urls` and `data_items` to be of same length (as they must correspond to each other), but"
            f" got lengths ({len(urls)}, {len(data_items)}) respectively"
        )
    parsed_responses = []
    actions = []
    async with ClientSession() as session:
        method_to_call = __choose_session_method(
            http_method=http_method,
            session_obj=session,
        )
        for url, data_item in zip(urls, data_items):
            actions.append(
                ensure_future(
                    __make_api_call(
                        successful_status_codes=successful_status_codes,
                        method_to_call=method_to_call,
                        url=url,
                        data=data_item,
                        **request_kwargs,
                    )
                )
            )
        parsed_responses = await gather(*actions)
    return parsed_responses


def __async_to_sync(*, async_func: Callable, **kwargs: Any) -> Any:
    """Converts given asynchronous function to synchronous function, and returns it's output"""
    event_loop = get_event_loop()
    output = event_loop.run_until_complete(
        async_func(**kwargs)
    )
    # event_loop.close()
    return output


def get(
        *,
        successful_status_codes: List[int],
        urls: List[str],
        **request_kwargs: Any,
    ) -> ParsedResponses:
    """Makes asynchronous API calls (in bulk) for the GET method"""
    return __async_to_sync(
        async_func=__make_api_calls_for_urls,
        http_method=_HTTP_METHOD_NAME.GET,
        successful_status_codes=successful_status_codes,
        urls=urls,
        **request_kwargs,
    )


def post(
        *,
        successful_status_codes: List[int],
        urls: List[str],
        data_items: List[Any],
        **request_kwargs: Any,
    ) -> ParsedResponses:
    """Makes asynchronous API calls (in bulk) for the POST method"""
    return __async_to_sync(
        async_func=__make_api_calls_for_urls_and_data_items,
        http_method=_HTTP_METHOD_NAME.POST,
        successful_status_codes=successful_status_codes,
        urls=urls,
        data_items=data_items,
        **request_kwargs,
    )


def put(
        *,
        successful_status_codes: List[int],
        urls: List[str],
        data_items: List[Any],
        **request_kwargs: Any,
    ) -> ParsedResponses:
    """Makes asynchronous API calls (in bulk) for the PUT method"""
    return __async_to_sync(
        async_func=__make_api_calls_for_urls_and_data_items,
        http_method=_HTTP_METHOD_NAME.PUT,
        successful_status_codes=successful_status_codes,
        urls=urls,
        data_items=data_items,
        **request_kwargs,
    )


def patch(
        *,
        successful_status_codes: List[int],
        urls: List[str],
        data_items: List[Any],
        **request_kwargs: Any,
    ) -> ParsedResponses:
    """Makes asynchronous API calls (in bulk) for the PATCH method"""
    return __async_to_sync(
        async_func=__make_api_calls_for_urls_and_data_items,
        http_method=_HTTP_METHOD_NAME.PATCH,
        successful_status_codes=successful_status_codes,
        urls=urls,
        data_items=data_items,
        **request_kwargs,
    )


def delete(
        *,
        successful_status_codes: List[int],
        urls: List[str],
        **request_kwargs: Any,
    ) -> ParsedResponses:
    """Makes asynchronous API calls (in bulk) for the DELETE method"""
    return __async_to_sync(
        async_func=__make_api_calls_for_urls,
        http_method=_HTTP_METHOD_NAME.DELETE,
        successful_status_codes=successful_status_codes,
        urls=urls,
        **request_kwargs,
    )

