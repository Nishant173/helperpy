from asyncio import (
    ensure_future,
    gather,
    get_event_loop,
)
from typing import Any, Callable, Dict, List, Optional

from aiohttp import ClientSession


class HTTP_METHOD_NAME:
    """Exposes class variables having the various HTTP method names"""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


def __choose_session_method(
        http_method: str,
        session_obj: ClientSession,
    ) -> Callable:
    """Returns callable that is used to make the asynchronous API requests"""
    if http_method == HTTP_METHOD_NAME.GET:
        return session_obj.get
    elif http_method == HTTP_METHOD_NAME.POST:
        return session_obj.post
    elif http_method == HTTP_METHOD_NAME.PUT:
        return session_obj.put
    elif http_method == HTTP_METHOD_NAME.PATCH:
        return session_obj.patch
    elif http_method == HTTP_METHOD_NAME.DELETE:
        return session_obj.delete
    raise ValueError(f"Got an invalid `http_method` '{http_method}'")


async def __make_api_call(
        session: ClientSession,
        http_method: str,
        url: str,
        successful_status_codes: List[int],
        **request_kwargs,
    ) -> Dict[str, Any]:
    """Returns dictionary having keys: ['data', 'error']"""
    method_to_call = __choose_session_method(
        http_method=http_method,
        session_obj=session,
    )
    async with method_to_call(url, **request_kwargs) as response:
        if response.status in successful_status_codes:
            data = await response.json()
            error = None
        else:
            data = None
            error = {
                "url": url,
                "status_code": response.status,
                "text": await response.text(),
            }
    return {"data": data, "error": error}


async def __make_api_calls(
        http_method: str,
        urls: List[str],
        successful_status_codes: List[int],
        data_items: Optional[List[Any]] = None,
        **request_kwargs,
    ) -> List[Dict[str, Any]]:
    """Returns list of dictionaries having keys ['data', 'error'], for each URL (API endpoint) called"""
    if data_items and (http_method in [HTTP_METHOD_NAME.GET, HTTP_METHOD_NAME.DELETE]):
        raise ValueError(f"Cannot pass in `data_items` if `http_method` is set to '{http_method}'")
    if data_items and (len(data_items) != len(urls)):
        raise ValueError(
            "Length of `data_items` must be equal to the length of `urls`,"
            " as they must correspond to each other"
        )

    results = []
    actions = []
    if not data_items:
        async with ClientSession() as session:
            for url in urls:
                future_obj = ensure_future(
                    __make_api_call(
                        session=session,
                        http_method=http_method,
                        url=url,
                        successful_status_codes=successful_status_codes,
                        **request_kwargs,
                    )
                )
                actions.append(future_obj)
            results = await gather(*actions)
        return results

    async with ClientSession() as session:
        for url, data_item in zip(urls, data_items):
            future_obj = ensure_future(
                __make_api_call(
                    session=session,
                    http_method=http_method,
                    url=url,
                    successful_status_codes=successful_status_codes,
                    data=data_item,
                    **request_kwargs,
                )
            )
            actions.append(future_obj)
        results = await gather(*actions)
    return results


def make_api_calls(
        http_method: str,
        urls: List[str],
        successful_status_codes: List[int],
        data_items: Optional[List[Any]] = None,
        **request_kwargs,
    ) -> List[Dict[str, Any]]:
    """
    Returns list of dictionaries having keys ['data', 'error'], for each URL (API endpoint) called.

    Parameters:
        - http_method (str): HTTP method
        - urls (list): List of URLs (API endpoints) to call
        - successful_status_codes (list): List of status codes that are considered successful for the requests made
        - data_items (list): List of data items. Each item must correspond to the URLs in `urls` parameter
        (pass in `data_items` only for POST, PUT, PATCH methods)
        - request_kwargs: Kwargs related to the actual requests made i.e; headers.

    Docs: https://docs.aiohttp.org/en/stable/client_reference.html

    Usage:
    >>> num_api_calls = 1200
    >>> results = make_api_calls(
            http_method=HTTP_METHOD_NAME.GET,
            urls=[f"https://pokeapi.co/api/v2/pokemon/{number}" for number in range(1, num_api_calls+1)],
            successful_status_codes=[200],
            headers={},
        )
    >>> results = make_api_calls(
            http_method=HTTP_METHOD_NAME.POST,
            urls=["https://reqres.in/api/users"] * num_api_calls,
            successful_status_codes=[200, 201],
            data_items=[
                {
                    "name": f"MyName{number}",
                    "age": number,
                    "fav_movie": f"FavMovieName{number}",
                } for number in range(1, num_api_calls+1)
            ],
            headers={},
        )
    """
    event_loop = get_event_loop()
    results = event_loop.run_until_complete(
        __make_api_calls(
            http_method=http_method,
            urls=urls,
            successful_status_codes=successful_status_codes,
            data_items=data_items,
            **request_kwargs,
        )
    )
    # event_loop.close()
    return results