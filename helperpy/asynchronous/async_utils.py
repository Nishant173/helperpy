import asyncio
from typing import Any, Dict, List, Optional

import aiohttp


class AsyncApiCaller:
    """
    Class used to communicate with multiple API endpoints asynchronously (for faster response times).

    Documentation references (aiohttp):
        - https://docs.aiohttp.org/en/stable/client_reference.html

    Usage:
    >>> aac = AsyncApiCaller(headers={}, successful_status_codes=[200])
    >>> data = aac.get_data(
            urls=[f"https://pokeapi.co/api/v2/pokemon/{number}" for number in range(1, 500+1)],
        )
    >>> aac.get_errors() # Returns list of errors (if any)
    """

    def __init__(
            self,
            headers: Optional[Dict[str, Any]] = None,
            successful_status_codes: Optional[List[int]] = None,
        ) -> None:
        """
        Parameters:
            - headers (dict): Headers for the API calls. Default: {}
            - successful_status_codes (list): List of successful status codes for the API calls. Default: [200, 204]
        """
        self.headers = headers if headers else {}
        self.successful_status_codes = successful_status_codes if successful_status_codes else [200, 204]
        self.__errors = []

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(headers={self.headers}, successful_status_codes={self.successful_status_codes})"

    def get_errors(self):
        return self.__errors

    async def __make_api_call(
            self,
            session: aiohttp.ClientSession,
            url: str,
        ) -> Any:
        async with session.get(url, headers=self.headers) as response:
            data = await response.json()
            if response.status not in self.successful_status_codes:
                data = None
                self.__errors.append({
                    "url": url,
                    "status_code": response.status,
                    "text": await response.text(),
                })
        return data

    async def __make_api_calls(
            self,
            urls: List[str],
        ) -> List[Any]:
        results = []
        actions = []
        async with aiohttp.ClientSession() as session:
            for url in urls:
                actions.append(
                    asyncio.ensure_future(
                        self.__make_api_call(
                            session=session,
                            url=url,
                        )
                    )
                )
            results = await asyncio.gather(*actions)
        return results

    def get_data(
            self,
            urls: List[str],
        ) -> List[Any]:
        """
        Returns list where each item contains the data of each URL (API endpoint) called.
        """
        event_loop = asyncio.get_event_loop()
        data = event_loop.run_until_complete(
            self.__make_api_calls(urls=urls)
        )
        event_loop.close()
        return data