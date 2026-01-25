from copy import copy
from typing import TYPE_CHECKING, Any, override
from urllib.parse import urljoin

from requests import PreparedRequest, Request, Response
from requests import Session as BaseSession
from requests.adapters import HTTPAdapter

if TYPE_CHECKING:
    from urllib3 import Retry


class Session(BaseSession):
    """
    A _requests_ session with a base URL, timeout by default and retry mechanism.
    """

    def __init__(
        self,
        base_url: str | None = None,
        timeout: int | None = None,
        retry: Retry | None = None,
    ) -> None:
        super().__init__()

        self.base_url = base_url
        self.timeout = timeout

        # apply retry
        if retry is not None:
            for adapter in self.adapters.values():
                if isinstance(adapter, HTTPAdapter):
                    adapter.max_retries = retry

    @override
    def prepare_request(self, request: Request) -> PreparedRequest:
        modified_request = copy(request)

        # apply base_url
        if self.base_url is not None:
            modified_request.url = urljoin(self.base_url, request.url)

        return super().prepare_request(modified_request)

    @override
    def send(self, request: PreparedRequest, **kwargs: Any) -> Response:
        # apply timeout
        if self.timeout is not None and kwargs.get("timeout") is None:
            kwargs["timeout"] = self.timeout

        return super().send(request, **kwargs)
