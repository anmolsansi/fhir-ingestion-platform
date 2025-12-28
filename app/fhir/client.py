from __future__ import annotations

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.core.config import REQUEST_TIMEOUT_SECS


class FHIRClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECS)
        resp.raise_for_status()
        return resp.json()

    def search_all(self, resource_type: str, params: dict) -> list[dict]:
        """
        FHIR search returns a Bundle. We follow Bundle.link[next] until done.
        """
        items: list[dict] = []
        bundle = self.get(f"/{resource_type}", params=params)

        while True:
            for entry in bundle.get("entry", []) or []:
                res = entry.get("resource")
                if res:
                    items.append(res)

            next_url = None
            for link in bundle.get("link", []) or []:
                if link.get("relation") == "next":
                    next_url = link.get("url")
                    break

            if not next_url:
                break

            # next_url is absolute; call it directly
            resp = requests.get(next_url, timeout=REQUEST_TIMEOUT_SECS)
            resp.raise_for_status()
            bundle = resp.json()

        return items
