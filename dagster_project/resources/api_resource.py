import requests
from dagster import ConfigurableResource, EnvVar
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RestCountriesApiResource(ConfigurableResource):
    """HTTP client for the REST Countries API with retry, timeout and auth config."""

    base_url: str = EnvVar("API_BASE_URL")
    token: str = EnvVar("API_TOKEN")
    timeout_seconds: int = EnvVar.int("API_TIMEOUT_SECONDS")
    max_retries: int = EnvVar.int("API_MAX_RETRIES")

    def _session(self) -> requests.Session:
        session = requests.Session()
        if self.token:
            session.headers["Authorization"] = f"Bearer {self.token}"
        retry = Retry(
            total=self.max_retries,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_all_countries(self) -> list[dict]:
        """Fetch every country, paging through the API's `data.objects` /
        `data.meta` pagination until all pages have been retrieved."""
        session = self._session()
        countries: list[dict] = []
        offset = 0
        limit = 100  # API-enforced cap on this plan; larger values 403

        while True:
            response = session.get(
                self.base_url,
                params={"limit": limit, "offset": offset},
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            page = response.json()["data"]
            countries.extend(page["objects"])

            if not page["meta"]["more"]:
                break
            offset += limit

        return countries
