from pendulum import Date

from .base_api_adaptor import BaseAPIAdaptor


class EODAPIClient(BaseAPIAdaptor):
    def __init__(self, api_key: str):
        self.api_key = api_key

        super().__init__(base_url="https://eodhistoricaldata.com/api")

    def build_headers(self):
        return {"Content-Type": "application/json"}

    def get_data(
        self,
        symbol: str,
        exchange: str,
        date_from: str,
        date_to: str,
    ):
        endpoint = f"/eod/{symbol}.{exchange}"

        params = {
            "api_token": self.api_key,
            "fmt": "json",
            "from": date_from,
            "to": date_to,
        }

        return self.get(endpoint=endpoint, params=params)

    def get_eod_bulk_last_day(self, exchange: str, date: str = None):
        endpoint = f"/eod-bulk-last-day/{exchange}"
        params = {"api_token": self.api_key, "fmt": "json"}

        if date:
            params["date"] = date

        return self.get(endpoint=endpoint, params=params)
