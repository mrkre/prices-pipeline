import requests

from abc import ABC, abstractmethod


class BaseAPIAdaptor(ABC):
    def __init__(self, base_url):
        self.base_url = base_url

    @abstractmethod
    def build_headers(self):
        """
        Method to be implemented by subclasses to construct headers for the API request.
        """
        pass

    @staticmethod
    def _handle_response(response):
        if response.status_code not in [200, 201]:
            raise Exception(
                f"Request failed with status {response.status_code}: {response.text}"
            )

        try:
            return response.json()
        except ValueError:
            raise Exception("Failed to decode JSON from API response.")

    def get(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        headers = self.build_headers()
        response = requests.get(url, params=params, headers=headers)
        return self._handle_response(response)

    def post(self, endpoint, payload=None):
        url = f"{self.base_url}/{endpoint}"
        headers = self.build_headers()
        response = requests.post(url, json=payload, headers=headers)
        return self._handle_response(response)

    def put(self, endpoint, payload=None):
        url = f"{self.base_url}/{endpoint}"
        headers = self.build_headers()
        response = requests.put(url, json=payload, headers=headers)
        return self._handle_response(response)

    def delete(self, endpoint):
        url = f"{self.base_url}/{endpoint}"
        headers = self.build_headers()
        response = requests.delete(url, headers=headers)
        return self._handle_response(response)
