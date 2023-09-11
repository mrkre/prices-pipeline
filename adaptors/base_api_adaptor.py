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

    def get(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        headers = self.build_headers()
        response = requests.get(url, params=params, headers=headers)
        return response.json()

    def post(self, endpoint, payload=None):
        url = f"{self.base_url}/{endpoint}"
        headers = self.build_headers()
        response = requests.post(url, json=payload, headers=headers)
        return response.json()

    def put(self, endpoint, payload=None):
        url = f"{self.base_url}/{endpoint}"
        headers = self.build_headers()
        response = requests.put(url, json=payload, headers=headers)
        return response.json()

    def delete(self, endpoint):
        url = f"{self.base_url}/{endpoint}"
        headers = self.build_headers()
        response = requests.delete(url, headers=headers)
        return response.json()
