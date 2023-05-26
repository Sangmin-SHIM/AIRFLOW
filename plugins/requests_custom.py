import requests
from bs4 import BeautifulSoup

class RequestCustom:
    def __init__(self, url, site_name):
        self.url = url
        self.site_name = site_name

    def get_url(self):
        return self.url

    def get_request(self):
        url=self.get_url()     
        return requests.get(url, allow_redirects=False)

    def get_status(self):
        request = self.get_request()
        status = request.status_code
        return status

    def get_soup(self):
        request=self.get_request()
        status = self.get_status()
        if status == 200:
            soup = BeautifulSoup(request.text, 'html.parser')
            return soup
        else:
            print("Error: ", status)
            return None