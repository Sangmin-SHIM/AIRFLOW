import requests
from bs4 import BeautifulSoup

class Request:
    def __init__(self, url, params=None):
        self.url = url
        self.params = params

    def get_url(self):
        if self.params is None: 
            return self.url
        url=str(self.url)+'/'+str(self.params)
        return url
    
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