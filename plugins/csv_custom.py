import csv

class CsvCustom:

    def __init__(self, site_name):
        self.site_name = site_name

    def create_csv(self):
        file = open(f'csv/{self.site_name}.csv', 'w',newline='',encoding='utf-8-sig')
        writer = csv.writer(file)
        headers = ["title", "link", "description", "price"]
        writer.writerow(headers)

    def write_csv(self,**kwargs): 
        title = kwargs['title']
        link = kwargs['link']
        description = kwargs['description']
        price = kwargs['price']

        file = open(f'csv/{self.site_name}.csv', 'a', newline='', encoding='utf-8-sig')
        writer = csv.writer(file)
        contents = ([title,link, description, price])
        writer.writerow(contents)
        return file

    def close_csv(self,file):
        file.close()