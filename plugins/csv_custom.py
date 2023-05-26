import csv
def create_csv(**kwargs):
    site_name = kwargs['site_name']

    file = open(f'{site_name}.csv', 'w',newline='',encoding='utf-8-sig')
    writer = csv.writer(file)
    headers = ["title", "link", "description", "price"]
    writer.writerow(headers)
    return file

def write_csv(**kwargs): 
    site_name = kwargs['site_name']
    title = kwargs['title']
    description = kwargs['description']
    price = kwargs['price']

    file = open(f'{site_name}.csv', 'a', newline='', encoding='utf-8-sig')
    writer = csv.writer(file)
    contents = ([title, description, price])
    writer.writerow(contents)
    return file

def close_csv(**kwargs):
    file = kwargs['file']
    file.close()

