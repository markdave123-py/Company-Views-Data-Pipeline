import os
import urllib.request

page = 'pageviews-20240601-120000.gz'



period = '2024/2024-06'

url = f'https://dumps.wikimedia.org/other/pageviews'

def getpage():
    return page

def unzipedPage():
    return page.split('.')[0]

def getpageUrl():
    return f'{url}/{period}/{page}'

def get_views():
    url = getpageUrl()
    dir = "/opt/airflow/dags/core_sentiment/views"

    if not os.path.exists(dir):
        os.makedirs(dir)

    filename = os.path.join(dir,url.split("/")[-1])

    print(f"Downloading {filename}")

    urllib.request.urlretrieve(url, filename)

    print(f"Downloaded {filename}")
