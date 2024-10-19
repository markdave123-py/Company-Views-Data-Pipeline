import os
import urllib.request


def get_views():
    url = "https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240101-000000.gz"
    dir = "/opt/airflow/dags/core_sentiment/views"

    if not os.path.exists(dir):
        os.makedirs(dir)

    filename = os.path.join(dir,url.split("/")[-1])

    print(f"Downloading {filename}")

    urllib.request.urlretrieve(url, filename)

    print(f"Downloaded {filename}")
