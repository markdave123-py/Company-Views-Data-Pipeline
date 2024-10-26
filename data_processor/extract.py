import csv
from core_sentiment.helper.getveiws import unzipedPage

allowed_companies = ("microsoft", "google", "amazon", "facebook", "apple")

base_dir = '/opt/airflow/dags/core_sentiment/'



def extract():
    page = unzipedPage()
    with open(f'{base_dir}views/{page}') as file:
        for line in file:
            if len(line.split(' ')) < 3:
                continue
            company = line.split(' ')[1].lower()
            views =  line.split(' ')[2]

            company_name = next((comp for comp in allowed_companies if comp in company), None)

            if company_name:
                with open(f'{base_dir}data_house/clean.csv', 'a') as f:
                    f.write(company + '<' + views + '<' + company_name + '\n')