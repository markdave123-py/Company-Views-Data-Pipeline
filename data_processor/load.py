import csv

allowed_companies = ("microsoft", "google", "amazon", "facebook", "apple")

base_dir = '/opt/airflow/dags/core_sentiment/'


def load():
    with open(f'{base_dir}/data_house/clean.csv', 'r') as csvfile, open(f'{base_dir}/data_house/load.sql', 'w') as sql:
        reader = csv.reader(csvfile, delimiter='<')

        sql.write('INSERT INTO companies_views (company, views, reference) VALUES\n')

        first_row = True

        for row in reader:
            reference = row[0].replace("'", "''")
            views = row[1]
            company = row[2].replace("'", "''")

            if not first_row:
                sql.write(',\n')
            first_row = False

            sql.write(f"('{company}', {views}, '{reference}')")

        sql.write(';\n')