from airflow.sdk import dag, task, asset
from pendulum import datetime
import os

import pendulum
@asset(
    schedule='@daily',
    uri="/opt/airflow/dags/assets/data_extract.txt",
    name="fetch_data",
    description="Asset for fetching data"
)

def first_asset(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    with open(self.uri, 'w') as f:
        f.write("This is the data extracted on {}".format(pendulum.now().to_date_string()))

        print("Data has been extracted and stored in {}".format(self.uri))