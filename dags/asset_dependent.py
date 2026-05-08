from airflow.sdk import dag, task, asset
import pendulum
from asset_main import first_asset

@asset(
    schedule=first_asset,
    uri="/opt/airflow/dags/assets/data_processed.txt",
    name="processed_data",
    description="Asset for processing data"

)

def processed_asset(self):
    with open(first_asset.uri, 'r') as f:
        data = f.read()
        print("Data read from the first asset: {}".format(data))
    
    processed_data = data + " and processed on {}".format(pendulum.now().to_date_string())
    
    with open(self.uri, 'w') as f:
        f.write(processed_data)
        print("Data has been processed and stored in {}".format(self.uri))