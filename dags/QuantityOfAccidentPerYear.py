from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd
import io
import requests


@dag(schedule_interval=None, start_date=days_ago(2), catchup=False, tags=['taskflow'])
def count_accidents_from_csv_file():
    @task(multiple_outputs=True)
    def download_excel_csv():
        url = "http://data.bloomington.in.gov/dataset/117733fb-31cb-480a-8b30-fbf425a690cd/resource/8673744e-53f2-" \
              "42d1-9d05-4e412bd55c94/download/monroe-county-crash-data2003-to-2015.csv"
        excel_file = requests.get(url)
        data = excel_file.content.decode(excel_file.encoding)
        ready_panda_dataframe = pd.read_csv(io.StringIO(data))
        ready_panda_dataframe = ready_panda_dataframe.fillna(0)
        ready_panda_dataframe = ready_panda_dataframe.to_dict()
        return ready_panda_dataframe

    @task()
    def count_stuff(pandas_data_frame):
        pandas_data_frame = pd.DataFrame.from_dict(pandas_data_frame)
        container = pandas_data_frame['Year'].value_counts()
        container = container.to_dict()
        return container

    @task()
    def print_result(quantity_of_accidents):
        print("Quantity of accidents per year:\n")
        print(quantity_of_accidents)

    downloaded_sheet = download_excel_csv()
    counted_stuff = count_stuff(downloaded_sheet)
    print_result(counted_stuff)


lets_do_this_thing = count_accidents_from_csv_file()
