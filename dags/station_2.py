import json
import requests
import os    
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import BranchPythonOperator

default_args = {
    'owner': 'airflow',
}
# [END default_args]


# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['demo'])
def tutorial_taskflow_api_etl_test_3():
    @task()
    def extract():
        data = json.load(open('config.json'))
        f = open("allStation.json","w+")
        f.close();
        data  = sorted(data, key = lambda i: i['priority'])
        for i in data:
            print(i);
            provider = i['provider'];
            isActive = i['isActive'];
            print(provider)
            if provider == 'AAB' and isActive:
                getAirAsiaStation()
            elif provider == 'kiwi' and isActive:
                getKiwiStationData()
            else:
                print('unknown provider')
        return data;

    def getAirAsiaStation():
        r = requests.get('https://ppsch.apiairasia.com/station/en-gb/file.json',timeout=10);
        r = r.json()
        for data in r:
            data['provider'] = 'airasia';
        if os.path.getsize("allStation.json") > 0:
            merged_list = json.load(open("allStation.json"))
        else:
            merged_list = {};
        merged_list = combiner(merged_list,r)
        json_object = json.dumps(merged_list, indent = 4, sort_keys=True)  
        f = open("allStation.json", "w+")
        f.write(json_object);
        f.close();

    def getKiwiStationData():
        k = requests.get('https://api.skypicker.com/locations/airasia/dump?locale=en-GB');
        k = k.json()
        for data in k:
            data['provider'] = 'kiwi';
        if os.path.getsize("allStation.json") > 0:
            merged_list = json.load(open("allStation.json"))
        else:
            merged_list = {};
        merged_list = combiner(merged_list,k)
        json_object = json.dumps(merged_list, indent = 4, sort_keys=True)  
        f = open("allStation.json","w+")
        f.write(json_object);
        f.close();


    def combiner(merged_list, provider_list):
        for i in provider_list:
            if i["StationCode"] in merged_list:
                # v = merged_list[i["StationCode"]]
                # v['provider'] = v['provider'] + ' ## '+ i['provider'];
                # merged_list[i["StationCode"]] = v;
                pass
            else:
                merged_list[i["StationCode"]] = i;
        return merged_list;



    @task()
    def create_all_station_list():
        res_list = json.load(open("allStation.json"))
        list_of_stations = sorted(list(res_list.keys()));
        json_object = json.dumps(list_of_stations, indent = 4, sort_keys=True)  
        f = open("twitterData.txt", "w+")
        f.write(json_object)
        f.close();

    extract() >> create_all_station_list()


tutorial_etl_dag_test = tutorial_taskflow_api_etl_test_3()
