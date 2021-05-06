
# pylint: disable=missing-function-docstring

# [START tutorial]
# [START import_module]
import json
import requests
import os    
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator


# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
# [END default_args]


# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['demo'])
def tutorial_taskflow_api_etl_test_http():
    @task()
    def getAirAsiaStation():
        r = requests.get('https://ppsch.apiairasia.com/station/en-gb/file.json',timeout=10);
        r = r.json()
        # app.logger.info(r)
        for data in r:
            data['provider'] = 'airasia';
        f = open("allStation.json", "w+")
        if os.path.getsize("allStation.json") > 0:
            merged_list = json.load(f)
        else:
            merged_list = {};
        merged_list = combiner(merged_list,r)
        json_object = json.dumps(merged_list, indent = 4, sort_keys=True)  
        f.write(json_object);
        f.close();
        print(r);
        # return r;

    @task()
    def getKiwiStationData():
        k = requests.get('https://api.skypicker.com/locations/airasia/dump?locale=en-GB');
        k = k.json()
        # app.logger.info(r)
        for data in k:
            data['provider'] = 'kiwi';
        # return k;
        merged_list = json.load(open("allStation.json"))
        # merged_list = json.load(f)
        merged_list = combiner(merged_list,k)
        json_object = json.dumps(merged_list, indent = 4, sort_keys=True)  
        f = open("allStation.json","w+")
        f.write(json_object);
        f.close();
        print(k);


    def combiner(merged_list, provider_list):
        for i in provider_list:
            if i["StationCode"] in merged_list:
                v = merged_list[i["StationCode"]]
                v['provider'] = v['provider'] + ' ## '+ i['provider'];
                merged_list[i["StationCode"]] = v;
            else:
                merged_list[i["StationCode"]] = i;
        return merged_list;
    # [END extract]

    @task()
    def http_test():
        task_get_op_response_filter = SimpleHttpOperator(
            task_id='get_op_response_filter',
            method='GET',
            endpoint='https://ppsch.apiairasia.com/station/en-gb/file.json',
            response_filter=lambda response: response.json(),
            xcom_push=True,
            # dag=dag,
        )
        print(task_get_op_response_filter);


    # [END load]

    # [START main_flow]
    # order_data = extract()
    # order_summary = transform(order_data)
    # load(order_summary["total_order_value"])
    http_test()
    # getAirAsiaStation() >> getKiwiStationData() >> create_all_station_list()
    # res_list = {}
    # res_list = combiner(res_list,aa_list)
    # kk_list = getKiwiStationData()
    # res_list = combiner(res_list,kk_list)
    # t_aa_data=combiner()

    # [END main_flow]


# [START dag_invocation]
tutorial_etl_dag_test_api = tutorial_taskflow_api_etl_test_http()
# [END dag_invocation]

# [END tutorial]

