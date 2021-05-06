# pylint: disable=missing-function-docstring

# [START tutorial]
# [START import_module]
import json
import requests
import os    
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import BranchPythonOperator


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
def tutorial_taskflow_api_etl_test_2_station():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    # [END instantiate_dag]

    # [START extract]
    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        data = json.load(open('config.json'))
        f = open("allStation.json","w+")
        f.close();
        # print(sorted(data, key = lambda i: i['priority']))
        # print(data);
        for i in data:
            print(i);
            provider = i['provider'];
            isActive = i['isActive'];
            print(isActive)
            print(provider)
            if provider == 'AAB' and isActive is True :
                getAirAsiaStation()
            elif provider == 'kiwi' and isActive is True :
                getKiwiStationData()
            else:
                print('unknown provider')
        return data;
        # data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        # order_data_dict = json.loads(data_string)
        # return order_data_dict

    # @task()
    def getAirAsiaStation():
        r = requests.get('https://ppsch.apiairasia.com/station/en-gb/file.json',timeout=10);
        r = r.json()
        # app.logger.info(r)
        for data in r:
            data['provider'] = 'airasia';
        # f = open("allStation.json", "w+")
        if os.path.getsize("allStation.json") > 0:
            merged_list = json.load(open("allStation.json"))
        else:
            merged_list = {};
        merged_list = combiner(merged_list,r)
        json_object = json.dumps(merged_list, indent = 4, sort_keys=True)  
        f = open("allStation.json", "w+")
        f.write(json_object);
        f.close();
        print(r);
        # return r;

    # @task()
    def getKiwiStationData():
        k = requests.get('https://api.skypicker.com/locations/airasia/dump?locale=en-GB');
        k = k.json()
        # app.logger.info(r)
        for data in k:
            data['provider'] = 'kiwi';
        # return k;
        # f = open("allStation.json", "w+")
        if os.path.getsize("allStation.json") > 0:
            merged_list = json.load(open("allStation.json"))
        else:
            merged_list = {};
        # merged_list = json.load(open("allStation.json"))
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
                # v = merged_list[i["StationCode"]]
                # v['provider'] = v['provider'] + ' ## '+ i['provider'];
                # merged_list[i["StationCode"]] = v;
                pass
            else:
                merged_list[i["StationCode"]] = i;
        return merged_list;
    # [END extract]

    # [START transform]
    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def create_all_station_list():
        res_list = json.load(open("allStation.json"))
        list_of_stations = sorted(list(res_list.keys()));
        json_object = json.dumps(list_of_stations, indent = 4, sort_keys=True)  
        f = open("twitterData.txt", "w+")
        f.write(json_object)
        # f.write(json.dumps(list_of_stations), indent=4, sort_keys=True);
        f.close();
        # [END transform]

    # [START load]
    @task()
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print("Total order value is: %.2f" % total_order_value)

    # [END load]

    # @task()
    # def branching_example():
    #     branch_op = BranchPythonOperator(
    #     task_id='branch_task',
    #     python_callable=branch_func,
    #     dag=dag)

    # [START main_flow]
    # order_data = extract()
    # order_summary = transform(order_data)
    # load(order_summary["total_order_value"])
    extract() >> create_all_station_list()
    # extract() >> getAirAsiaStation() >> getKiwiStationData() >> create_all_station_list()
    # res_list = {}
    # res_list = combiner(res_list,aa_list)
    # kk_list = getKiwiStationData()
    # res_list = combiner(res_list,kk_list)
    # t_aa_data=combiner()

    # [END main_flow]


# [START dag_invocation]
tutorial_etl_dag_test = tutorial_taskflow_api_etl_test_2_station()
# [END dag_invocation]

# [END tutorial]