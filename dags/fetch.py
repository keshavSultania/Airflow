from flask import json
import requests
import os
from flask import current_app as app
from flask import jsonify
from operator import itemgetter

def getAirAsiaStation():
    r = requests.get('https://ppsch.apiairasia.com/station/en-gb/file.json',timeout=10);
    r = r.json()
    # app.logger.info(r)
    for data in r:
        data['provider'] = 'airasia';
    return r;

def getKiwiStationData():
    k = requests.get('https://locations.locations-dev.skypicker.com/locations/airasia/dump?locale=en-GB',timeout=10);
    k = k.json()
    # app.logger.info(r)
    for data in k:
        data['provider'] = 'kiwi';
    return k;

def getTravelPortData():
    SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
    json_url = os.path.join(SITE_ROOT, "../sample", "travelport.json")
    k = json.load(open(json_url))
    for data in k:
            data['provider'] = 'travelport';
    # app.logger.info(k)
    return k;
    # return data;

def getConfig():
    SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
    json_url = os.path.join(SITE_ROOT, "../config", "config2.json")
    data = json.load(open(json_url))
    return data;

def combiner(merged_list, provider_list):
    for i in provider_list:
        if i["StationCode"] in merged_list:
            v = merged_list[i["StationCode"]]
            v['provider'] = v['provider'] + ' ## '+ i['provider'];
            merged_list[i["StationCode"]] = v;
        else:
            merged_list[i["StationCode"]] = i;

def getData():
    # config = getConfig();
    res_list = {}

    a = getAirAsiaStation();
    combiner(merged_list=res_list, provider_list=a);

    k = getKiwiStationData();
    combiner(merged_list=res_list, provider_list=k);

    t = getTravelPortData();
    combiner(merged_list=res_list, provider_list=t);

    list_of_stations = sorted(list(res_list.keys()));
    # app.logger.info(list_of_stations);

    json_object = json.dumps(list_of_stations, indent = 4, sort_keys=True)  
    f = open("twitterData.txt", "w+")
    for ele in list_of_stations:
        f.write(ele+'\n')
    # f.write(json.dumps(list_of_stations), indent=4, sort_keys=True);
    f.close();


    f = open("allStation.json", "w+")
    # for ele in list_of_stations:
    #     f.write(ele+'\n')
    f.write(json_object);
    f.close();

    return jsonify(list_of_stations);
    return res_list;

    l = list(res_list.values())
    l = sorted(l, key=itemgetter('StationCode'))
    return jsonify(l);
    
    # merged_dict = {key: value for (key, value) in (k.items() + r.items())}
    # jsonString_merged = json.dumps(merged_dict)

    # return jsonString_merged;
    l = a + k;
    # l = sorted(l, key = lambda i: (i['StationCode']))
    l = sorted(l, key=itemgetter('StationCode'))
    # l = list(OrderedDict.fromkeys(l))
    # print sorted(lis, key=itemgetter('age'))
    # res_list = []
    # for i in range(len(l)):
    #     if l[i] not in test_list[i + 1:]:
    #         res_list.append(l[i])

    return jsonify(l);

    # return r[0].json()
    return jsonify(k);