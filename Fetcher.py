import requests
import json
import datetime
import pandas as pd

def fetch_kerala_data():
    print("\n ==> fetch_kerala_data() is called")
    #url for the API request
    covid_data_url = "https://keralastats.coronasafe.live/latest.json"
    kerala_geojson_url = "https://raw.githubusercontent.com/geohacker/kerala/master/geojsons/district.geojson"

    #Fetch the covid data from the API
    response = requests.get(covid_data_url)
    covid_data = response.json()

    #Fetch the Kerala geojson data from the API
    response = requests.get(kerala_geojson_url)
    geojson_data = response.json()

    #read disctrict wise lat/long cordinates from KeralaCoordinates.json
    kerala_cordinates = ""
    with open("KeralaCoordinates.json", "r+") as file:
        kerala_cordinates = json.load(file)

    #run through the covid data and append cordinate values to all disctricts
    for item in kerala_cordinates:
        district_covid_summary = covid_data['summary'].get(item['district'])
        district_cordinate_detail = {'latitude': item['latitude'], 'longitude': item['longitude']}
        district_covid_summary.update(district_cordinate_detail)
        #moving delta objects to inside summary of each district
        district_covid_delta = covid_data['delta'].get(item['district'])
        district_covid_summary.update({'delta' : district_covid_delta})

    #appending tag id for geo_json file
    covid_data.update({'geo_json_id': 'DISTRICT'})
    #delting delta object
    del covid_data['delta']
    #renaming summary to data
    covid_data_summary = covid_data['summary']
    covid_data.update({'data': covid_data_summary})
    del covid_data['summary']

    #add geojson featur for each district from geojson file
    for item in geojson_data['features']:
        district = item['properties']['DISTRICT']
        covid_data['data'][district].update({'geojson_feature': item})        

    #comment this line if you don't have the stomach to see the output you have birthed
    # print(" ==> Entire Kerala Covid Data : ")
    # print(json.dumps(covid_data, sort_keys=True, indent=4))

    #save into new json file
    with open("Kerala_Covid_Cordinate_Data.json", "w+") as file:
        json.dump(covid_data, file, sort_keys=True, indent=4)

    print("\n===> Kerala data succefully fetched!!!")

def fetch_country_wise_data():
    print("\n ==> fetch_country_wise_data() is called")
    csv_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/06-12-2020.csv'
    df = pd.read_csv(csv_url)
    df.to_json('World_Covid_Data.json')
    # print(df)

    #TODO: conversion to json not implemented correctly yet
    with open("World_Covid_Data.json", "r+") as file:
        world_covid_data = json.load(file)
        json.dump(world_covid_data, file, sort_keys=    True, indent=4)

    print("\n===> World covid data succefully fetched!!!")


##########################################################
## prog starts over here 
##########################################################
def main():
    start_time = datetime.datetime.now()

    # do all tasks here
    fetch_kerala_data()
    fetch_country_wise_data()

    print("It took {} seconds".format((datetime.datetime.now() - start_time)))
##########################################################


if __name__ == "__main__":
    main()