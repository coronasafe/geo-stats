import requests
import json
import datetime
import pandas as pd

def fetch_kerala_data():
    print("\n ==> fetch_kerala_data() is called")
    #url for the API request
    covid_data_url = "https://keralastats.coronasafe.live/latest.json"

    #Fetch the covid data from the API
    response = requests.get(covid_data_url)
    covid_data = response.json()   

    #read disctrict wise lat/long cordinates from KeralaCoordinates.json
    kerala_cordinates = ""
    with open("KeralaCoordinates.json", "r+") as file:
        kerala_cordinates = json.load(file)

    #read disctrict wise geo/json cordinates from KeralaCoordinates.json
    geojson_data = ""
    with open("Kerala_District.geojson", "r+") as file:
        geojson_data = json.load(file)

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
        json.dump(covid_data, file, sort_keys=True)

    print("\n===> Kerala data succefully fetched!!!")


def fetch_country_wise_data():
    print("\n ==> fetch_country_wise_data() is called")
    csv_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/06-12-2020.csv'
    df = pd.read_csv(csv_url)

    # clean the dataset (Removing FIPS, Admin2, Last_Update, Combined_Key, Province_State, Country_Region)
    # and adding 'State, Country' as a combined column
    df.drop(['FIPS', 'Admin2', 'Last_Update', 'Combined_Key'], axis=1, inplace=True)
    df['Province_State'] = df['Province_State'].fillna("")
    df.sort_values(['Country_Region', 'Province_State'], inplace=True)
    df['Province_State'] = df['Province_State'].map(lambda name: "" if(name == "") else  ", " + name)
    df['Country, State'] = df['Country_Region'] + df['Province_State']
    df.drop(['Province_State', 'Country_Region'], axis=1, inplace=True)
    #uncomment the below line if you wish to see the entire universe
    # pd.set_option("display.max_rows", None, "display.max_columns", None)

    # to combine all the unique 'Country, State' combination
    # grouped by mean of 'Lat', 'Long_', 'Incidence_Rate', 'Case-Fatality_Ratio'
    # grouped by sum of 'Confirmed', 'Deaths', 'Recovered', 'Active'
    lat_long_grp = df[['Lat', 'Long_', 'Incidence_Rate', 'Case-Fatality_Ratio']].groupby(df['Country, State'])
    rest_grp = df[['Confirmed', 'Deaths', 'Recovered', 'Active']].groupby(df['Country, State'])
    lat_long_col = lat_long_grp.mean()
    rest_all_col = rest_grp.sum()    
    result = pd.concat([lat_long_col, rest_all_col], axis=1, sort=False)
    
    # dump all this shit into a json and a csv file and call it a day
    print(" ==> Entire World Covid Data : ")
    print(result)    
    result.to_json('World_Covid_Data.json')
    result.to_csv('World_Covid_Data.csv')


    #TODO: conversion to json not implemented correctly yet
    # world_covid_data = ""
    # with open("World_Covid_Data.json", "r+") as file:
    #     world_covid_data = json.load(file)
    #     json.dump(world_covid_data, file, sort_keys=True)

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