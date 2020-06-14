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
    with open("geo.json/kerala_cordinates.json", "r+") as file:
        kerala_cordinates = json.load(file)

    #read disctrict wise geo/json cordinates from Kerala_District.geojson
    geojson_data = ""
    with open("geo.json/kerala_district.geo.json", "r+") as file:
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
    with open("covid_data_json/kerala_covid_data.json", "w") as file:
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

    # uncomment the below line if you wish to see the entire universe
    # pd.set_option("display.max_rows", None, "display.max_columns", None)

    # to combine all the unique 'Country, State' combination
    # grouped by mean of 'Lat', 'Long_', 'Incidence_Rate', 'Case-Fatality_Ratio'
    # grouped by sum of 'Confirmed', 'Deaths', 'Recovered', 'Active'
    lat_long_grp = df[['Lat', 'Long_', 'Incidence_Rate', 'Case-Fatality_Ratio']].groupby(df['Country, State'])
    rest_grp = df[['Confirmed', 'Deaths', 'Recovered', 'Active']].groupby(df['Country, State'])
    lat_long_col = lat_long_grp.mean()
    rest_all_col = rest_grp.sum()    
    result = pd.concat([lat_long_col, rest_all_col], axis=1, sort=False)
    result.reset_index(inplace=True)
    result['Country'] = result['Country, State'].map(lambda name: name.split(', ')[0])
     
    # dump all this shit into a json and a csv file and call it a day
    # print(" ==> Entire World Covid Data : ")
    # print(result)    
    result.to_json('covid_data_json/world_covid_data.json')
    result.to_csv('covid_data_csv/world_covid_data.csv', index = False)


    #TODO: conversion to json not implemented correctly yet
    world_covid_data = ""
    with open("covid_data_json/world_covid_data.json", "r+") as file:
        world_covid_data = json.load(file)
        world_covid_data.update({'geo_json_id': 'name'})
        json.dump(world_covid_data, file, sort_keys=True)

    print("\n===> World covid data succefully fetched!!!")


def separate_india_data():
    print("\n ==> separate_india_data() is called")

    world_covid_data = pd.read_csv("World_Covid_Data.csv")
    india_data = world_covid_data[world_covid_data["Country"] == "India"]
    india_data['State'] = india_data['Country, State'].map(lambda x: x.split(',')[1].strip())
    india_data.drop(['Country', 'Country, State'], axis=1, inplace=True)

    columns = list(india_data)
    # move the column to head of list using index, pop and insert
    columns.insert(0, columns.pop(columns.index('State')))
    india_data = india_data.loc[:, columns]
    # print(india_data) 

    india_geojson = ""
    with open("geo.json/india_district.geo.json", "r+") as file:
        india_geojson = json.load(file)

    add_geo_json_feature(india_data, india_geojson, 'ST_NM', 'india_covid_data')     
    print("\n===> India covid data was succefully created!!!")


def separate_usa_data():
    print("\n ==> separate_usa_data() is called")

    world_covid_data = pd.read_csv("World_Covid_Data.csv")
    us_data = world_covid_data[world_covid_data["Country"] == "US"]
    us_data['State'] = us_data['Country, State'].map(lambda x: x.split(',')[1].strip())
    us_data.drop(['Country', 'Country, State'], axis=1, inplace=True)

    columns = list(us_data)
    # move the column to head of list using index, pop and insert
    columns.insert(0, columns.pop(columns.index('State')))
    # use ix to reorder
    us_data = us_data.loc[:, columns]

    usa_geojson = ""
    with open("geo.json/usa_state_provinces.geo.json", "r+") as file:
        usa_geojson = json.load(file)

    add_geo_json_feature(us_data, usa_geojson, 'name', 'usa_covid_data')  
    print("\n===> USA covid data was succefully created!!!")


def separate_others_data():
    print("\n===> separate_others_data() was called")
    world_covid_data = pd.read_csv("World_Covid_Data.csv")
    world_covid_data['Country'] = world_covid_data['Country, State'].map(lambda x: x.split(',')[0].strip())
    lat_long_grp = world_covid_data[['Lat', 'Long_', 'Incidence_Rate', 'Case-Fatality_Ratio']].groupby(world_covid_data['Country'])
    rest_grp = world_covid_data[['Confirmed', 'Deaths', 'Recovered', 'Active']].groupby(world_covid_data['Country'])
    lat_long_col = lat_long_grp.mean()
    rest_all_col = rest_grp.sum()    
    result = pd.concat([lat_long_col, rest_all_col], axis=1, sort=False)
    result.reset_index(inplace=True)
    # print(result)

    country_geojson = ""
    with open("geo.json/countries.geo.json", "r+") as file:
        country_geojson = json.load(file)

    add_geo_json_feature(result, country_geojson, 'name', 'other_countries_covid_data')   
    print("\n===> Rest of the countries' covid data was succefully created!!!")


def add_geo_json_feature(covid_df, geo_json_data, feature_id, json_output_name):   
    print("\n===> add_geo_json_feature() -> feature_id = " + feature_id + "json_output_name = " + json_output_name)
    
    covid_df['geo_json_feature'] = covid_df.iloc[:, 0]
    for item in geo_json_data['features']:   
        cordinate_feature = str(item)     
        covid_df.loc[covid_df['geo_json_feature'] == item['properties'][feature_id], 'geo_json_feature'] = cordinate_feature

    # to set regions that have no geo.json feature to null 
    covid_df['geo_json_feature'] = covid_df['geo_json_feature'].map(lambda x: x if 'properties' in x else "null")
    
    # comment this line if you wish to see all the end product json files
    # print(covid_df)

    # dump all data in csv and json then run for your life 
    covid_df.to_csv('covid_data_csv/' + json_output_name + '.csv', index = False)
    with open("covid_data_json/" + json_output_name + ".json", "w+") as file:
        json.dump(covid_df.to_json(), file, sort_keys=True)

    print("\n===> add_geo_json_feature was succefully!!!")


##########################################################
## prog starts over here 
##########################################################
def main():
    start_time = datetime.datetime.now()

    # do all tasks here
    fetch_kerala_data()
    fetch_country_wise_data()
    separate_india_data()
    separate_usa_data()
    separate_others_data()

    print("It took {} seconds".format((datetime.datetime.now() - start_time)))
##########################################################


if __name__ == "__main__":
    main()