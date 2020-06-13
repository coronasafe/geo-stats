import requests
import json
import datetime

def fetch_kerala_data():
    #url for the API request
    url = "https://keralastats.coronasafe.live/latest.json"

    #Fetch the data from the API
    response = requests.get(url)
    covid_data = response.json()

    #read disctrict wise lat/long cordinates from KeralaCoordinates.json
    kerala_cordinates = ""
    with open("KeralaCoordinates.json", "r+") as file:
        kerala_cordinates = json.load(file)

    #run through the covid data and append cordinate values to all disctricts
    for item in kerala_cordinates:
        district_covid_summary = covid_data['summary'].get(item['district'])
        district_covid_delta = covid_data['delta'].get(item['district'])
        district_cordinate_detail = {'latitude': item['latitude'], 'longitude': item['longitude']}
        district_covid_summary.update(district_cordinate_detail)
        district_covid_summary.update({'delta' : district_covid_delta})

    #appending tag id for geo_json file
    del covid_data['delta']
    covid_data_summary = covid_data['summary']
    del covid_data['summary']
    covid_data.update({'data': covid_data_summary})
    covid_data.update({'geo_json_id': 'DISTRICT'})

    #comment this line if you don't have the stomach to see the output you have birthed
    # print(json.dumps(covid_data, sort_keys=True, indent=4))

    #save into new json file
    with open("Kerala_Covid_Cordinate_Data.json", "w+") as file:
        json.dump(covid_data, file, sort_keys=True, indent=4)

    print("\n\n===> Kerala Paarrttyyyyyy!!!")



##########################################################
## prog starts over here 
##########################################################
# start_time = datetime.datetime.now()
fetch_kerala_data()

# end_time = datetime.datetime.now()
# time_taken = end_time - start_time
# time_taken = round((time_taken.seconds/60), 2)
# print("It took {} seconds".format(time_taken))
