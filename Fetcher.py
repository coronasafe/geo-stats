import requests
import json

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
    district_covid_detail = covid_data['summary'].get(item['district'])
    district_cordinate_detail = {'latitude': item['latitude'], 'longitude': item['longitude']}
    district_covid_detail.update(district_cordinate_detail)

#appending tag id for geo_json file
covid_data.update({'geo_json_id': 'DISTRICT'})

#comment this line if you don't have the stomach to see the output you have birthed
print(json.dumps(covid_data, sort_keys=True, indent=4))

#save into new json file
with open("Kerala_Covid_Cordinate_Data.json", "w+") as file:
    json.dump(covid_data, file, sort_keys=True, indent=4)

print("\n\n===> Paarrttyyyyyy!!!")