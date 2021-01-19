import http.client
import urllib
import pandas as pd
import requests
import json
from urllib import request
from urllib import parse

DATABASE_LOCATION = "sqlite:///nearest_cities.sqlite"
TOKEN = "QE9XT42YJSLYRFZXMUO5TBKMNBL1YVTO"

if __name__ == "__main__":
    headers = {"Accept" : "application/json","Content-Type" : "application/json","Authorization" : "Bearer {token}".format(token=TOKEN)}

    lat = "-18.126221"
    lng = "178.448398"

    r = requests.get("https://api.geodatasource.com/cities?key={token}&lat={lat}&lng={lng}".format(token=TOKEN, lat=lat, lng= lng), headers = headers)

    data = r.json()

    country = []
    city = []
    latitude = []
    longitude = []
    sunrise = []
    sunset = []
    distance_km = []

    for item in data:
        country.append(item["country"])
        city.append(item["city"])
        latitude.append(item["latitude"])
        longitude.append(item["longitude"])
        sunrise.append(item["sunrise"])
        sunset.append(item["sunset"])
        distance_km.append(item["distance_km"])


    item_dict = {
        "country" : country,
        "city" : city,
        "latitude" : latitude,
        "longitude" : longitude,
        "sunrise" : sunrise,
        "sunset" : sunset,
        "distance_km" : distance_km
    }

    item_df = pd.DataFrame(item_dict, columns = ["country", "city", "latitude", "longitude", "sunrise", "sunset", "distance_km"])


    print(item_df)
