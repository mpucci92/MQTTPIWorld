import pandas as pd
import numpy as np
import paho.mqtt.client as mqtt
from random import randrange, uniform
import time
import paho.mqtt.publish as publish


def loadData(path, encoding, delimiter):
    df = pd.read_csv(path, encoding=encoding, delimiter=delimiter)
    return df


path_to_data = r"C:\Users\mpucci\Desktop\Systems Engineer Work\PI World - OCS - MQTT\FleetDataOCS_20210822190600.csv"
encoding = 'latin-1'
delimiter = ';'

df = loadData(path_to_data, encoding, delimiter)

# Data cleansing - No Null Values
latitude_mean = df.Latitude.mean()
longitude_mean = df.Longitude.mean()
values = {"Latitude": latitude_mean, "Longitude": longitude_mean}
df = df.fillna(value=values)
df['Boost Pressure'] = df['Boost Pressure'].astype(float)
df['Brake Air Pressure'] = df['Brake Air Pressure'].astype(float)
df['Differential Temperature'] = df['Differential Temperature'].astype(float)
df['Brake Stroke'] = df['Brake Stroke'].astype(float)

while True:
    topic_delimiter = "/"
    list_of_trucks = df['Haul Truck Template'].value_counts().index.tolist()
    list_of_properties = list(df.columns[2:25])
    for asset in list_of_trucks:
        for prop in list_of_properties:
            topic = asset + topic_delimiter + prop
            value_to_push = np.random.choice(df[df['Haul Truck Template'] == asset][prop])
            try:
                # print(value_to_push)
                publish.single(topic, value_to_push, hostname="169.254.205.27")
            except Exception as e:
                print(prop)

    time.sleep(60)
