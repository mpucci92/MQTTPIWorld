import pandas as pd
import numpy as np
import time
import os
from datetime import datetime
import paho.mqtt.publish as publish


def loadData(path, encoding, delimiter):
    # Loading data from CSV #
    df = pd.read_csv(path, encoding=encoding, delimiter=delimiter)
    return df

def cleanData(df):
    # Data cleansing - No Null Values #
    latitude_mean = df.Latitude.mean()
    longitude_mean = df.Longitude.mean()
    values = {"Latitude": latitude_mean, "Longitude": longitude_mean}
    df = df.fillna(value=values)
    df['Boost Pressure'] = df['Boost Pressure'].astype(float)
    df['Brake Air Pressure'] = df['Brake Air Pressure'].astype(float)
    df['Differential Temperature'] = df['Differential Temperature'].astype(float)
    df['Brake Stroke'] = df['Brake Stroke'].astype(float)

    return df

# Directory Settings #
directory = (os.path.dirname(os.path.realpath(__file__)))
data_directory = '\\Data\\'


if __name__ == "__main__":

    # Input Parameters #
    path_to_data = directory + data_directory + 'FleetDataOCS.csv'
    encoding = 'utf-8'
    delimiter = ';'
    topic_delimiter = "/"
    hostname_broker = "localhost"
    send_interval = 5

    # Loading and Cleaning Input Data #
    df = loadData(path_to_data, encoding, delimiter)
    df = cleanData(df)

    # Main Block - Creation of Topics and Sending of data to MQTT Broker #
    while True:
        list_of_trucks = df['Haul Truck Template'].value_counts().index.tolist()
        list_of_properties = list(df.columns[1:25])
        for asset in list_of_trucks:
            for prop in list_of_properties:
                topic = asset + topic_delimiter + prop

                if prop == 'TimeStamp':
                    value_to_push = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
                else:
                    value_to_push = np.random.choice(df[df['Haul Truck Template'] == asset][prop])
                try:
                    publish.single(topic, value_to_push, hostname=hostname_broker)
                except Exception as e:
                    print(prop)

        time.sleep(send_interval)