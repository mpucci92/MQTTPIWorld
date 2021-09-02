import pandas as pd
import numpy as np
import time
import os
from datetime import datetime
import paho.mqtt.publish as publish

def loadData(path, encoding, delimiter):
    df = pd.read_csv(path, encoding=encoding, delimiter=delimiter)
    return df

def cleanData(df):
    # Data cleansing - No Null Values #
    latitude_mean = df.Latitude.mean()
    longitude_mean = df.Longitude.mean()
    values = {"Latitude": latitude_mean, "Longitude": longitude_mean}
    df = df.fillna(value=values)
    df['Brake Air Pressure'] = df['Brake Air Pressure'].astype(float)

    return df

def renameColumns(df):
    df = df.rename(columns={'Aftercooler Temperature': 'Aftercooler_Temperature',
                            'Brake Air Pressure': 'Brake_Air_Pressure',
                            'Engine Coolant Temperature': 'Engine_Coolant_Temperature',
                            'Engine Fuel Rate': 'Engine_Fuel_Rate',
                            'Engine Load': 'Engine_Load',
                            'Engine Oil Pressure': 'Engine_Oil_Pressure',
                            'Engine RPM': 'Engine_RPM',
                            'Ground Speed': 'Ground_Speed'}, errors="raise")
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
    topic_constant = 'fleet'
    hostname_broker = "localhost"
    send_interval = 2

    columns_to_keep = ['Haul Truck Template', 'Aftercooler Temperature',
                       'Brake Air Pressure', 'Engine Coolant Temperature', 'Engine Fuel Rate', 'Engine Load',
                       'Engine Oil Pressure', 'Engine RPM', 'Ground Speed', 'Longitude', 'Latitude']

    truck_status = ['Running', 'Running', 'Running', 'Running', 'Running', 'Running',
                    'Running', 'Under Maintenance', 'Under Maintenance', 'Stopped']

    df = loadData(path_to_data, encoding, delimiter)
    df = df.loc[:, columns_to_keep]
    df = cleanData(df)
    df = renameColumns(df)

    while True:
        list_of_trucks = df['Haul Truck Template'].value_counts().index.tolist()
        list_of_properties = list(df.columns)[1:]

        for asset in list_of_trucks:  # truck01
            dictionary = {}
            for prop in list_of_properties:
                topic = topic_constant + topic_delimiter + asset
                dictionary[prop] = np.random.choice(df[df['Haul Truck Template'] == asset][prop])
                dictionary['Status'] = np.random.choice(truck_status)
            try:
                publish.single(topic, str(dictionary), hostname=hostname_broker)
            except Exception as e:
                print(topic)

            time.sleep(send_interval)
