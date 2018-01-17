import numpy as np
import wfdb
import json
from Stream import Kafka
from time import time, sleep
from influxdb import InfluxDBClient
import datetime

topic = "ecgStream"  # kafka topic for sending messages
sampleRate = 360  # per second


path = "C:/Deep Learning/mitdb/"
file = path + '100'
points = 10000
kafka = Kafka.Kafka(topic)
data = {}

#It will take 325 seconds (5.4 min) for 65000 points to deliver at a rate of 200 sample per second.

# Read data from the MIT-BIH dataset
record = wfdb.rdsamp(file, channels=[0])
t = (np.arange(len(record.p_signals[0:points]))-1)/record.fs
signal_slice = np.ndarray.flatten(record.p_signals)

#client.create_database('example') # create database, if not exist

# Connect t InfluxDB database
client = InfluxDBClient('localhost', 8086, 'root', 'root', 'medit')

# Iterate over the data and stream it to Apache Kafka
for x in signal_slice:
    data['VALUE'] = x
    data['TIMESTAMP'] = str(datetime.datetime.utcnow())
    data['USER'] = 'Medit'
    data['SENSOR_ID'] = 'sensor12345ECG'
    data['SAMPLE_RATE'] = sampleRate
    sleep(1/sampleRate)  # Time in seconds
    kafka.sendMessage(str.encode(json.dumps(data)))
    json_body = [
        {
            "measurement": "ecg_orignal",
            "tags": {
                "user": data['USER'],
                "SENSOR_ID": data['SENSOR_ID']
            },
            "time": data['TIMESTAMP'],
            "fields": {
                "value": x
            }
        }
    ]
    # Write original signal to the InfluxDB
    client.write_points(json_body)

