from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import numpy as np
import scipy.signal as ss
from influxdb import InfluxDBClient
from collections import deque

# For storing data in InfluxDB, which is visualized in Grafana
client = InfluxDBClient('localhost', 8086, 'root', 'root', 'medit')

# queue for holding signal values
xs = deque(maxlen=500)


def reduce(a, b):
    if isinstance(a, list):
        if isinstance(b, list):
            # a and b both are in list
            return a + b
        else:
            # a is in list, but b is not
            a.append(b)
            return a
    else:
        print('1st time')
        x = []
        x.append(a)
        x.append(b)
        a = x
        return a

def invReduce(a, b):
    pass

# A bandpass filter for cleaning the ECG signal
def filterSignal(signal):
    lowfreq = 0.5
    highfreq = 40
    rate = 360

    lowpass = ss.butter(1, highfreq / (rate / 2.0), 'low')
    highpass = ss.butter(1, lowfreq / (rate / 2.0), 'high')

    ecg_low = ss.filtfilt(*lowpass, x=signal)
    ecg_band = ss.filtfilt(*highpass, x=ecg_low)

    return ecg_band

# Main loop for processing the real-time stream
def process(a):
    if not a or a is None:
        return
    a = a.collect()
    if len(a) == 0:
        return
    a = a[0]
    a = np.array(a)
    a = a.ravel()

    # Filter works for minimum 6 values in an array
    if len(a) < 6:
        return
    signal = a
    values = [json.loads(json.dumps(element))['VALUE'] for element in signal]

    filtered_ecg = filterSignal(values)

    # Create a JSON array to store data in InfluxDB
    for x in range(0, len(filtered_ecg)):
        json_body = [
            {
                "measurement": "ecg_Filtered",
                "tags": {
                    "user": signal[x]['USER'],
                    "SENSOR_ID": signal[x]['SENSOR_ID']
                },
                "time": signal[x]['TIMESTAMP'],
                "fields": {
                    "value": filtered_ecg[x]
                }
            }
        ]

        # Write data to InfluxDB
        client.write_points(json_body)



sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount", master="local[7]")
ssc = StreamingContext(sc, 1)
ssc.checkpoint('C:/tmp/spark_temp')
brokers = 'localhost:9092'
topic = 'ecgStream'
stream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
ecgValues = stream.map(lambda x: json.loads(x[1]))

es = ecgValues.reduceByWindow(reduce, invReduce, 4, 2)
es2 = es.transform(process)
es2.pprint()
ssc.start()
ssc.awaitTermination()



