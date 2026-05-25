from urllib.request import urlopen
import json
import time
from datetime import datetime

import urllib
import pandas as pd

from kafka import KafkaProducer
from kafka.errors import KafkaError



#RACE DETAILS DATA

#retrieving session
filters={
    "year":2024,
    "country_name":"Japan",
    "session_name":"Race"
}
query_string = urllib.parse.urlencode(filters)
url=f'https://api.openf1.org/v1/sessions?{query_string}'
responce=urlopen(url)
session_data=json.loads(responce.read().decode('utf-8'))
session_data_frame=pd.DataFrame(session_data)
# print(session_data)


#retrieve teams and drivers participating in the session
session_key=session_data[0]['session_key']
url=f'https://api.openf1.org/v1/drivers?session_key={session_key}'
response=urlopen(url)
drivers_details=json.loads(response.read().decode('utf-8'))
drivers_data_frame=pd.DataFrame(drivers_details)
# print(drivers_details)


#DATA FOR SIMULATING THE SESSION

#retrieving session telemetry
url=f'https://api.openf1.org/v1/location?session_key={session_key}&driver_number=1' #for testing , I only have included driver number 1
response=urlopen(url)
telemetry_data=json.loads(response.read().decode('utf-8')) 
telemetry_data_frame=pd.DataFrame(telemetry_data)
# print(telemetry_data)

#TODO- simulate the race using the telemetry obtained - Filter required fields - Send to Red Panda stream?
# for tel_entry in telemetry_data:
#     print(tel_entry)
#     time.sleep(0.27) #Open F1 updates telemetry every 0.27 seconds

#intervals
url=f'https://api.openf1.org/v1/intervals?session_key={session_key}&driver_number=1' #updates -> every 4 minutes 
response=urlopen(url)
interval_from_lead=json.loads(response.read().decode('utf-8'))
interval_from_lead_data_frame=pd.DataFrame(interval_from_lead)
# print(interval_from_lead)

#weather_data
url=f'https://api.openf1.org/v1/weather?session_key={session_key}' #updates -> every minute
response=urlopen(url)
weather_data=json.loads(response.read().decode('utf-8'))
weather_data_frame=pd.DataFrame(weather_data)
# print(weather_data)

#pit stop data
url=f'https://api.openf1.org/v1/pit?session_key={session_key}&driver_number=1'
response=urlopen(url)
pit_stop_data=json.loads(response.read().decode('utf-8'))
# print(pit_stop_data)
pit_stop_data_frame=pd.DataFrame(pit_stop_data)

#lap details - This is a consolidated data of lap. Maybe send this data after each lap is completed.
url=f'https://api.openf1.org/v1/laps?session_key={session_key}&driver_number=1'
response=urlopen(url)
lap_details=json.loads(response.read().decode('utf-8')) 
# print(lap_details)
lap_details_data_frame=pd.DataFrame(lap_details)

# Intergrate all dataframes and order them based on timestamp
df_live_telemetry=pd.concat([telemetry_data_frame,interval_from_lead_data_frame,pit_stop_data_frame])
df_live_telemetry.sort_values(by='date',inplace=True)
print(df_live_telemetry.columns)
# print(df_live_telemetry.head(20))

df_live_telemetry['date'] = pd.to_datetime(df_live_telemetry['date'],format='ISO8601',utc=True)
df_live_telemetry['delay']=df_live_telemetry['date'].diff().dt.total_seconds().fillna(0)
# print(df_live_telemetry.head(20))

#As the timestamp is not JSON serializable
def telemetry_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

#SENDING SIMULATED LIVE TELEMETRY TO REDPANDA STREAM
producer= KafkaProducer(
    bootstrap_servers= "localhost:9092",
    value_serializer=lambda m: json.dumps(m, default=telemetry_serializer).encode('utf-8')
)

topic = "f1_live_telemetry"

def on_success(metadata):
    print(f"Message sent successfully to topic {metadata.topic} partition {metadata.partition} offset {metadata.offset}")

def on_error(e):
  print(f"Error sending message: {e}")


#PRODUCER
for index,row in df_live_telemetry.iterrows():
   tel=row.to_dict()
   future=producer.send(topic,tel)
   future.add_callback(on_success)
   future.add_errback(on_error)
   time.sleep(row['delay'])


producer.flush()
producer.close()

#TODO  - Check if there is some error in joining of dataframes as multiple rows have 0,0,0 as position
#TODO - After the stream is sent, remove fields for which value in NaN