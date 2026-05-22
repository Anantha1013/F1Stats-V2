from urllib.request import urlopen
import json
import time

import urllib

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
# print(session_data)


#retrieve teams and drivers participating in the session
session_key=session_data[0]['session_key']
url=f'https://api.openf1.org/v1/drivers?session_key={session_key}'
response=urlopen(url)
drivers_details=json.loads(response.read().decode('utf-8'))
# print(drivers_details)

#retrieving session telemetry
url=f'https://api.openf1.org/v1/location?session_key={session_key}&driver_number=1' #for testing , I only have included driver number 1
response=urlopen(url)
telemetry_data=json.loads(response.read().decode('utf-8')) #
print(telemetry_data[0])

#TODO- simulate the race using the telemetry obtained - Filter required fields - Send to Red Panda stream?
for tel_entry in telemetry_data:
    print(tel_entry)
    time.delay(0.27) #Open F1 updates telemetry every 0.27 seconds

#intervals
url=f'https://api.openf1.org/v1/intervals?session_key={session_key}&driver_number=1' #updates -> every 4 minutes 
response=urlopen(url)
interval_from_lead=json.loads(response.read().decode('utf-8'))
# print(interval_from_lead)