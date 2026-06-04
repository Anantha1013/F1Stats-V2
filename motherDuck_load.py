import json
import requests
import duckdb as ddb
import pandas as pd
import os
from dotenv import load_dotenv


load_dotenv()

#ESTABLISHING MOTHERdUCK CONNECTION
mdb_token=os.getenv('MDB_TOKEN')
mdb_database=os.getenv('MDB_DATABASE')
print(" Connecting to MotherDuck...")
con=ddb.connect(f'md:{mdb_database}?motherduck_token={mdb_token}')
print("Success....Listing databases")
con.sql("SHOW DATABASES").show()

#Lap Data Table in Mother Duck
#season, round and circuit id in another table?
#season, round, driver_id, lap_number, lap_time (seconds),position
#required pit in (in_lap) and out_lap - true or false values - To integrate here or in the separate pit stop table?
def load_historical_lap_data():
    try:
        relation_name="lap_Data"

        historical_data_years=[2023,2024,2025] #run for one season at a time to avoid api rate limits. 
        for year in historical_data_years:
            round=1
            while round<=24: #run 7 rounds at a time
                limit=100
                offset=0
                total=1
                while offset<total:
                    url=f'https://api.jolpi.ca/ergast/f1/{year}/{round}/laps.json'
                    params={'limit':limit,'offset':offset}
                    response=requests.get(url,params=params)
                    if response.status_code==200:
                        lap_data=response.json()
                        total=int(lap_data['MRData']['total'])
                        races = lap_data['MRData']['RaceTable']['Races']
                        races[0]['circuitId']=races[0]['Circuit']['circuitId']
                        temp_lap_df = pd.json_normalize(races,record_path=['Laps', 'Timings'],meta=['season', 'round','circuitId',['Laps','number']])
                        temp_lap_df=temp_lap_df.rename(columns={'Laps.number':'lap_number'})
                        # print(f"Retrieved data for year {year} and round {round} with offset {offset}. Total records: {total}")
                        # print(temp_lap_df.head(30))
                        
                        try:
                            table_exists = con.sql(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{relation_name}'").fetchone()
                            if not table_exists:
                                con.sql(f"CREATE TABLE IF NOT EXISTS {mdb_database}.main.{relation_name} AS SELECT * FROM temp_lap_df")
                                print(f"Created table {relation_name} and inserted first batch")
                            else:
                                con.sql(f"INSERT INTO {mdb_database}.main.{relation_name} SELECT * FROM temp_lap_df")
                                print(f"Written a batch of data to MotherDuck for year {year} and {round} with offset {offset}.....")
                        except Exception as e:
                            print("Failed to write data to MotherDuck:",e)
                    
                    else:
                        print(f"Failed to retrieve data for year {year} and {round} with offset {offset}. Status code: {response.status_code}")
                    offset+=limit
                    # break
                round+=1
                # break
            # break
            
    except Exception as e:
        print("Failed to connect to MotherDuck:",e)

#Pit stop data Table in Mother Duck
#season, round, circuit, driver_id, lap,pit_duration,stop,time,position (or position_before?), gap_before?
def load_historical_pit_stop_data():
    try:
        relation_name="pit_stop_table"
        historical_years=[2025]
        for year in historical_years:
            round=1
            while round<=24:
                limit=100
                offset=0
                total=1
                while offset<total:
                    url=f"https://api.jolpi.ca/ergast/f1/{year}/{round}/pitstops.json"
                    params={'limit':limit,'offset':offset}
                    response=requests.get(url,params=params)
                    if response.status_code==200:
                        pit_stop_data=response.json()
                        total=int(pit_stop_data['MRData']['total'])
                        pit_stops=pit_stop_data['MRData']['RaceTable']['Races'][0]
                        pit_stops['circuitid']=pit_stops['Circuit']['circuitId']
                        temp_pit_stop_df=pd.json_normalize(pit_stops,record_path=['PitStops'],meta=['season','round','circuitid'])
                        # print(temp_pit_stop_df)

                        try:
                            table_exists = con.sql(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{relation_name}'").fetchone()
                            if not table_exists:
                                con.sql(f"CREATE TABLE IF NOT EXISTS {mdb_database}.main.{relation_name} AS SELECT * FROM temp_pit_stop_df")
                                print(f"Created table {relation_name} and inserted first batch")
                            else:
                                con.sql(f"INSERT INTO {mdb_database}.main.{relation_name} SELECT * FROM temp_pit_stop_df")
                                print(f"Written a batch of data to MotherDuck for year {year} and {round} with offset {offset}.....")
                        except Exception as e:
                            print("Failed to write data to MotherDuck:",e)

                    else:
                        print(f'Failed to retrive pit stop data for season {year} and round {round} with offste {offset}: Status code: {response.status.code}')
                    offset+=limit
                    # break
                round+=1
                # break
            # break
    except Exception as e:
        print("Some error occured while loading pit stop data: ",e)

#Weather data table in Mother Duck
def load_historical_weather_data():
    try:
        relation_name="weather_relation"
        url="https://api.openf1.org/v1/weather"
        response=requests.get(url)
        if response.status_code==200:
            weather_data=response.json()
            temp_weather_df=pd.json_normalize(weather_data)
            temp_weather_df=temp_weather_df.rename(columns={'session_key':'openF1_session_key'})
            temp_weather_df=temp_weather_df.drop(columns=['meeting_key'])
            try:
                table_exists = con.sql(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{relation_name}'").fetchone()
                if not table_exists:
                    con.sql(f"CREATE TABLE IF NOT EXISTS {mdb_database}.main.{relation_name} AS SELECT * FROM temp_weather_df")
                    print(f"Created table {relation_name} and inserted weather data for 2023, 2024, 2025 and recent 2026 races")
                else:
                    con.sql(f"INSERT INTO {mdb_database}.main.{relation_name} SELECT * FROM temp_weather_df")
                    print("Written weather table to motherDuck")
            except Exception as e:
                print("Failed to write data to MotherDuck:",e)
        else:
            print(f"Failed to fetch weather data:{response.status_code}")
    except Exception as e:
        print("Some error occured while fethcing weather data: ",e)

if __name__=="__main__":
    load_historical_lap_data()
    load_historical_pit_stop_data()
    load_historical_weather_data()


#TODO - Create a table in mother duck for relating session key in Open f1 to season, round and circuit in Jolpica
#TODO - Integrate pit stop table and interval in open f1
#TODO - Tyre conditions table from FastF1?
