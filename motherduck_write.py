import duckdb
import os

os.environ["MOTHERDUCK_TOKEN"] = (<"token">)
local_csv_path = "F1Stats-V2/standings_2022.jsonl"
target_database = "f1_historical"
target_table = "f1_history"

print(" Connecting to MotherDuck...")


con = duckdb.connect(f"md:{target_database}")

con.sql("SHOW ALL DATABASES").show()

print(f"Processing CSV locally and streaming to MotherDuck...")
# This single command reads the CSV locally and creates the table in the cloud
con.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_database}.{target_table} AS 
    SELECT * FROM '{local_csv_path}';
""")

print("Upload complete! Zero cloud compute limits were harmed.")

print("\n First 5 rows in MotherDuck:")
con.sql(f"SELECT * FROM {target_database}.{target_table} LIMIT 5;").show()
