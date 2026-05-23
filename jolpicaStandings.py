import os
import json
import time
import requests

output_dir = "f1"
os.makedirs(output_dir, exist_ok=True)

target_seasons = [2022, 2023, 2024, 2025]

for season in target_seasons:
    print(f"\n Season: {season}")
    
    schedule_url = f"https://api.jolpi.ca/ergast/f1/{season}.json"
    try:
        schedule_res = requests.get(schedule_url)
        if schedule_res.status_code == 200:
            schedule_data = schedule_res.json()
            total_rounds = int(schedule_data["MRData"]["total"])
            print(f"{total_rounds} rounds for the {season} season.")
        else:
            continue
    except Exception as e:
        continue

    year_path = os.path.join(output_dir, f"standings_{season}.jsonl")
    
    with open(year_path, "w", encoding="utf-8") as jsonl_file:
        
        for round_num in range(1, total_rounds + 1):
            url = f"https://api.jolpi.ca/ergast/f1/{season}/{round_num}/driverstandings.json"
            print(f"   -> Extracting Round {round_num}/{total_rounds}...")
            
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    raw_data = response.json()
                    
                    try:
                        standings_array = raw_data["MRData"]["StandingsTable"]["StandingsLists"][0]["DriverStandings"]
                        
                    
                        line_object = {
                            "season": season,
                            "round": round_num,
                            "driver_standings": standings_array
                        }
                        
                        jsonl_file.write(json.dumps(line_object) + "\n")
                        
                    except (KeyError, IndexError):
                        print(f"Unexpected JSON schema format on Round {round_num}")
                else:
                    print(f"HTTP {response.status_code}  Round {round_num}")
            except Exception as e:
                print(f"connection failure on Round {round_num}: {e}")
                
            time.sleep(1.0)
            

print("\n process completed.")