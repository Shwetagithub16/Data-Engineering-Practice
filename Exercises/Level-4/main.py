import glob
import os
import pandas as pd
import json
search_directory = "./data"
file_extension = "*.json"

#Taks:
#Find all the json files located in the data folder
#After flattening out the nested json, read them with Python and convert them to csv files.

def main():
    # your code here
    json_files = glob.glob(os.path.join(search_directory,"**", file_extension), recursive=True)
    print(json_files)

    def flatten_json(nested_json, parent_key='', sep='_'):
        """Recursively flatten a nested JSON object."""
        items = {}
        for k, v in nested_json.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(flatten_json(v, new_key, sep=sep))
            elif isinstance(v, list):
                if all(isinstance(i, dict) for i in v):  # Handle list of dicts
                    for idx, item in enumerate(v):
                        items.update(flatten_json(item, f"{new_key}_{idx}", sep=sep))
                else:
                    items[new_key] = ",".join(map(str, v))  # Convert lists to strings
            else:
                items[new_key] = v
        return items

    def process_json_file(json_file):
        """Load and flatten JSON data, then save as CSV."""
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Ensure the JSON is a list of records for CSV conversion
        if isinstance(data, dict):
            data = [data]

        flattened_data = [flatten_json(record) for record in data]

        # Convert to DataFrame and save as CSV
        df = pd.DataFrame(flattened_data)
        csv_file = json_file.replace(".json", ".csv")

        df.to_csv(csv_file, index=False, encoding="utf-8")
        print(f"Saved CSV: {csv_file}")

    print(f"Found {len(json_files)} JSON files.")

    for json_file in json_files:
        process_json_file(json_file)

    print("All JSON files processed.")
if __name__ == "__main__":
    main()
