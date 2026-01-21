#!/usr/bin/env python3
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import StringIO

import pandas as pd
import requests


def main():
    if os.path.exists("downloads_tracking.json"):
        with open("downloads_tracking.json", "r") as f:
            tracking = json.load(f)
    else:
        tracking = {}

    url = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
    response = requests.get(url)
    response.raise_for_status()
    all_datasets = response.json()
    
    hospital_datasets = []
    for dataset in all_datasets:
        themes = dataset.get("theme", [])
        if "Hospitals" in themes:
            hospital_datasets.append(dataset)
    
    os.makedirs("data", exist_ok=True)
    
    def process_dataset(dataset):
        dataset_id = dataset.get("identifier")
        title = dataset.get("title", "Unknown")
        modified_date = dataset.get("modified")
        
        # Find the CSV download URL
        csv_url = None
        for dist in dataset.get("distribution", []):
            if dist.get("mediaType") == "text/csv":
                csv_url = dist.get("downloadURL")
                break
        
        if not csv_url:
            return
        
        # Check if we need to download this dataset
        if dataset_id in tracking and tracking[dataset_id].get("modified_date") == modified_date:
            return
        
        try:
            response = requests.get(csv_url, timeout=60)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text), low_memory=False)
            new_columns = []
            for col in df.columns:
                col = re.sub(r"[^\w\s]", "", col)
                col = col.replace(" ", "_")
                col = col.lower()
                col = re.sub(r"_+", "_", col)
                col = col.strip("_")
                new_columns.append(col)
            
            df.columns = new_columns
            
            output_filename = f"data/{dataset_id}.csv"
            df.to_csv(output_filename, index=False)

            tracking[dataset_id] = {
                "title": title,
                "modified_date": modified_date,
                "downloaded_at": datetime.now().isoformat(),
                "rows": len(df),
                "columns": len(df.columns)
            }
        except Exception as e:
            print(f"  ✗ Error processing {title}: {e}")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        for dataset in hospital_datasets:
            executor.submit(process_dataset, dataset)
    
    with open("downloads_tracking.json", "w") as f:
        json.dump(tracking, f, indent=2)
    
    print()
    print(f"Processed files are in the 'data' folder")
    print(f"Tracking data saved to 'downloads_tracking.json'")

if __name__ == "__main__":
    main()
