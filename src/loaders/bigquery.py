import json
import os
from datetime import datetime

class BigQueryLoader:
    def __init__(self, sink: str = "bq", dataset_id: str = None):
        self.sink = sink
        self.output_dir = "/Users/galih/personal/de-platform-warehouse/result"
        
        if self.sink == "local":
            os.makedirs(self.output_dir, exist_ok=True)
        elif self.sink == "bq":
            from google.cloud import bigquery
            
            project_id = os.getenv("GCP_PROJECT_ID")
            
            # Use JSON dataset if provided; otherwise, fallback to the global .env dataset
            self.dataset_id = dataset_id or os.getenv("GCP_DATASET_ID")
            
            if not project_id:
                raise ValueError("GCP_PROJECT_ID must be defined in the .env file.")
            if not self.dataset_id:
                raise ValueError("A dataset must be defined globally in .env (GCP_DATASET_ID) or locally in the JSON config.")
                
            self.client = bigquery.Client(project=project_id)
            self.dataset_ref = f"{project_id}.{self.dataset_id}"

    def load(self, data: list, table_name: str, time_field: str, type_overrides: dict):
        """Routes data loading logic based on the requested sink."""
        print(f"  [Loader] Preparing to write {len(data)} payloads to `{table_name}` via {self.sink.upper()}.")
        
        if self.sink == "local":
            self._load_local(data, table_name)
        elif self.sink == "bq":
            self._load_bq(data, table_name, time_field, type_overrides)

    def _load_local(self, data: list, table_name: str):
        """Saves the extracted payload to a local JSON file."""
        run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_{run_timestamp}.json"
        filepath = os.path.join(self.output_dir, filename)

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            print(f"  [Loader] SUCCESS: Data saved locally to {filepath}")
        except Exception as e:
            print(f"  [Loader] ERROR: Failed to save data locally. {e}")

    def _load_bq(self, data: list, table_name: str, time_field: str, type_overrides: dict):
        """Transforms columnar Open-Meteo arrays into rows and loads into BigQuery."""
        from google.cloud import bigquery
        
        table_id = f"{self.dataset_ref}.{table_name}"
        rows_to_insert = []
        current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # 1. Flatten the Data
        for response in data:
            target_name = response.get("_target_name", "unknown")
            
            data_block = response.get("daily") or response.get("hourly")
            if not data_block:
                continue
                
            times = data_block.get("time", [])
            
            for i, t in enumerate(times):
                row = {
                    "location": target_name,
                    time_field: t,
                    "processed_timestamp": current_time
                }
                
                for key, values in data_block.items():
                    if key == "time":
                        continue
                        
                    val = values[i] if values and i < len(values) else None
                    row[key] = val
                    
                rows_to_insert.append(row)

        if not rows_to_insert:
            print("  [Loader] No rows to insert after transformation.")
            return

        print(f"  [Loader] Transformed into {len(rows_to_insert)} rows. Executing Load Job...")

        # 2. Configure and run the BigQuery Load Job
        job_config = bigquery.LoadJobConfig(
            autodetect=True, 
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="processed_timestamp"
            )
        )

        try:
            job = self.client.load_table_from_json(
                rows_to_insert, 
                table_id, 
                job_config=job_config
            )
            job.result() 
            
            print(f"  [Loader] SUCCESS: Appended {job.output_rows} rows to `{table_id}`.")
            
        except Exception as e:
            print(f"  [Loader] ERROR during BigQuery load: {e}")
            if hasattr(e, 'errors'):
                print(f"  [Loader] Details: {e.errors}")