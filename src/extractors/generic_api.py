import requests
from src.loaders.bigquery import BigQueryLoader

class GenericAPIExtractor:
    def __init__(self, config: dict, sink: str = "bq"):
        self.config = config
        self.api_name = self.config.get("api_name", "unknown_api")
        self.sink = sink
        self.loader = BigQueryLoader(sink=self.sink)

    def extract(self) -> dict:
        """Executes API requests for each pipeline and target combination."""
        method = self.config.get("method", "GET").upper()
        base_url = self.config.get("base_url", "").rstrip("/")
        headers = self.config.get("headers", {})
        
        targets = self.config.get("targets", {"default": {}})
        pipelines = self.config.get("pipelines", {"default": {}})
        
        all_results = {}

        for pipeline_name, pipeline_config in pipelines.items():
            print(f"[{self.api_name}] Starting pipeline: {pipeline_name}")
            
            pipeline_data = []
            pipeline_params = pipeline_config.get("request_params", {})
            
            for target_name, target_coords in targets.items():
                merged_params = {**pipeline_params, **target_coords}
                
                # --- FIX: Convert lists to comma-separated strings for the API ---
                for key, value in merged_params.items():
                    if isinstance(value, list):
                        merged_params[key] = ",".join(str(v) for v in value)
                
                print(f"  -> Fetching target: {target_name}")
                
                response = requests.request(
                    method=method,
                    url=base_url,
                    headers=headers,
                    params=merged_params,
                )
                
                # --- FIX: Better debugging if the response fails ---
                try:
                    response.raise_for_status()
                    data = response.json()
                except Exception as e:
                    print(f"\n[ERROR] API Request failed for {target_name}")
                    print(f"URL Executed: {response.url}")
                    print(f"Status Code: {response.status_code}")
                    print(f"Raw Response: {response.text}\n")
                    raise e
                
                data["_target_name"] = target_name 
                pipeline_data.append(data)

            all_results[pipeline_name] = {
                "bq_config": pipeline_config,
                "data": pipeline_data
            }

        return all_results

    def run(self) -> None:
        """Orchestrates extraction and passes grouped data to the loader."""
        print(f"[{self.api_name}] Starting ingestion...")
        
        pipeline_results = self.extract()
        
        for pipeline_name, result in pipeline_results.items():
            data = result["data"]
            bq_config = result["bq_config"]
            
            print(f"[{self.api_name} - {pipeline_name}] Retrieved data for {len(data)} targets. Sending to loader...")
            
            self.loader.load(
                data=data,
                table_name=bq_config.get("bq_table", pipeline_name),
                time_field=bq_config.get("time_field", ""),
                type_overrides=bq_config.get("field_type_overrides", {})
            )