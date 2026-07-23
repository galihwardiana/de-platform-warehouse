import json
import os
import re

def load_and_resolve_config(config_path: str) -> dict:
    """Loads a JSON file, resolves env vars, and merges external target files."""
    with open(config_path, 'r') as file:
        config_str = file.read()

    pattern = re.compile(r'\$\{([^}]+)\}')
    
    def replace_env(match):
        env_var = match.group(1)
        return os.environ.get(env_var, "")

    resolved_config_str = pattern.sub(replace_env, config_str)
    config = json.loads(resolved_config_str)

    if "targets_file" in config:
        targets_path = config["targets_file"]
        
        if not os.path.exists(targets_path):
            raise FileNotFoundError(f"Targets file missing: {targets_path}")
            
        with open(targets_path, 'r') as targets_file:
            config["targets"] = json.load(targets_file)

    return config