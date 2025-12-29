import json
import os
import re

def load_config(config_path):
    with open(config_path, 'r') as f:
        content = f.read()

    pattern = re.compile(r'\$\{([^}]+)\}')
    
    def replace_env(match):
        env_var = match.group(1)
        return os.getenv(env_var, f"${{{env_var}}}")
    
    updated_content = pattern.sub(replace_env, content)
    return json.loads(updated_content)