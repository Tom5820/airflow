# plugins/utils/get_config.py
from airflow.models import Variable

_CONFIG = None

def get_config():
    """Lazy load config from Airflow Variables"""
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = Variable.get("SECRET_CONFIG", deserialize_json=True)
    return _CONFIG

def get(key: str, default=None):
    """Get config value by key"""
    config = get_config()
    return config.get(key, default)

# Để backward compatible, có thể dùng property
class ConfigProxy:
    def __getitem__(self, key):
        return get_config()[key]
    
    def get(self, key, default=None):
        return get_config().get(key, default)

CONFIG = ConfigProxy()