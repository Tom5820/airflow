from airflow.models import Variable

CONFIG = Variable.get("SECRET_CONFIG", deserialize_json=True)
def get(key: str, default=None):
    return CONFIG.get(key, default)