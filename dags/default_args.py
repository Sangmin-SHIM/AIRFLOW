from datetime import timedelta, datetime

DEFAULT_ARGS = {
    "owner": "sangmin SHIM",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2023, 5, 1),
}

def get_default_args():
    return DEFAULT_ARGS