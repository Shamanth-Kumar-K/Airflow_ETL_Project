from airflow.models import Variable

def validate_api_key():
    try:
        api_key = Variable.get("ETL_API_KEY")
    except KeyError:
        raise PermissionError("ETL API key missing in Airflow Variables")

    if not api_key:
        raise PermissionError("ETL API key is empty")
