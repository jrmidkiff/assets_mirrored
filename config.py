API_SECRET = 'CCO/asset_management_API'
API_TOKEN_REFRESH_SECRET = 'CCO/asset_management_API/token_reset_request'
DB_SECRET_LOGIN = 'databridge-v2/citygeo'
DB_SECRET_HOST = 'databridge-v2/hostname'
DB_SECRET_HOST_TEST = 'databridge-v2/hostname-testing'
DB_SECRET_LOCAL = 'databridge-v2/hostname-pgbouncer'
DB_SECRET_LOCAL_TEST = 'databridge-v2/hostname-testing-pgbouncer'
SFTP_SECRET = 'SFTP Server - CityGeo'
AIRFLOW_SECRET = 'airflow-v2/airflow'
FILE_NAME = 'asset_data.xlsx'
SCHEMA = 'citygeo'
VIEWER_SCHEMA = 'viewer_cco'

MAX_CONCURRENT_CALLS = 20  # Safe limit recommended by InThing, owner of Visium API
API_UPDATE_FILE = 'api_update.json'

SFTP_DIRECTORY = 'CCO_Asset_Management'

DAG_NAME_ASSETS = f'{SCHEMA}__assets'
DAG_NAME_ASSET_ROUTER_LOCATIONS = f'{SCHEMA}__asset_router_locations'
DAG_NAME_ASSET_HISTORY = f'{SCHEMA}__asset_history'
DAG_NAME_POLLBOOK_LOCATIONS = f'{SCHEMA}__pollbook_locations'
