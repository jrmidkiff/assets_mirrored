import sqlalchemy as sa, requests
import config as conf, utils
from config_db import asset_history, metadata, create_engine, setup_db_tables
from assetdetails import request_new_access_token
import citygeo_secrets as cgs
from typing import Sequence
from concurrent.futures import ThreadPoolExecutor
import datetime as dt, zoneinfo, logging, time, threading, json
from urllib3.util import Retry
from requests.adapters import HTTPAdapter


def setup_global_vars(run_local: bool): 
    '''Set up the global variables needed for multithreading'''
    global base_url, base_headers, asset_histories, locally_run, thread_data
    base_url = cgs.connect_with_secrets(get_base_url, conf.API_SECRET)
    base_headers = cgs.connect_with_secrets(get_headers, conf.API_SECRET)
    asset_histories = []
    locally_run = run_local
    thread_data = threading.local()


def get_base_url(creds: dict) -> str: 
    '''Get base url for API request'''
    return creds[conf.API_SECRET]['asset_history_api_url']


def get_headers(creds: dict) -> dict: 
    '''Get headers for API request'''
    return {
        'Authorization': f'Bearer {creds[conf.API_SECRET]["API_KEY"]}',
        'Content-Type': 'application/json'
    }


def validate_api_token(id: str): 
    '''Confirm that the API Token is valid
    
    If close to its expiration, automatically request a new token and update 
    the relevant secret in Keeper/Drive/Cache and the headers used in this script 
    for all requests
    
    #### Parameters
    - `id`: The ID of an asset to get the history of. Becomes part of URL.'''    
    global base_headers
    logger.info('Validating API Token')
    now = dt.datetime.now(tz=zoneinfo.ZoneInfo('US/Eastern'))

    with open(conf.API_UPDATE_FILE, 'r') as f: 
        previous_api_update = json.load(f)
        previous_timestamp = dt.datetime.fromisoformat(previous_api_update['timestamp'])
        previous_expires_in_delta = dt.timedelta(seconds=previous_api_update['expires_in_seconds'])
    
    # 14/15 is (mostly) arbitrary to represent that the token is close to expiring
    if previous_timestamp + (previous_expires_in_delta * 14/15) < now : 
        logger.info(f'Token is close to expiring. Attempting to refresh token\n')
        response_json = cgs.connect_with_secrets(
            request_new_access_token, conf.API_TOKEN_REFRESH_SECRET, full_response=True)
        api_update_dict = {}
        new_api_key = response_json['access_token']
        expires_in = response_json['expires_in']
        creds = cgs.get_secrets(conf.API_SECRET)
        old_api_key = creds[conf.API_SECRET]['API_KEY']
        api_update_dict['timestamp'] = str(now)
        api_update_dict['expires_in_seconds'] = expires_in
        api_update_dict['old_key_ends_with'] = old_api_key[-5:]
        api_update_dict['new_key_ends_with'] = new_api_key[-5:]

        # "Heisenbug" appears only in production but not in debugging. 
        # Perhaps because this function runs in < 1 second in production, and their 
        # API can't reset to a new token fast enough, causing it to reissue a 401 HTTP error...
        logger.info(f'API Update Information: {api_update_dict}')
        creds[conf.API_SECRET]['API_KEY'] = new_api_key
        logger.info('Sleeping for 5 seconds\n')
        time.sleep(5) # Allow their API time to instate the new token
        
        base_headers = get_headers(creds) # global var; specified earlier in this func
        s = create_session()        
        page = 1
        verify = not locally_run
        url = f'{base_url}{id}/observations'
        response = s.get(url, headers=base_headers, params={'page': page}, verify=verify)    

        if response.ok: 
            cgs.update_secret(conf.API_SECRET, {'API_KEY': new_api_key})
            with open(conf.API_UPDATE_FILE, 'w') as f: 
                json.dump(api_update_dict, f)
                f.write('\n')
    else: 
        logger.info(f'Current token valid until {str(previous_timestamp + previous_expires_in_delta)}')

    
def get_asset_history(id: str):
    '''Get the data from the API with multithreading, extending the global list 
    asset_histories

    Each thread will have a separate instance of this function with one id at a time
    #### Parameters
    - `id`: The ID of an asset to get the history of. Becomes part of URL.'''
    if not hasattr(thread_data, "session"):
        thread_data.session = create_session()
    s = thread_data.session
    url = f'{base_url}{id}/observations'
    page = 1
    id_history = []
    print_string = f'Requesting {url = }\n'
    now = dt.datetime.now(tz=zoneinfo.ZoneInfo('US/Eastern'))
    while True: 
        verify = not locally_run
        response = s.get(url, headers=base_headers, params={'page': page}, verify=verify)    
        response.raise_for_status()
        j = response.json()
        data = j['data']
        if data == []: 
            last_page = page - 1
            break

        id_history.extend(data)
        if j['totalEntityCount'] < j['pageLength']: 
            last_page = page
            break

        try: 
            latest = dt.datetime.strptime(data[-1]['lastSeenTime'], "%Y-%m-%dT%H:%M:%S.%f%z")
        except ValueError: 
            latest = dt.datetime.strptime(data[-1]['lastSeenTime'], "%Y-%m-%dT%H:%M:%S%z")
        if now - latest > dt.timedelta(days=365): 
            print_string += f'\tBeyond 365 days, ceasing to extract records\n'
            last_page = page
            break
        page += 1
    
    print_string += f'\t{len(id_history):,} records on {last_page} page(s) consumed by thread {threading.current_thread().name}'
    for row in id_history: 
        row['id'] = id
    
    logger.info(print_string)
    asset_histories.extend(id_history)
    

def create_session() -> requests.Session: 
    '''Ceate the requests session for automatic retries on a 500 error'''
    # https://requests.readthedocs.io/en/latest/user/advanced/#example-automatic-retries
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[500],
        allowed_methods={'GET', 'POST'}
    )
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s


def update(ids: Sequence[str], run_local:bool, test: bool = True):
    '''Update asset_history table'''
    global logger
    logger = logging.getLogger('main')
    
    logger.info(f'{"*" * 80}')
    logger.info('Beginning Run Asset History script\n')
    
    engine = cgs.connect_with_secrets(create_engine, 
        conf.DB_SECRET_HOST, conf.DB_SECRET_HOST_TEST, 
        conf.DB_SECRET_LOCAL, conf.DB_SECRET_LOCAL_TEST, 
        conf.DB_SECRET_LOGIN, 
        test=test, run_local=run_local)
    
    setup_db_tables(engine, metadata, drop=False)
    setup_global_vars(run_local=run_local)
    validate_api_token(id=ids[0])

    with ThreadPoolExecutor(max_workers=min(len(ids), conf.MAX_CONCURRENT_CALLS)) as executor: # Automatically waits for all futures to finish executing
        iterator = executor.map(get_asset_history, ids, timeout=300)
        print(f'Active thread count: {threading.active_count()}\n')
    for _ in iterator: # Go through each thread afterwards and detect if any error occured
        pass
    
    # Unfortunately iterating twice through the data now in order to match lower_case and add updated_on
    now = dt.datetime.now(
        tz=zoneinfo.ZoneInfo('US/Eastern'))
    data = []
    for row in asset_histories: 
        new_row = {k.lower(): v for k, v in row.items()}
        new_row['updated_on'] = now
        data.append(new_row)

    with engine.begin() as conn: 
        # Delete all IDs updated in Assets
        stmt_delete = (sa
                        .delete(asset_history)
                        .where(asset_history.c.id.in_(ids)))
        result = conn.execute(stmt_delete)
        utils.print_sa_stmt(stmt_delete, result.rowcount)
        
        # Append the history of those IDs updated in or newly appended to Assets
        stmt = sa.insert(asset_history)
        result = conn.execute(stmt, data)
        utils.print_sa_stmt(stmt, len(asset_histories))
