import requests, fabric
import sys, logging, time
import config
import citygeo_secrets


global logger
logger = logging.getLogger('main')


def connect_to_api(creds: dict, data=None, attempt:int=0, run_local:bool=False) -> requests.models.Response:
    '''Get the data from the API'''
    logger.info(f'Accessing API - attempt {attempt}\n')
    headers = {
        'Authorization': f'Bearer {creds[config.API_SECRET]["API_KEY"]}',
        'Content-Type': 'application/json'
    }
    if run_local: 
        return requests.get(creds[config.API_SECRET]['API_URL'], 
                            headers=headers, params=data, verify=False)
    else: 
        return requests.get(creds[config.API_SECRET]['API_URL'], 
                            headers=headers, params=data)


def get_asset_data(run_local: bool) -> dict: 
    '''Get asset data from API
        
    If the API indicates an expired token, attempt to generate a new token and get 
    the data one additional time before failing. If successful, will write this 
    new API token to secrets manager
    - `run_local`: If True, do not verify SSL certificates
    '''
    response = citygeo_secrets.connect_with_secrets(
        connect_to_api, config.API_SECRET, attempt=0, run_local=run_local)
    
    if response.ok:
        return response.json()
    elif response.status_code in (401, 500): # Access token needs to be refreshed
        logger.info(f'Received {response.status_code} status code - attempting to refresh token\n')
        new_api_key = citygeo_secrets.connect_with_secrets(
            request_new_access_token, config.API_TOKEN_REFRESH_SECRET)
        creds = citygeo_secrets.get_secrets(config.API_SECRET)

        # "Heisenbug" appears only in production but not in debugging. 
        # Perhaps because this function runs in < 1 second in production, and their 
        # API can't reset to a new token fast enough, causing it to reissue a 401 HTTP error...
        old_api_key = creds[config.API_SECRET]['API_KEY']
        logger.debug(f'Old API Key ends with: {old_api_key[-5:]}')
        creds[config.API_SECRET]['API_KEY'] = new_api_key
        logger.debug(f'New API Key ends with: {new_api_key[-5:]}')
        logger.debug('Sleeping for 5 seconds')
        time.sleep(5) # Allow their API time to instate the new token

        response = connect_to_api(creds=creds, attempt=1, run_local=run_local)
        if response.ok: 
            citygeo_secrets.update_secret(config.API_SECRET, {'API_KEY': new_api_key})
            return response.json()
    
    sys.exit(f"Request failed with status code: {response.status_code}")


def request_new_access_token(creds: dict, full_response:bool=False) -> str | dict: 
    '''Request a new access token from the API
    #### Parameters
    - `creds`: Dict of credentials
    - `full_response`: If False (default), then simply return the new access token, 
    otherwise return the full HTTP response.'''
    url = creds[config.API_TOKEN_REFRESH_SECRET].pop('url')
    data = creds[config.API_TOKEN_REFRESH_SECRET]
    
    response = requests.post(url, data)
    logger.info(f'New token acquired - expires in {response.json()["expires_in"]:,} seconds\n')

    if not full_response: 
        return response.json()['access_token']
    else: 
        return response.json()



def get_sftp_conn(sftp_creds: dict) -> fabric.Connection: 
    '''Return SFTP connection'''

    # look_for_keys: False is necessary otherwise paramiko will incorrectly attempt
    # to use our user's keys in our ~/.ssh folder and fail without ever trying
    # to use our supplied password
    creds = sftp_creds['SFTP Server - CityGeo']
    sftp_conn = fabric.Connection(
        host=creds['Host'], user=creds['login'], 
        connect_kwargs={'password': creds['password'], 'look_for_keys': False},
        )
    return sftp_conn


def upload_to_sftp(local_filename: str, remote_filename: str): 
    '''Upload a file to SFTP'''
    sftp_conn = citygeo_secrets.connect_with_secrets(get_sftp_conn, config.SFTP_SECRET)
    with sftp_conn.sftp() as sftp_client: 
        sftp_client.put(local_filename, remote_filename)
    logger.info(f'Successfully uploaded file to SFTP server at "{remote_filename}"\n')
