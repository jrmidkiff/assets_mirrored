import requests, json, click
from config import AIRFLOW_SECRET
from datetime import datetime, timezone, timedelta
import urllib.parse
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
import citygeo_secrets


def check_dag_runnable(creds: dict, dagname: str, session: requests.Session) -> bool:
    '''Determine if we we can run the dag which checks to see if the dag is already running
    or if the dag recently failed. Runs after we determine that the source dataset has changed.'''
    # Docs: https://airflow.apache.org/docs/apache-airflow/2.2.3/stable-rest-api-ref.html#operation/post_dag_run

    url = creds[AIRFLOW_SECRET]['url']
    login = creds[AIRFLOW_SECRET]['login']
    password = creds[AIRFLOW_SECRET]['password']
    
    # only return jobs that have a start time from the last 4 days
    hours = 96
    minus4days = datetime.isoformat(datetime.now(timezone.utc) - timedelta(hours=hours))
    minus4days = urllib.parse.quote(minus4days, safe="")

    # DAGNAME should look something like: 'elections__polling_places'
    endpoint = f'{url}/api/v1/dags/{dagname}'
    # NOTE: add order_by so that we definitely look at the latest runs only by their end_date.
    # the '-' reverses the sort order (descending) so we don't get our results truncated
    # and we missed recently running dags.
    # EDIT: let's have it look at dag_run_id instead since that is guaranteed to have a date
    # whereas other fields may not.

    order_by = '&order_by=-dag_run_id'
    runs_endpoint_url = endpoint + f'/dagRuns?limit=20&start_date_gte={minus4days}' + order_by

    body = {
        "conf": { }
    }
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    # First let's figure out if the DAG is paused, otherwise we'll have an issue with DAGs queuing
    # up endlessly. The dagRuns endpoint doesn't return runs in a 'queued' state (possibly only for
    # paused dags?), so we have to use this method first.
    result = session.get(
        endpoint,
        headers=headers,
        data=json.dumps(body),
        auth=(login, password), 
        verify=False
    )
    result.raise_for_status()
    return_dict = result.json()
    if return_dict['is_paused']:
        raise AssertionError(f'Dag "{dagname}" is paused, not triggering..')

    # Now check out the dag runs in the last 96 hours if the dag is not paused.
    result = session.get(
        runs_endpoint_url,
        headers=headers,
        data=json.dumps(body),
        auth=(login, password), 
        verify=False
        )
    result.raise_for_status()
    return_dict = result.json()

    # Loop over all returned runs for this dag
    # Looking for 'running' and 'failed' states.
    if return_dict["dag_runs"]:
        for i in return_dict["dag_runs"]:
            if i['state'] == 'running':
                print(f'Dag "{dagname}" is still running, not triggering.')
                dag_run_id = i['dag_run_id']
                print(f'Running dag_run_id: {dag_run_id}')
                return False
            # note: dagRuns does not appear to return info about queued runs
            # if the dag is paused.
            if i['state'] == 'queued':
                print(f'Dag "{dagname}" is already queued, not triggering.')
                dag_run_id = i['dag_run_id']
                print(f'Queued dag_run_id: {dag_run_id}')
                return False
    # If we got through the for loop without returning, return True
    else: 
        print(f'No dags returned in last {hours} hours, triggering')
    print('check_dag_runnable passed.\n')
    return True


def trigger_dag(creds: dict, dagname: str, session: requests.Session):
    '''Trigger an airflow dag'''
    # Docs: https://airflow.apache.org/docs/apache-airflow/2.2.3/stable-rest-api-ref.html#operation/post_dag_run
    # https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#tag/DAGRun

    url = creds[AIRFLOW_SECRET]['url']
    login = creds[AIRFLOW_SECRET]['login']
    password = creds[AIRFLOW_SECRET]['password']
    
    # DAGNAME should look something like: 'elections__polling_places'
    print(f'Triggering DAG run via API! Dagname should be: {dagname}')

    endpoint_URL=f'{url}/api/v1/dags/{dagname}/dagRuns'

    # turns out dates aren't needed? Specifying no logical_date (execution_date)
    # will make it run immediately, if the Dag is enabled?
    body = {
        "conf": { }
    }
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    result = session.post(
        endpoint_URL,
        headers=headers,
        data=json.dumps(body),
        auth=(login, password), 
        verify=False
        )
    if result.status_code != 200:
        print('Did not get a 200 response code back from the API!')
        print(result.text)
    print("New dag_run_id: " + str(result.json()['dag_run_id']))


def main(dagname: str): 
    '''Trigger an airflow dag if the dag is runnable
    - dagname: name of dag to trigger'''
    citygeo_secrets.set_config(keeper_dir='~')
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[400, 404],
        allowed_methods={'GET', 'POST'}
    )
    s.mount('https://', HTTPAdapter(max_retries=retries))

    if citygeo_secrets.connect_with_secrets(check_dag_runnable, AIRFLOW_SECRET, 
                                            dagname=dagname, session=s): 
        citygeo_secrets.connect_with_secrets(trigger_dag, AIRFLOW_SECRET, 
                                             dagname=dagname, session=s)
