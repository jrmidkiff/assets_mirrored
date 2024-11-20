**_THIS IS A MIRRORED VERSION OF AN INTERNAL PRODUCTION ETL REPOSITORY LAST UPDATED 2024-11-20_**

# phillyvotes-assetmanagement
_Principal Author: James Midkiff (CityGeo)_  
_Author of_ dag_trigger.py: _Roland MacDavid (CityGeo)_  
_Secondary Author: Jophy Samuel (CCO)_  

First, download the latest asset data from https://phldoe.visium.io, upload that data to the table _Assets_ in databridge, and update an excel file in [the SFTP server](https://secure-ftp.phila.gov/). 

Next, for each asset that was upserted into databridge, get the observation history for those assets and upload their history to  the table _Asset_History_. 

And finally, join this _Assets_ data to the _Routers_ data via the table _router_precincts_ to associate each asset with a router location by precinct; insert this data into the table _Asset_Router_Locations_.

In each case, it will initiate an Airflow DAG that back-syncs the data from Databridge-V2 to Oracle and uploads the data to AGO. 

None of these tables are geo-registered with ArcGIS Pro - they will not have _objectid_ fields.

_Routers_ repository: 
https://github.com/CityOfPhiladelphia/phillyvotes-routers

**This is sensitive data that should not be shared publicly.**

## Downstream dbt projects
* [cco/assets](https://github.com/CityOfPhiladelphia/dbt-airflow/tree/main/cco/assets)
* [cco/pollbooks](https://github.com/CityOfPhiladelphia/dbt-airflow/tree/main/cco/pollbooks)
    * Also triggers its dag so as to have a higher frequency than the Airflow migrated trigger dag frequency of 15 minutes

## Usage
The Jenkins automation server will run this pipeline every 10 minutes: 
* `python run.py` runs the script after having created and sourced a virtual environment. Options: 
    * `--test` uses test database credentials and will not trigger DAGs or upload to SFTP.
    * `--log=<value>` changes the logging mode; choose one of ['error', 'warn', 'info', 'debug']
    * `--drop` will drop the tables for the database whose credentials are used. **Warning: Destructive**
    * `--run_local` - Run this script on a local machine outside of our AWS environment. See below

In its current format, the script can only be run by OIT CityGeo because it depends on access to CityGeo's Keeper password management account. 

![Pipeline](<./Pipeline Diagram.PNG>)

## Notes

### Assets
The API access token for the visium API expires every 15 days; the script is capable of automatically detecting an expired token, generating a new one, then successfully processing the data. 

Technically, requesting a new token doesn't seem to invalidate old credentials, which will instead expire on their own schedule after 15 days. When you request a new token, you're simply doing that - requesting a new, valid token

### Asset_History
> **Q**:  Can you give me a little more detail about how the _asset history_ API relates to the _assets_ API?  
**A**: _Asset History_ API saves all data of _Asset Observations_, hence it is updated more frequently (including if the asset is reported on the same location multiple times), whereas the _Asset_ API retrieves only the "LastSeen" data and updates only periodically if the asset is continuously seen at one location, and additionally only if it moves to another location.

*Assets* "lastseentime" and the *Asset_History* "lastseentime" do not correspond exactly. Do not expect to see records from the former in the latter. Both tables may receive new records that are different from existing records only by the "lastseentime".  

_Asset_History_ is processed using multithreading for efficiency. With single-threading, the script could process the history of roughly 1,200 - 1,800 in 10 minutes. With up to 20 threads (the maximum recommended by InThing, the owner of Visium, employees), the script can now process the history of roughly 7,400 - 8,000 in 10 minutes, an increase of 4x-6x. 

### Repository Updates
This repository will automatically update `api_update.timestamp` to easily show when the latest Visium API Token was generated. 

This relies on a git remote URL called "automated", a deploy key located in this repository for EC2-User, and matching information in EC2-User's .ssh config file for the server this script runs on (currently linux-scripts-aws3). For more information, run: 
* `git remote -v`
* `code ~/.ssh/config`
* `ls ~/.ssh` (as EC2-User)

As a normal user, you can run `git push` or `git pull` as normal to pull from the origin url. EC2-User can _only_ run `git push automated main` and `git pull automated main` because that is the only URL they have access to. 

### Timezone
The Oracle database's time zone is set to `UTC +0000`. This means that all timestamp columns will by default display in that timezone (4 or 5 hours ahead of US-Eastern depending on daylight savings time). To see the timestamp columns in your local time zone, use the following query in Oracle: 

```SQL
-- Display timestamp fields in local timezone
SELECT a.* ,
    a.LASTSEENTIME AT LOCAL AS LASTSEENTIME_LOCAL , 
    a.UPDATED_ON AT LOCAL AS updated_on_local 
FROM GIS_ELECTIONS.ASSETS a 
ORDER BY a.UPDATED_ON DESC;

-- To check your local session timezone
SELECT SESSIONTIMEZONE FROM DUAL;

-- To set your local session timezone if results are unexpected
ALTER SESSION SET TIME_ZONE = 'America/New_York';
```

This is not a concern in the Databridge-V2 postgres database or in the excel file upload to SFTP which by default are in US-Eastern timezone. 

## Files
### Assets Files
* `run.sh` - Main bash script file that runs `run.py` and pushes any updates for `api_update.timestamp` to GitHub
    * See _Repository Updates_ above
* `run.py` - Main script file
* `dag_trigger.py` - File to trigger the Airflow pipeline dag with Keeper-related functions
* `assetdetails.py` - API and SFTP-related functions
* `utils.py` - Miscellaneous utility functions
* `config.py` - Configuration information
* `models.py` - Database table definition file using [peewee ORM](https://docs.peewee-orm.com/en/latest/index.html)
* `api_update.timestamp` - File to track the latest Assets API Token Reset
* `requirements.txt` - Python module requirements
* `extract_postgres_load_oracle.sh` - bash script to extract from postgres and load to Oracle manually. This script was only run to alleviate any concerns about uploading to AGO

### Asset_History Files
* `run_asset_history.py` - Main python file, triggered by `run.py`

### Asset_Router_Locations Files
* `run_asset_router_locations.py` - Main script file to join _assets_ and _routers_ tables. This is triggered by `run.py`
* `config_db.py` - Database configuration information
* `asset_router_locations.sql` - SQL file with some useful data analysis; not needed by scripts. 

## Running This Script locally
If you need to run this script locally, which hopefully you will never need to, then you must perform the following steps: 
1. Install python packages: 
    * For Linux or Windows, see `requirements.txt`
1. You may encounter SSLCertVerificationErrors in pip. To bypass that, either
    1. Get off the City network and off the VPN, or 
    1. Run `pip install --trusted-host pypi.org <package(s)>`
1. Pass `--run_local` to `routers.py`, which will: 
    1. All network requests (to Keeper, to APIs) will run _without_ verifying SSL certs
    1. Warnings about the lack of SSL certificate verification will be suppressed
    1. The databridge-v2 host will go to the pgbouncer address rather than directly to the server
    
## API Documentation
Documentation regarding the API V2 is available: 
1. Go to the Keeper secrets
1. Open up the "api_documentation_url" and input the "api_documentation_username" and "api_documentation_password" from the secret
1. Search for _/api/item/items-overview-report/data_

Each API instance (_assets_ being separate from _assets_history_) has a rate limit of 200 calls per minute according to communications with InThing, the owner of Visium.
