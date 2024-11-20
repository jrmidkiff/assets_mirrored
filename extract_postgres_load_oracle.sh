#!/bin/bash

##### 
# This is a bash script
# Run `source <this_bash_script>` to extract from postgres and load to Oracle manually
# This script was only run to alleviate any concerns about uploading to AGO
#####
source venv/bin/activate # wherever citygeo_secrets & python are installed

# Writes out a file of environment variables 
# Note that 'databridge-oracle/hostname' requires two levels to access the host value
python -c "
import citygeo_secrets as cgs 
cgs.set_config(keeper_dir='~')
cgs.generate_env_file('keeper', 
    USER_POSTGRESQL = (
        'databridge-v2/postgres', 
        'login'), 
    PW_POSTGRESQL = (
        'databridge-v2/postgres', 
        'password'), 
    HOST_POSTGRESQL = (
        'databridge-v2/hostname', 
        'host'), 
    PORT_POSTGRESQL = (
        'databridge-v2/hostname', 
        'port'), 
    DB_POSTGRESQL = (
        'databridge-v2/hostname', 
        'database'), 
    USER_ORACLE = (
        'SDE', 
        'login'), 
    PW_ORACLE = (
        'SDE', 
        'password'), 
    HOST_ORACLE = (
        'databridge-oracle/hostname', 
        ['host', 'hostName']), 
    PORT_ORACLE = (
        'databridge-oracle/hostname', 
        ['host', 'port']), 
    DB_ORACLE = (
        'databridge-oracle/hostname', 
        'database'), 
    PROTOCOL_ORACLE = (
        'databridge-oracle/hostname', 
        'protocol'), 
    AWS_ACCESS_KEY_ID = (
        'Citygeo AWS Key Pair PROD', 
        'access_key'),
    AWS_SECRET_ACCESS_KEY = (
        'Citygeo AWS Key Pair PROD', 
        'secret_key')
    )
"
# After running this bash command, it will instruct you to copy three commands 
# similar to these:
ENV_VARS_FILE="/home/ubuntu/databridge-etl-tools/citygeo_secrets_env_vars.bash"
source $ENV_VARS_FILE
rm $ENV_VARS_FILE

# "asset_history" "asset_router_locations" "assets" "router_info" "router_locations" "routers"
myArray=("asset_history" "asset_router_locations" "assets" "router_info" "router_locations" "routers")

# Loop through the array and assign each element to the variable $table
for item in "${myArray[@]}"; do
    table="$item"
    table_uppercase="${table^^}"  # Convert to uppercase
    echo "Processing: $table"

    # Extract your postgres table
    databridge_etl_tools postgres  \
        --table_name=$table \
        --table_schema=citygeo  \
        --connection_string="postgresql://$USER_POSTGRESQL:$PW_POSTGRESQL@$HOST_POSTGRESQL:$PORT_POSTGRESQL/$DB_POSTGRESQL" \
        --s3_bucket=citygeo-airflow-databridge2  \
        --s3_key="staging/citygeo/$table-backwards-sync.csv" \
        extract

    # Load into oracle as SDE user
    databridge_etl_tools oracle  \
        --table_name=$table_uppercase  \
        --table_schema=GIS_ELECTIONS  \
        --connection_string="$USER_ORACLE/$PW_ORACLE@(DESCRIPTION=(ADDRESS=(PROTOCOL=$PROTOCOL_ORACLE)(HOST=$HOST_ORACLE)(PORT=$PORT_ORACLE))(CONNECT_DATA=(SID=$DB_ORACLE)))" \
        --s3_bucket=citygeo-airflow-databridge2  \
        --s3_key="staging/citygeo/$table-backwards-sync.csv"  \
        load 

done

deactivate
