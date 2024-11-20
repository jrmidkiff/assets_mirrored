import config
import playhouse.postgres_ext as pwp # Extension to peewee module


def init_db(creds: dict, test: bool, run_local: bool, database: pwp.PostgresqlExtDatabase) -> pwp.PostgresqlExtDatabase: 
    '''Initialize and return the database at run-time'''
    schema_creds = creds[config.DB_SECRET_LOGIN]
    if run_local: 
        if test: 
            db_creds = creds[config.DB_SECRET_LOCAL_TEST]
        else: 
            db_creds = creds[config.DB_SECRET_LOCAL]
    elif test: 
        db_creds = creds[config.DB_SECRET_HOST_TEST]
    else: 
        db_creds = creds[config.DB_SECRET_HOST]
    database.init(
        database=db_creds['database'],
        user=schema_creds['login'],
        password=schema_creds['password'],
        host=db_creds['host'],
        port=db_creds['port'], 
        sslmode='require'
    )
    return database

blank_db = pwp.PostgresqlExtDatabase(None) # Defer initialization of database until run-time - see run.py

class BaseModel(pwp.Model):
    itemname = pwp.CharField(null=True)
    description = pwp.CharField(null=True)
    serial = pwp.CharField(null=True)
    manufacturer = pwp.CharField(null=True)
    model = pwp.CharField(null=True)
    itemclass = pwp.CharField(null=True)
    itemtype = pwp.CharField(null=True)
    owner = pwp.CharField(null=True)
    lastseenlocation = pwp.CharField(null=True)
    lastseenperson = pwp.CharField(null=True)
    lastseentime = pwp.DateTimeTZField(null=True)
    id = pwp.CharField(max_length=36, primary_key=True)  # 'id' is the string key
    # totalcount = pwp.IntegerField(null=True) # No longer in use in order to not send every ID to run_asset_history.py when one asset is added and this count changes
    updated_on = pwp.DateTimeTZField(null=False)
    precinct = pwp.CharField(max_length=5, null=True)  # CityGeo added

    class Meta:
        database = blank_db


class Asset(BaseModel):
    class Meta:
        schema = config.SCHEMA
        table_name = 'assets'


class Asset_Temp(BaseModel):
    class Meta:
        table_name = 'assets_temp'
