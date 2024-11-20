import sqlalchemy as sa, logging
import config as conf


def create_engine(creds: dict, test: bool, run_local: bool) -> sa.Engine:
    '''Compose the URL object, create engine, and test connection'''
    if run_local: 
        if test: 
            db_creds = creds[conf.DB_SECRET_LOCAL_TEST]
        else: 
            db_creds = creds[conf.DB_SECRET_LOCAL]
    elif test: 
        db_creds = creds[conf.DB_SECRET_HOST_TEST]
    else: 
        db_creds = creds[conf.DB_SECRET_HOST]
    creds_schema = creds[conf.DB_SECRET_LOGIN]
    url_object = sa.URL.create(
        drivername='postgresql+psycopg',
        username=creds_schema['login'],
        password=creds_schema['password'],
        host=db_creds['host'],
        port=db_creds['port'],
        database=db_creds['database']
    )
    engine = sa.create_engine(url_object)
    engine.connect()
    return engine


def setup_db_tables(engine: sa.Engine, metadata: sa.MetaData, drop: bool):
    '''Possibly drop and re-create tables in database'''
    if drop:
        metadata.drop_all(bind=engine, checkfirst=True)
        logger.info(
            f'DROP IF EXISTS for tables {list(metadata.tables.keys())} executed\n')
    metadata.create_all(bind=engine, checkfirst=True)
    logger.info(
        f'CREATE IF NOT EXISTS for tables {list(metadata.tables.keys())} executed\n')


# Define connection and table info
global logger
logger = logging.getLogger('main')

metadata = sa.MetaData()
asset_router_locations = sa.Table(
    'asset_router_locations', metadata,
    sa.Column("asset_precinct", sa.String(5)) , 
    sa.Column("asset_id", sa.String(255)) , 
    sa.Column("asset_itemname", sa.String(255)) ,
    sa.Column("asset_description", sa.String(255)) ,
    sa.Column("asset_serial", sa.String(255)) ,
    sa.Column("asset_manufacturer", sa.String(255)) ,
    sa.Column("asset_model", sa.String(255)) ,
    sa.Column("asset_itemclass", sa.String(255)) ,
    sa.Column("asset_itemtype", sa.String(255)) ,
    sa.Column("asset_owner", sa.String(255)) ,
    sa.Column("asset_lastseenlocation", sa.String(255)) ,
    sa.Column("asset_lastseenperson", sa.String(255)) ,
    sa.Column("asset_lastseentime", sa.TIMESTAMP(timezone=True)) ,
    sa.Column("router_account_id", sa.String(255)) ,
    sa.Column("router_actual_firmware_id", sa.String(255)) ,
    sa.Column("router_asset_id", sa.String(255)) ,
    sa.Column("router_config_status", sa.String(255)) ,
    sa.Column("router_created_at", sa.TIMESTAMP(timezone=True)) ,
    sa.Column("router_custom1", sa.String(255)) ,
    sa.Column("router_custom2", sa.String(255)) ,
    sa.Column("router_description", sa.String(255)) ,
    sa.Column("router_device_type", sa.String(255)) ,
    sa.Column("router_full_product_name", sa.String(255)) ,
    sa.Column("router_group__id", sa.String(255)) ,
    sa.Column("router_id", sa.String(255)) , 
    sa.Column("router_ipv4_address", sa.String(255)) ,
    sa.Column("router_locality", sa.String(255)) ,
    sa.Column("router_mac", sa.String(255)) ,
    sa.Column("router_name", sa.String(255)) ,
    sa.Column("router_product_id", sa.String(255)) ,
    sa.Column("router_reboot_required", sa.Integer()) ,
    sa.Column("router_serial_number", sa.String(255)) ,
    sa.Column("router_state", sa.String(255)) ,
    sa.Column("router_state_updated_at", sa.TIMESTAMP(timezone=True)) ,
    sa.Column("router_target_firmware_id", sa.String(255)) ,
    sa.Column("router_updated_at", sa.TIMESTAMP(timezone=True)) ,
    sa.Column("router_upgrade_pending", sa.Integer()) ,
    sa.Column("router_location_accuracy", sa.String(255)) ,
    sa.Column("rtr_location_altitude_meters", sa.String(255)) ,
    sa.Column("router_location_id", sa.String(255)) ,
    sa.Column("router_location_latitude", sa.String(255)) ,
    sa.Column("router_location_longitude", sa.String(255)) ,
    sa.Column("router_location_method", sa.String(255)) ,
    sa.Column("router_location_updated_at", sa.String(255)), 
    sa.Column("polling_places_placename", sa.String(255)), 
    schema=conf.SCHEMA
)

asset_history_columns = [
    sa.Column("id", sa.String(255)),
    sa.Column("tagepc", sa.String(255)),
    sa.Column("lastseenlocationname", sa.String(255)),
    sa.Column("lastseenpersonfullname", sa.String(255)),
    sa.Column("lastseentime", sa.TIMESTAMP(timezone=True)), 
    sa.Column("updated_on", sa.TIMESTAMP(timezone=True)) # CityGeo
]

asset_history = sa.Table(
    'asset_history', metadata,
    *asset_history_columns,
    schema=conf.SCHEMA
)
