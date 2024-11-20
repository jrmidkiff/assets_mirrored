from assetdetails import get_asset_data, upload_to_sftp
import config, utils, run_asset_router_locations, run_asset_history, dag_trigger
from models import init_db, blank_db, Asset, Asset_Temp
import pandas as pd, numpy as np, click
import os, datetime, zoneinfo, logging, urllib3
import citygeo_secrets as cgs
from typing import Sequence
from paramiko.ssh_exception import NoValidConnectionsError


def prepare_df(asset_list: list) -> pd.DataFrame: 
    '''Prepare dataframe from data'''
    logger.info(f"{len(asset_list):,} assets found.")
    logger.info(f'Preparing records...\n')
    df = pd.DataFrame(asset_list)
    df.columns = [x.lower() for x in df.columns]
    
    # Drop any fields that come from the API but aren't defined in our table schema
    keep_fields = [] 
    asset_meta_fields = Asset._meta.fields
    for field in df.columns: 
        if field in asset_meta_fields: 
            keep_fields.append(field)
    df = df.loc[: , keep_fields]
    df['updated_on'] = datetime.datetime.now(tz=zoneinfo.ZoneInfo('US/Eastern'))
    df = extract_precinct(df)
    return df
    

def delete_removed_ids(df: pd.DataFrame) -> int: 
    '''Remove records that no longer appear in the API, returning count deleted'''
    ids_deleted = (Asset
                   .delete()
                   .where(Asset.id.not_in(df['id'].to_list()))
                   .returning(Asset.id)
                   .execute())
    count_deleted = len(ids_deleted)
    logger.info(f'Removed {count_deleted} IDs no longer in API\n')
    return count_deleted


def upsert(records: list, df: pd.DataFrame) -> Sequence[tuple[str]]: 
    '''Upsert API records into database, return ids updated/inserted'''
    logger.info(f'Inserting into temp table...\n')
    (Asset_Temp
        .insert_many(records, fields=list(df.columns))
        .execute())
    
    asset_intersect_fields = get_intersect_fields(
        df, Asset._meta.fields, ['updated_on'])
    asset_temp_intersect_fields = get_intersect_fields(
        df, Asset_Temp._meta.fields, ['updated_on'])

    logger.info(f'Determining new or updated records...\n')
    intersect_subquery = (
        Asset_Temp
        .select(*asset_temp_intersect_fields)
        .intersect(    
            Asset    
            .select(*asset_intersect_fields)))
    id_subquery = intersect_subquery.select_from(intersect_subquery.c.id)
    delete_query = Asset_Temp.delete().where(Asset_Temp.id.in_(id_subquery))
    delete_query.execute() # Delete unchanged records from temp table

    logger.info('Upserting records...\n')
    fields = list(Asset_Temp._meta.fields.values())
    upsert_query = (Asset.insert_from(Asset_Temp.select(), fields=fields)
        .on_conflict(
            conflict_target=[Asset.id], 
            preserve=fields)
        .returning(Asset.id))
    rv = upsert_query.execute() # Upsert only the changed records to maintain the "updated_on" field
    
    row_count = Asset.select().count()
    ids = rv.cursor.fetchall()
    count_upserted = len(ids)
    logger.info(f'{count_upserted:,} records inserted or updated since last API request')
    logger.info(f'{row_count:,} records now exist\n')
    return ids


def get_intersect_fields(df: pd.DataFrame, meta_fields: dict, ignore_fields: list[str]) -> list: 
    '''Return the peewee field types present in a dataframe
    - ignore_fields (list[str]): List of fields in dataframe to ignore
    '''
    intersect_fields = []
    for field in df.drop(columns=ignore_fields).columns: 
        peewee_field = meta_fields[field]
        intersect_fields.append(peewee_field)
    return intersect_fields


def correct_timezone(df: pd.DataFrame, column: str, timezone: str): 
    '''Change timezone columns from UTC to correct timezone and then remove timezone'''
    df[column] = df[column].dt.tz_convert(timezone)
    df[column] = df[column].dt.tz_localize(None) # Must remove tz to export to excel
    return df


def extract_precinct(df: pd.DataFrame) -> pd.DataFrame: 
    '''Extract precinct from data in the following priority: 
    1. itemname
    2. manufacturer & model
    '''
    df = pd.concat(
        [df, 
         df['itemname'].str.extract('(?P<ward>\d{1,2})\s*-\s*(?P<division>\d{1,2})')], # 1st ward-division match in column, only
        axis=1)
    df['ward'] = df['ward'].mask(df['ward'].isna(), df['manufacturer'].str.extract('(?P<ward>\d{1,2})')['ward'])
    df['division'] = df['division'].mask(df['division'].isna(), df['model'].str.extract('(?P<division>\d{1,2})')['division'])

    df['precinct'] = (
        df['ward'].str.pad(width=2, fillchar='0') + 
        '-' + 
        df['division'].str.pad(width=2, fillchar='0')
    )
    df = df.drop(columns=['ward', 'division'])
    return df


def trigger_dag(dagname: str, test: bool): 
    '''Trigger dag via subprocess'''
    if test: 
        logger.info(f'TEST mode - not triggering DAG {dagname}')
    else: 
        dag_trigger.main(dagname)
    print('DAG trigger complete')


@click.command
@click.option('--test',  is_flag=True, default=False, help='Run in test mode')
@click.option('--run_local', is_flag=True, default=False, help='Run this script on a local machine outside of AWS environment')
@click.option('--log', 
              type=click.Choice(['error', 'warn', 'info', 'debug'], case_sensitive=False), 
              default=None, help='Log level to use')
def main(test: bool, run_local: bool, log: str): 
    '''Entry point for Asset management process'''
    logging.basicConfig(format='%(levelname)s: %(message)s')
    if log == None: 
        log = 'debug' if test else 'info'
    cgs.set_config(keeper_dir='~')
    cgs.set_config(log_level=log)
    log_level = getattr(logging, log.upper(), None)
    global logger
    logger = logging.getLogger('main')
    logger.setLevel(level=log_level)

    logger.info(f'Start Process, log level = {log.upper()}, {test = }')
    if run_local: 
        logger.info(f'Running in "LOCAL MODE" - SSL certificate validation is turned off')
        cgs.set_config(verify_ssl_certs=False)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    database = cgs.connect_with_secrets(init_db, 
        config.DB_SECRET_HOST, config.DB_SECRET_HOST_TEST, 
        config.DB_SECRET_LOCAL, config.DB_SECRET_LOCAL_TEST, 
        config.DB_SECRET_LOGIN,  
        test=test, run_local=run_local, database=blank_db)
    
    timer = utils.SimpleTimer()

    asset_data = get_asset_data(run_local)
    if asset_data is not None:
        with database: 
            Asset.create_table(safe=True)
            Asset_Temp.create_table(temporary=True)

            df = prepare_df(asset_data['data'])
            count_deleted = delete_removed_ids(df=df)

            asset_list_insert = df.replace({np.nan: None}).to_records(index=False)
            ids_upserted = upsert(records=asset_list_insert, df=df)

            if count_deleted == 0 and len(ids_upserted) == 0: # Exit without triggering dag run if no records changed
                logger.info(f'No records were deleted or upserted - data is unchanged')
                logger.info(f'Not triggering any table updates, DAGs, or SFTP upload!\n')
                logger.info(timer.end())
                logger.info('Done!')
                exit(0)

        l = []
        for row in Asset.select().dicts(): # Get back the authoritative data from db
            l.append(row)
        df = pd.DataFrame(l)

        for timezone_col in ['lastseentime', 'updated_on']: 
            df = correct_timezone(df, timezone_col, 'US/Eastern')

        df.to_excel(config.FILE_NAME, sheet_name='Sheet1', index=False)
        if not test: 
            try: 
                upload_to_sftp(
                    local_filename=config.FILE_NAME, 
                    remote_filename=f'{config.SFTP_DIRECTORY}/{config.FILE_NAME}')
            except NoValidConnectionsError as e: 
                logger.error(f'Unable to upload to SFTP!')
                logger.error(e)
        else: 
            logger.info(f'TEST mode - Not uploading to SFTP')
        try: 
            os.remove(config.FILE_NAME)
            logger.info(f'Successfully removed local file copy\n')
        except FileNotFoundError: 
            logger.info(f'Unable to remove file {config.FILE_NAME}\n')

        trigger_dag(dagname=config.DAG_NAME_ASSETS, test=test)

        logger.info(timer.end())
        ids = [id[0] for id in ids_upserted]
        timer.start_lap()
        run_asset_history.update(ids, test=test, run_local=run_local)
        timer.end_lap()
        
        trigger_dag(dagname=config.DAG_NAME_ASSET_HISTORY, test=test)
        trigger_dag(dagname=config.DAG_NAME_POLLBOOK_LOCATIONS, test=test)
        
    else: 
        logger.info('No asset data found. Not updating asset history.\n')
    
    logger.info(timer.end())
    run_asset_router_locations.main(test=test, run_local=run_local)
    trigger_dag(dagname=config.DAG_NAME_ASSET_ROUTER_LOCATIONS, test=test)

    logger.info(timer.end())
    logger.info('Done!\n')


if __name__ == "__main__":
    main()
