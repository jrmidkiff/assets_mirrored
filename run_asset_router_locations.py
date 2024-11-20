import sqlalchemy as sa, logging
import config as conf, utils
from config_db import asset_router_locations, metadata, create_engine, setup_db_tables
import citygeo_secrets as cgs


def get_rowcount(db_conn: sa.Connection, table: sa.Table) -> int: 
    '''Get the current row count of a table'''
    stmt = sa.select(sa.func.count()).select_from(table)
    result = db_conn.execute(stmt)
    rowcount = result.scalar()
    return rowcount


def join_assets_routers(assets: sa.Table, routers: sa.Table, 
        router_precincts: sa.Table, asset_router_locations: sa.Table, conn: sa.Connection): 
    '''Join Assets and Routers table together by precinct via router_precincts'''
    pass
    stmt_select = (
        sa.Select(
            assets.c.precinct.label("asset_precinct") , 
            assets.c.id.label("asset_id") , 
            assets.c.itemname.label("asset_itemname") , 
            assets.c.description.label("asset_description") , 
            assets.c.serial.label("asset_serial") , 
            assets.c.manufacturer.label("asset_manufacturer") , 
            assets.c.model.label("asset_model") , 
            assets.c.itemclass.label("asset_itemclass") , 
            assets.c.owner.label("asset_owner") , 
            assets.c.lastseenlocation.label("asset_lastseenlocation") , 
            assets.c.lastseenperson.label("asset_lastseenperson") , 
            assets.c.lastseentime.label("asset_lastseentime") , 
            routers.c.account_id.label("router_account_id") , 
            routers.c.actual_firmware_id.label("router_actual_firmware_id") , 
            routers.c.asset_id.label("router_asset_id") , 
            routers.c.config_status.label("router_config_status") , 
            routers.c.created_at.label("router_created_at") , 
            routers.c.custom1.label("router_custom1") , 
            routers.c.custom2.label("router_custom2") , 
            routers.c.description.label("router_description") , 
            routers.c.device_type.label("router_device_type") , 
            routers.c.full_product_name.label("router_full_product_name") , 
            routers.c.group__id.label("router_group__id") , 
            routers.c.id.label("router_id") , 
            routers.c.ipv4_address.label("router_ipv4_address") , 
            routers.c.locality.label("router_locality") , 
            routers.c.mac.label("router_mac") , 
            routers.c.name.label("router_name") , 
            routers.c.product_id.label("router_product_id") , 
            routers.c.reboot_required.label("router_reboot_required") , 
            routers.c.serial_number.label("router_serial_number") , 
            routers.c.state.label("router_state") , 
            routers.c.state_updated_at.label("router_state_updated_at") , 
            routers.c.target_firmware_id.label("router_target_firmware_id") , 
            routers.c.updated_at.label("router_updated_at") , 
            routers.c.upgrade_pending.label("router_upgrade_pending") , 
            routers.c.location_accuracy.label("router_location_accuracy") , 
            routers.c.location_altitude_meters.label("rtr_location_altitude_meters") , 
            routers.c.location_id.label("router_location_id") , 
            routers.c.location_latitude.label("router_location_latitude") , 
            routers.c.location_longitude.label("router_location_longitude") , 
            routers.c.location_method.label("router_location_method") , 
            routers.c.location_updated_at.label("router_location_updated_at"), 
            routers.c.polling_places_placename.label("polling_places_placename")
        )
        .select_from(assets)
        .join(router_precincts, assets.c.precinct == router_precincts.c.precinct, isouter=True)
        .join(routers, router_precincts.c.id == routers.c.id, isouter=True)
        .order_by(assets.c.precinct, routers.c.name, assets.c.itemname)
        .subquery()
    )
    stmt = asset_router_locations.insert().from_select(stmt_select.c, stmt_select)
    result = conn.execute(stmt)
    updated_rowcount = get_rowcount(conn, asset_router_locations)
    utils.print_sa_stmt(stmt, updated_rowcount)    


def main(test: bool = False, run_local:bool=False, drop: bool = False): 
    '''Update asset_router_locations table
    - `test`: Whether to use test credentials
    - `run_local`: If True, do not validate SSL certificates
    - `drop`: Whether to drop tables. DESTRUCTIVE. '''
    global logger
    logger = logging.getLogger('main')

    logger.info(f'{"*" * 80}')
    logger.info('Beginning Run Asset Router Locations script\n')
    logger.info(f'Test mode: {test}')
    engine = cgs.connect_with_secrets(create_engine, 
        conf.DB_SECRET_HOST, conf.DB_SECRET_HOST_TEST, 
        conf.DB_SECRET_LOCAL, conf.DB_SECRET_LOCAL_TEST, 
        conf.DB_SECRET_LOGIN,  
        test=test, run_local=run_local)
    
    # assets, routers, and router_precincts tables must already exist for this script to run
    assets = sa.Table("assets", metadata, schema=conf.SCHEMA, autoload_with=engine) 
    routers = sa.Table("routers", metadata, schema=conf.VIEWER_SCHEMA, autoload_with=engine)
    router_precincts = sa.Table("router_precincts", metadata, schema=conf.VIEWER_SCHEMA, autoload_with=engine)

    setup_db_tables(engine, metadata, drop)
    
    with engine.begin() as conn: 
        stmt = sa.delete(asset_router_locations)  
        result = conn.execute(stmt)
        utils.print_sa_stmt(stmt, result.rowcount)

        join_assets_routers(assets, routers, router_precincts, asset_router_locations, conn)
