import os 
import logging
import yaml
import datetime as dt
from database.postgres import PostgresDB 
from trading_price.etl.extract import Extract 
from trading_price.etl.transform import Transform
from trading_price.etl.load import Load 
from utility.date_time import DateTime
from io import StringIO 
from utility.metadata_logging import MetadataLogging 



def pipeline() -> bool:

    log_stream = StringIO()

    # set the stream on the basic config 
    logging.basicConfig(stream=log_stream, format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s")

    # instantiate the logger
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO) 

    
    try:

        # get yaml config 
        with open("trading_price/config.yaml") as stream:
            config = yaml.safe_load(stream)


        # get secret keys
        api_key_id = os.environ.get("api_key_id")
        api_secret_key = os.environ.get("api_secret_key")


        metadata_logger = MetadataLogging()

        metadata_logger_table = f"metadata_log_{config.get('load').get('database').get('target_table_name')}"
        metadata_logger_run_id = metadata_logger.get_latest_run_id(db_table=metadata_logger_table)
        
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="started",
            run_id=metadata_logger_run_id,
            run_config=config,
            db_table=metadata_logger_table,
        )

        


        logger.info("Commencing extraction")


        # get the dates 
        if config.get("extract").get("date_picker").lower() == "most_recent_weekday":

            start_date = DateTime.get_most_recent_weekday_from_today()
            end_date = DateTime.get_end_date(start_date=start_date, days_from_start=1)



        elif config.get("extract").get("date_picker").lower() == "date_range":

            start_date = config.get("extract").get("start_date")
            end_date = config.get("extract").get("end_date")


        elif config.get("extract").get("date_picker").lower() == "days_from_start":

            start_date = config.get("extract").get("start_date")
            days_from_start = config.get("extract").get("days_from_start")
            end_date = DateTime.get_end_date(start_date=start_date, days_from_start=days_from_start)



        elif config.get("extract").get("date_picker").lower() == "days_from_end":
            end_date = config.get("extract").get("end_date")
            days_from_end = config.get("extract").get("days_from_end")
            start_date = DateTime.get_start_date(end_date=end_date, days_from_end=days_from_end)


        else:
            logger.exception("Did not configure extract dates correctly")

        print(f"Date picker: {config.get('extract').get('date_picker')}")
        print(f"Start_date: {start_date}")
        print(f"End_date: {end_date}")




        # Extract the data 
        df = Extract.extract(
            stock_ticker=config.get("extract").get("stock_ticker"),
            api_key_id = api_key_id,
            api_secret_key = api_secret_key,
            start_date = start_date,
            end_date = end_date 
        )

            
        # get exchange codes 
        exchange_codes_file_path = f"{config.get('load').get('file').get('target_file_directory')}/exchange_codes.csv"
        df_exchange_codes = Extract.extract_exchange_codes(exchange_codes_file_path)

        logger.info("Extraction complete")


        logger.info("Commencing transformation")

        # transform the data 
        df_transform = Transform.transform(
            df = df,
            df_exchange_codes = df_exchange_codes 
        )

        logger.info("Tranformation complete")


        # load the file (upsert)
        logger.info("Commencing file load")

        Load.load(
            df = df_transform,
            load_target = config.get("load").get("file").get("load_target"),
            target_file_directory = config.get("load").get("file").get("target_file_directory"),
            target_file_name = config.get("load").get("file").get("target_file_name")
        )

        logger.info("File load complete")


        
        # create the engine for the postgresql database 
        engine = PostgresDB.create_pg_engine()


        # load to database 
        Load.load(
            df = df_transform,
            load_target = config.get("load").get("database").get("load_target"),
            target_database_engine = engine,
            target_table_name = config.get("load").get("database").get("target_table_name")
        )

        logger.info("Database load complete")



        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="completed",
            run_id=metadata_logger_run_id,
            run_config=config,
            db_table=metadata_logger_table,
            run_log=log_stream.getvalue()
        )

        print(f"log_stream value: {log_stream.getvalue()}")
        return True


    except BaseException as err:

        logger.exception(err)

        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="completed",
            run_id=metadata_logger_run_id,
            run_config=config,
            db_table=metadata_logger_table,
            run_log=log_stream.getvalue()
        )


        print(log_stream.getvalue())






if __name__ == "__main__":
    
    if pipeline():
        logging.info("Success")
