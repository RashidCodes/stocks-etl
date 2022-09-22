from sqlalchemy import create_engine
from sqlalchemy.engine import URL 
import os 



class PostgresDB():

    @staticmethod
    def create_pg_engine():
        db_user = os.environ.get("db_user")
        db_password = os.environ.get("db_password")
        db_server_name = os.environ.get("db_server_name")
        db_database_name = os.environ.get("db_database_name")


        # connect to the database 
        connection_url = URL.create(
            drivername = 'postgresql+pg8000',
            username = db_user,
            password = db_password,
            host = db_server_name,
            port = 5432,
            database = db_database_name 
        )

        engine = create_engine(connection_url)
        return engine
