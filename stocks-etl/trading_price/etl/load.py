import os 
from pandas import DataFrame, read_parquet, concat
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from sqlalchemy.dialects import postgresql 



class Load():

    @staticmethod 
    def load(
        df: DataFrame,
        load_target: str,
        load_method: str = 'upsert',
        target_file_directory: str = None,
        target_file_name: str = None,
        target_database_engine = None,
        target_table_name: str = None 
    ) -> None:

        """ 

        Load dataframe to either a file or a database 


        Parameters
        -----------

        df: DataFrame 
            dataframe to load

        load_target: str 
            choose either 'file' or 'database'

        load_method: str 
            Currently only "upsert" is supported

        target_file_directory: str 
            The directory where the file will be written to in the parquet format 

        target_file_name: str
            The name of the target fiile e.g. stock.parquet 

        target_database_engine:
            SQLAlchemy engine for the target database 

        target_table_name: str 
            The name of the SQL table to create and/or upsert data to 



        Returns
        -------


        """ 

        if load_target == "file":

            if load_method == "upsert":

                if target_file_name in os.listdir(f"{target_file_directory}/"):

                    # read the file in 
                    current_df = read_parquet(f"{target_file_directory}/{target_file_name}")
                    # get new data using the indexes 
                    df_concat = concat([current_df, df[~df.index.isin(current_df.index)]])

                    # save the data 
                    df_concat.to_parquet(f"{target_file_location}/{target_file_name}")


        elif load_target == "database":


            # create the data if it does not exist 
            meta = MetaData()

            stock_price_tesla_table = Table(
                target_table_name, meta,
                Column("timestamp", String, primary_key=True),
                Column("exchange", String, primary_key=True),
                Column("price", Float),
                Column("size", Integer)
            )


            meta.create_all(target_database_engine)

            insert_statement = postgresql.insert(stock_price_tesla_table).values(df.to_dict(orient='records'))

            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements = ["timestamp", "exchange"],
                set_= {c.key: c for c in insert_statement.excluded if c.key not in ["timestamp", "exchange"]})


            # execute the query 
            target_database_engine.execute(upsert_statement)


        else:
            raise Exception("The parameters passed in results in no action being performed")

