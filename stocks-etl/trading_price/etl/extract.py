import requests
from pandas import DataFrame, json_normalize, read_csv
from utility.date_time import DateTime 




class Extract():

    """ Extract logic """


    @staticmethod 
    def extract(
        stock_ticker: str,
        api_key_id: str,
        api_secret_key: str,
        start_date: str,
        end_date: str 
    ) -> DataFrame:


        """

        Extract trades data from the ALPACA API 


        Parameters
        ----------

        stock_ticker: str 
            The ticker of a stock e.g. AAPL

        api_key_id: str 
            The api key id from Alpaca 

        api_secret_key: str 
            The api secret key from Alpaca 

        start_date: str
            date to begin extracting data from 

        end_date: str 
            date to stop extracting data to 



        Returns
        -------
        df: DataFrame 
            The dataframe containing ticker trades


        """ 


        base_url = f"https://data.alpaca.markets/v2/stocks/{stock_ticker}/trades"
        response_data = []

        for date in DateTime.generate_datetime_ranges(start_date=start_date, end_date=end_date):

            start_time = date.get("start_time")
            end_time = date.get("end_time") 


            params = {
                "start": start_time,
                "end": end_time 
            } 


            headers = {
                "APCA-API-KEY-ID": api_key_id,
                "APCA-API-SECRET-KEY": api_secret_key
            }

            response = requests.get(base_url, params=params, headers=headers)

            if response.status_code == 200:
                
                if response.json().get("trades") is not None:
                    response_data.extend(response.json().get("trades"))


        # read the json to a dataframe
        df = json_normalize(response_data, meta=["symbol"])

        return df 




    @staticmethod
    def extract_exchange_codes(fp: str) -> DataFrame:

        """ 

        Reads exchange codes CSV file and returns a dataframe

        
        Parameters 
        ----------

        fp: str 
            The filepath of the exchange codes CSV file 



        Returns
        -------
        df: DataFrame 
            Dataframe containing exchange codes 


        """ 

        df = read_csv(fp)

        return df
