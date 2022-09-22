from pandas import DataFrame, merge 


class Transform():

    @staticmethod
    def transform(
        df: DataFrame,
        df_exchange_codes: DataFrame 
    ) -> DataFrame:

        """

        Performs transformation on the data produced from the extract() function.


        Parameters
        ----------

        df: DataFrame 
            Data frame containg the trades produced from the extract function 

        df_exchange_codes: DataFrame 
            Data frame containing information on the exchange codes



        Returns
        --------
        df_ask_bid_exchange_de_dup: DataFrame 
            Data frame containing de-duplicated data


        """ 

        # rename the columns
        df_quotes_renamed = df.rename(columns={
            "t": "timestamp",
            "x": "exchange",
            "p": "price",
            "s": "size"
        })


        df_quotes_selected = df_quotes_renamed[["timestamp", "exchange", "price", "size"]]

        # merge the trades and the exchange dataframe
        df_exchange = merge(left=df_quotes_selected, right=df_exchange_codes, left_on="exchange", right_on="exchange_code")
        

        # remove irrelevant exchange data
        df_exchange = df_exchange.drop(columns=["exchange_code", "exchange"])

        df_exchange = df_exchange.rename(columns = {
            "exchange_name": "exchange"
        }) 

        # find some aggregates 
        df_ask_bid_exchange_aggr = df_exchange.groupby(["timestamp", "exchange"]).agg({
            "price": "mean",
            "size": "sum"
        }).reset_index()


        return df_ask_bid_exchange_aggr

