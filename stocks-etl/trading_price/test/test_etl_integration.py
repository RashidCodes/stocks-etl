from trading_price.etl.extract import Extract 
from trading_price.etl.transform import Transform 
import os


def test_extract_transformation_integration():

    # assemble 

    api_key_id = os.environ.get("api_key_id")
    api_secret_key = os.environ.get("api_secret_key")


    df = Extract.extract(
        stock_ticker = "tsla",
        api_key_id = api_key_id,
        api_secret_key = api_secret_key,
        start_date = '2020-01-01',
        end_date = '2020-01-02'
    )

    df_exchange_codes = Extract.extract_exchange_codes("trading_price/data/exchange_codes.csv")


    # act 
    transform_df = Transform.transform(
        df=df,
        df_exchange_codes = df_exchange_codes 
    )

    # assert 
    transform_df.iloc[0][["exchange", "price"]].to_dict() == {"exchange": "Cboe EDGX", 'price': 418.93}
