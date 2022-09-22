from trading_price.etl.transform import Transform 
from pandas import DataFrame, testing




def test_transform():

    # assemble 
    input_df = DataFrame({
        "t": ["123", "124"],
        "x": ["A", "B"],
        "p": [10.1, 20.1],
        "s": [10, 20]
    })


    exchange_codes_df = DataFrame({
        "exchange_code": ["A", "B"],
        "exchange_name": ["NYSE American (AMEX)", "NASDAQ OMX BX"]
    }) 

    
    expected_df = DataFrame({
        "timestamp": ["123", "124"],
        "exchange": ["NYSE American (AMEX)", "NASDAQ OMX BX"],
        "price": [10.1, 20.1],
        "size": [10, 20]
    })


    # act 
    output_df = Transform.transform(input_df, df_exchange_codes=exchange_codes_df)

    # assert 
    testing.assert_frame_equal(output_df, expected_df, check_exact=True)

