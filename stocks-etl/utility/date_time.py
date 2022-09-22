import datetime as dt 
import logging


class DateTime():

    @staticmethod 
    def get_today() -> str:

        """ Return today's date in 'yyyy-mm-dd' format """
        return dt.date.today().strftime("%Y-%m-%d")


    @staticmethod 
    def get_most_recent_weekday_from_today() -> str:

        """

        Get the most recent weekday from today's date 

        """

        date = dt.date.today()

        while date.weekday() > 4:
            date -= dt.timedelta(days=1)

        return date.strftime("%Y-%m-%d")



    @staticmethod 
    def get_end_date(start_date: str = None, days_from_start: int = None) -> dt.date:

        """ 
        Get the end_date by counting forward from the start_date 


        Parameters
        ---------
        start_date: date 
            The start_date in 'yyyy-mm-dd' format


        days_from_start: int 
            The number of days from the start_date 



        Returns
        -------
        end_date: date 
            The end_date 

        """ 

        if start_date is not None and days_from_start is not None:
            parsed_start_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
            end_date = parsed_start_date + dt.timedelta(days=days_from_start)

            return end_date.strftime("%Y-%m-%d") 


        else:
            raise Exception("The parameters passed in results in no action being performed")




    @staticmethod
    def get_start_date(end_date: str = None, days_from_end: str = None) -> str:

        """ 
        
        Get the start_date by counting backwards from the end_date 


        Parameters
        ----------
        end_date: str 
            The end_date in 'yyyy-mm-dd' format 


        days_from_end: str
            The number of days before the start_date 



        Returns
        -------
        start_date: dt.date
            The start_date 


        """ 

        if end_date is not None and days_from_end is not None:
            parsed_end_date = dt.datetime.strptime(end_date, "%Y-%m-%d")
            start_date = parsed_end_date - dt.timedelta(days=days_from_end)

            return start_date.strftime("%Y-%m-%d")

        else:
            raise Exception("The parameters passed in results in no action being performed")







    @staticmethod
    def generate_datetime_ranges(
        start_date: str = None,
        end_date: str = None 
    ) -> list:

        """ 

        Generate a range of datetime ranges 

        Paramters
        ---------
        start_date: str 
            The start date in string format "yyyy-mm-dd"

        end_date: str 
            The end date in string format "yyyy-mm-dd"


        Returns
        -------
        date_range: list 
            A list of date ranges 



        Examples
        --------
        >> generate_datetime_ranges(start_date='2020-01-01', end_date='2022-01-02')

        [
            {'start_time': '2020-01-01T00:00:00.00Z', 'end_time': '2020-01-02T00:00:00.00Z'},
            {'start_time': '2020-01-01T00:00:00.00Z', 'end_time': '2020-01-02T00:00:00.00Z'}...
        ]

        """


        date_range = [] 

        if start_date is not None and end_date is not None:

           dte_start_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
           dte_end_date = dt.datetime.strptime(end_date, "%Y-%m-%d")

           date_range = [
                {
                    "start_time": (dte_start_date + dt.timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%S.00Z"),
                    "end_time": (dte_start_date + dt.timedelta(days=i) + dt.timedelta(days=1)).strftime("%Y-%m-%d")

                } 

                for i in range((dte_end_date - dte_start_date).days)
            ] 

           return date_range

        else:
            raise Exception("The parameters passed in results in no action being performed")





