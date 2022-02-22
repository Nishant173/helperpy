from typing import List, Optional, Tuple, Union

from datetime import (
    date,
    datetime,
    timedelta,
)

import pandas as pd
from randomtimestamp import randomtimestamp

from helperpy.core.exceptions import (
    raise_exception_if_invalid_option,
    raise_exception_if_invalid_type,
)


DATE_STRING_FORMAT = "%Y-%m-%d" # Eg: "2020-03-19"
DATETIME_STRING_FORMAT = "%Y-%m-%d %H:%M:%S" # Eg: "2020-03-19 17:45:08"
TIMESTAMP_INTEGER_FORMAT = "%Y%m%d%H%M%S" # Will be converted to an integer after parsing


class TimeUnitConverter:
    """Class for time related unit conversions"""
    SECONDS_PER_MINUTE = 60
    SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60
    SECONDS_PER_DAY = SECONDS_PER_HOUR * 24
    SECONDS_PER_WEEK = SECONDS_PER_DAY * 7
    SECONDS_PER_NON_LEAP_YEAR = SECONDS_PER_DAY * 365
    SECONDS_PER_LEAP_YEAR = SECONDS_PER_DAY * 366

    MINUTES_PER_HOUR = 60
    MINUTES_PER_DAY = MINUTES_PER_HOUR * 24
    MINUTES_PER_WEEK = MINUTES_PER_DAY * 7
    MINUTES_PER_NON_LEAP_YEAR = MINUTES_PER_DAY * 365
    MINUTES_PER_LEAP_YEAR = MINUTES_PER_DAY * 366

    HOURS_PER_DAY = 24
    HOURS_PER_WEEK = HOURS_PER_DAY * 7
    HOURS_PER_NON_LEAP_YEAR = HOURS_PER_DAY * 365
    HOURS_PER_LEAP_YEAR = HOURS_PER_DAY * 366



def get_current_timestamp_as_integer() -> int:
    """Returns current timestamp as an integer (Format: yyyymmddhhmmss)"""
    dt_obj = datetime.now()
    ts_now = dt_obj.strftime(TIMESTAMP_INTEGER_FORMAT)
    return int(ts_now)


def get_random_timestamp_as_integer() -> int:
    """Returns random timestamp as an integer (Format: yyyymmddhhmmss)"""
    ts_random = randomtimestamp(
        start_year=1900,
        end_year=datetime.now().year - 1,
        pattern=TIMESTAMP_INTEGER_FORMAT,
    )
    return int(ts_random)


def get_random_timestamp(
        start_year: Optional[int] = 1800,
        end_year: Optional[int] = 2200,
    ) -> datetime:
    return datetime.strptime(randomtimestamp(start_year=start_year, end_year=end_year), "%d-%m-%Y %H:%M:%S")


def period_to_datetime(period_obj: pd._libs.tslibs.period.Period) -> datetime:
    """Converts Pandas period object to datetime object"""
    dt_obj = datetime(
        year=period_obj.year,
        month=period_obj.month,
        day=period_obj.day,
        hour=period_obj.hour,
        minute=period_obj.minute,
        second=period_obj.second,
    )
    return dt_obj


def convert_to_datetime(
        date_string: str,
        hour: int,
        minute: int,
        second: int,
    ) -> datetime:
    """
    Parameters:
        - date_string (str): Date string of format "yyyy-mm-dd"
        - hour (int): Ranges from 0 to 23, indicating hour of the day
        - minute (int): Ranges from 0 to 59, indicating minute of the hour
        - second (int): Ranges from 0 to 59, indicating second of the minute
    """
    dt_obj = parse_date_string(date_string=date_string).replace(
        hour=hour,
        minute=minute,
        second=second,
    )
    return dt_obj


def get_day_of_week(
        dt_obj: Union[datetime, date],
        shorten: Optional[bool] = False,
    ) -> str:
    """
    Returns the day of the week.
    Day of week options when `shorten` is set to False: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'].
    Day of week options when `shorten` is set to True: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].
    """
    if shorten:
        return dt_obj.strftime("%a")
    return dt_obj.strftime("%A")


def to_date_string(dt_obj: datetime) -> str:
    """Converts datetime object to date string of format 'yyyy-mm-dd'"""
    date_string = dt_obj.strftime(DATE_STRING_FORMAT)
    return date_string


def to_datetime_string(dt_obj: datetime) -> str:
    """Converts datetime object to datetime string of format 'yyyy-mm-dd hh:mm:ss'"""
    datetime_string = dt_obj.strftime(DATETIME_STRING_FORMAT)
    return datetime_string


def parse_date_string(date_string: str) -> datetime:
    """
    Parses date string of format 'yyyy-mm-dd' into datetime object.
    >>> parse_date_string(date_string="2020-03-28")
    """
    dt_obj = datetime.strptime(date_string, DATE_STRING_FORMAT)
    return dt_obj


def parse_datetime_string(datetime_string: str) -> datetime:
    """
    Parses datetime string of format 'yyyy-mm-dd hh:mm:ss' into datetime object.
    Note: The 'hour' in the `datetime_string` must be of 24 hour format (from 0-23).
    >>> parse_datetime_string(datetime_string="2020-03-28 17:53:04")
    """
    dt_obj = datetime.strptime(datetime_string, DATETIME_STRING_FORMAT)
    return dt_obj


def ist_to_utc(dt_obj: datetime) -> datetime:
    """Subtracts 5 hours and 30 minutes from datetime object"""
    return dt_obj - timedelta(hours=5, minutes=30)


def utc_to_ist(dt_obj: datetime) -> datetime:
    """Adds 5 hours and 30 minutes to datetime object"""
    return dt_obj + timedelta(hours=5, minutes=30)


def convert_to_naive_timezone(dt_obj: datetime) -> datetime:
    """Converts datetime object to datetime object with a naive timezone"""
    dt_obj_naive_tz = dt_obj.replace(tzinfo=None)
    return dt_obj_naive_tz


def get_dates_between(
        start_date: str,
        end_date: str,
        as_type: Optional[str] = 'string',
    ) -> Union[List[datetime], List[date], List[str]]:
    """
    Returns list of dates between the given `start_date` and `end_date`.
    Input date-string format must be 'yyyy-mm-dd'.
    Options for `as_type` are: ['date', 'datetime', 'string']. Default: 'string'.
    """
    raise_exception_if_invalid_option(
        option_name='as_type',
        option_value=as_type,
        valid_option_values=['date', 'datetime', 'string'],
    )
    period_objs = pd.date_range(start=start_date, end=end_date, freq='1D')
    func_mapper = {
        'date': lambda period_obj: period_to_datetime(period_obj=period_obj).date(),
        'datetime': lambda period_obj: period_to_datetime(period_obj=period_obj),
        'string': lambda period_obj: to_date_string(dt_obj=period_to_datetime(period_obj=period_obj)),
    }
    return list(map(func_mapper[as_type], period_objs))


def offset_between_dates(
        start_date: str,
        end_date: str,
        offset_in_seconds: int,
    ) -> List[datetime]:
    """
    Returns list of datetime objects separated by the given offset (between the given date-range).

    >>> offset_between_dates(
            start_date="2019-05-15",
            end_date="2019-05-16",
            offset_in_seconds=60*60*8, # Equivalent of 8 hours
        )

    References:
        - [Pandas date range](https://pandas.pydata.org/docs/reference/api/pandas.date_range.html)
        - [Timeseries offset aliases](https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases)
    """
    raise_exception_if_invalid_type(
        parameter_name='offset_in_seconds', parameter_value=offset_in_seconds, expected_type=int,
    )
    if offset_in_seconds <= 0:
        raise ValueError(f"Expected `offset_in_seconds` to be > 0, but got {offset_in_seconds}")
    
    dt_end_date = parse_date_string(date_string=end_date)
    periods_objs = pd.period_range(
        start=start_date,
        end=to_date_string(dt_obj=dt_end_date + timedelta(days=1)),
        freq=f"{offset_in_seconds}S",
    )
    dt_objs = list(map(period_to_datetime, periods_objs))
    if dt_objs[-1] == dt_end_date + timedelta(days=1):
        dt_objs.pop() # Remove last item
    return dt_objs


def offset_between_datetimes(
        start_timestamp: str,
        end_timestamp: str,
        offset_in_seconds: int,
        include_start_timestamp: Optional[bool] = True,
        include_end_timestamp: Optional[bool] = True,
    ) -> List[datetime]:
    """
    Returns list of datetime objects separated by the given offset (between the given datetime-range).

    >>> offset_between_datetimes(
            start_timestamp="2019-05-17 13:30:00",
            end_timestamp="2019-05-17 22:30:00",
            offset_in_seconds=60*60*1, # Equivalent of 1 hour
        )

    References:
        - [Pandas date range](https://pandas.pydata.org/docs/reference/api/pandas.date_range.html)
        - [Timeseries offset aliases](https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases)
    """
    raise_exception_if_invalid_type(
        parameter_name='offset_in_seconds', parameter_value=offset_in_seconds, expected_type=int,
    )
    if offset_in_seconds <= 0:
        raise ValueError(f"Expected `offset_in_seconds` to be > 0, but got {offset_in_seconds}")
    
    periods_objs = pd.period_range(start=start_timestamp, end=end_timestamp, freq=f"{offset_in_seconds}S")
    dt_objs = list(map(period_to_datetime, periods_objs))
    if not include_start_timestamp and dt_objs:
        if start_timestamp == to_datetime_string(dt_obj=dt_objs[0]):
            dt_objs.pop(0)
    if not include_end_timestamp and dt_objs:
        if end_timestamp == to_datetime_string(dt_obj=dt_objs[-1]):
            dt_objs.pop(-1)
    return dt_objs



class DateWiseBucketer:
    """
    Class that provides functionality to get date-wise buckets of date-ranges.
    
    >>> dwb = DateWiseBucketer(
            date_string="2020-01-01",
            num_days_per_bucket=8,
            num_buckets=3,
        )
    >>> dwb.get_buckets(backward=False) # Returns [('2020-01-01', '2020-01-08'), ('2020-01-09', '2020-01-16'), ('2020-01-17', '2020-01-24')]
    >>> dwb.get_buckets(backward=True) # Returns [('2019-12-09', '2019-12-16'), ('2019-12-17', '2019-12-24'), ('2019-12-25', '2020-01-01')]
    """
    
    def __init__(
            self,
            date_string: str,
            num_days_per_bucket: int,
            num_buckets: int,
        ) -> None:
        """
        Parameters:
            - date_string (str): Date-string of format 'yyyy-mm-dd'
            - num_days_per_bucket (int): Number of days per bucket (must be > 0)
            - num_buckets (int): Number of buckets (must be > 0)
        """
        self.__raise_exception_if_invalid_input(
            date_string=date_string,
            num_days_per_bucket=num_days_per_bucket,
            num_buckets=num_buckets,
        )
        self.date_string = date_string
        self.num_days_per_bucket = num_days_per_bucket
        self.num_buckets = num_buckets
    
    def __raise_exception_if_invalid_input(
            self,
            date_string: str,
            num_days_per_bucket: int,
            num_buckets: int,
        ) -> None:
        """Raises an exception if any of the given inputs are invalid; otherwise returns None"""
        # Type errors
        raise_exception_if_invalid_type(
            parameter_name='date_string', parameter_value=date_string, expected_type=str,
        )
        raise_exception_if_invalid_type(
            parameter_name='num_days_per_bucket', parameter_value=num_days_per_bucket, expected_type=int,
        )
        raise_exception_if_invalid_type(
            parameter_name='num_buckets', parameter_value=num_buckets, expected_type=int,
        )

        # Value errors
        try:
            _ = datetime.strptime(date_string, DATE_STRING_FORMAT)
        except ValueError:
            raise ValueError(f"Expected `date_string` to be a valid date string of format 'yyyy-mm-dd', but got '{date_string}'")
        if num_days_per_bucket <= 0:
            raise ValueError(f"Expected `num_days_per_bucket` to be > 0, but got {num_days_per_bucket}")
        if num_buckets <= 0:
            raise ValueError(f"Expected `num_buckets` to be > 0, but got {num_buckets}")
        return None
    
    def __str__(self) -> str:
        return f"DateWiseBucketer(date_string='{self.date_string}', num_days_per_bucket={self.num_days_per_bucket}, num_buckets={self.num_buckets})"
    
    def get_buckets(
            self,
            backward: Optional[bool] = False,
            as_type: Optional[str] = 'string',
        ) -> Union[List[Tuple[datetime, datetime]], List[Tuple[date, date]], List[Tuple[str, str]]]:
        """
        Returns list of tuples of (start_date, end_date) buckets. They will always be in ascending order.
        Options for `as_type` are: ['date', 'datetime', 'string']. Default: 'string'.
        """
        raise_exception_if_invalid_option(
            option_name='as_type',
            option_value=as_type,
            valid_option_values=['date', 'datetime', 'string'],
        )
        freq = f"-{self.num_days_per_bucket}D" if backward else f"{self.num_days_per_bucket}D"
        period_objs = pd.date_range(start=self.date_string, freq=freq, periods=self.num_buckets)
        if backward:
            dt_bucket_end_dates = sorted(list(map(period_to_datetime, period_objs)), reverse=False)
            dt_bucket_start_dates = list(map(lambda dt_obj: dt_obj - timedelta(days=self.num_days_per_bucket - 1), dt_bucket_end_dates))
        else:
            dt_bucket_start_dates = sorted(list(map(period_to_datetime, period_objs)), reverse=False)
        buckets = list(map(lambda dt_obj: (dt_obj, dt_obj + timedelta(days=self.num_days_per_bucket - 1)), dt_bucket_start_dates))
        if as_type == 'date':
            buckets = list(map(lambda bucket: (bucket[0].date(), bucket[1].date()), buckets))
        elif as_type == 'string':
            buckets = list(map(lambda bucket: (to_date_string(bucket[0]), to_date_string(bucket[1])), buckets))
        return buckets



class MonthWiseBucketer:
    """
    Class that provides functionality to get month-wise buckets of date-ranges.
    
    >>> mwb = MonthWiseBucketer(
            year=2020,
            month=1,
            num_buckets=3,
        )
    >>> mwb.get_buckets(backward=False) # Returns [('2020-01-01', '2020-01-31'), ('2020-02-01', '2020-02-29'), ('2020-03-01', '2020-03-31')]
    >>> mwb.get_buckets(backward=True) # Returns [('2019-11-01', '2019-11-30'), ('2019-12-01', '2019-12-31'), ('2020-01-01', '2020-01-31')]
    """

    def __init__(
            self,
            year: int,
            month: int,
            num_buckets: int,
        ) -> None:
        """
        Parameters:
            - year (int): Year as integer
            - month (int): Month as integer (Range: 1-12)
            - num_buckets (int): Number of buckets (must be > 0)
        """
        self.__raise_exception_if_invalid_input(
            year=year,
            month=month,
            num_buckets=num_buckets,
        )
        self.year = year
        self.month = month
        self.num_buckets = num_buckets
    
    def __raise_exception_if_invalid_input(
            self,
            year: int,
            month: int,
            num_buckets: int,
        ) -> None:
        """Raises an exception if any of the given inputs are invalid; otherwise returns None"""
        # Type errors
        raise_exception_if_invalid_type(
            parameter_name='year', parameter_value=year, expected_type=int,
        )
        raise_exception_if_invalid_type(
            parameter_name='month', parameter_value=month, expected_type=int,
        )
        raise_exception_if_invalid_type(
            parameter_name='num_buckets', parameter_value=num_buckets, expected_type=int,
        )
        
        # Value errors
        if year <= 0:
            raise ValueError(f"Expected `year` to be > 0, but got {year}")
        if not (1 <= month <= 12):
            raise ValueError(f"Expected `month` to be in range 1-12, but got {month}")
        if num_buckets <= 0:
            raise ValueError(f"Expected `num_buckets` to be > 0, but got {num_buckets}")
        return None
    
    def __str__(self) -> str:
        return f"MonthWiseBucketer(year={self.year}, month={self.month}, num_buckets={self.num_buckets})"
    
    def get_buckets(
            self,
            backward: Optional[bool] = False,
            as_type: Optional[str] = 'string',
        ) -> Union[List[Tuple[datetime, datetime]], List[Tuple[date, date]], List[Tuple[str, str]]]:
        """
        Returns list of tuples of (start_date, end_date) buckets. They will always be in ascending order.
        Options for `as_type` are: ['date', 'datetime', 'string']. Default: 'string'.
        """
        raise_exception_if_invalid_option(
            option_name='as_type',
            option_value=as_type,
            valid_option_values=['date', 'datetime', 'string'],
        )
        period_objs = pd.date_range(
            start=datetime(year=self.year, month=self.month, day=1).strftime(DATE_STRING_FORMAT),
            freq="-1M" if backward else "1M",
            periods=self.num_buckets,
        )
        dt_bucket_end_dates = sorted(list(map(period_to_datetime, period_objs)), reverse=False)
        buckets = list(map(lambda dt_obj: (dt_obj.replace(day=1), dt_obj), dt_bucket_end_dates))
        if as_type == 'date':
            buckets = list(map(lambda bucket: (bucket[0].date(), bucket[1].date()), buckets))
        elif as_type == 'string':
            buckets = list(map(lambda bucket: (to_date_string(bucket[0]), to_date_string(bucket[1])), buckets))
        return buckets



class SecondWiseBucketer:
    """
    Class that provides functionality to get second-wise buckets of datetime-ranges.
    
    >>> swb = SecondWiseBucketer(
            start_timestamp="2019-05-18 17:00:00",
            end_timestamp="2019-05-18 17:20:00",
            offset_in_seconds=60*5, # Equivalent of 5 minutes
        )
    >>> swb.get_buckets() # Returns [('2019-05-18 17:00:00', '2019-05-18 17:05:00'), ('2019-05-18 17:05:00', '2019-05-18 17:10:00'), ('2019-05-18 17:10:00', '2019-05-18 17:15:00'), ('2019-05-18 17:15:00', '2019-05-18 17:20:00')]
    """

    def __init__(
            self,
            start_timestamp: str,
            end_timestamp: str,
            offset_in_seconds: int,
        ) -> None:
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.offset_in_seconds = offset_in_seconds
    
    def __str__(self) -> str:
        return f"SecondWiseBucketer(start_timestamp='{self.start_timestamp}', end_timestamp='{self.end_timestamp}', offset_in_seconds={self.offset_in_seconds})"
    
    def get_buckets(self, as_type: Optional[str] = 'string') -> Union[List[Tuple[datetime, datetime]], List[Tuple[str, str]]]:
        """
        Returns list of tuples of (start_timestamp, end_timestamp) buckets. They will always be in ascending order.
        Options for `as_type` are: ['datetime', 'string']. Default: 'string'.
        """
        raise_exception_if_invalid_option(
            option_name='as_type',
            option_value=as_type,
            valid_option_values=['datetime', 'string'],
        )
        timestamps = offset_between_datetimes(
            start_timestamp=self.start_timestamp,
            end_timestamp=self.end_timestamp,
            offset_in_seconds=self.offset_in_seconds,
            include_start_timestamp=True,
            include_end_timestamp=True,
        )
        if as_type == 'string':
            timestamps = list(map(to_datetime_string, timestamps))
        buckets = [(timestamps[idx], timestamps[idx + 1]) for idx in range(len(timestamps) - 1)]
        return buckets