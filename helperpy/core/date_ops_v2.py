from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, Union


DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f%z"

MONTHS_HAVING_LESS_THAN_30_DAYS = set([2])
MONTHS_HAVING_30_DAYS = set([4, 6, 9, 11])
MONTHS_HAVING_31_DAYS = set([1, 3, 5, 7, 8, 10, 12])
NUM_MONTHS_PER_YEAR = 12


class TimeUnitConverter:
    """Class for time-related conversions"""

    MICROSECONDS_PER_SECOND = 1_000_000
    MICROSECONDS_PER_MINUTE = MICROSECONDS_PER_SECOND * 60
    MICROSECONDS_PER_HOUR = MICROSECONDS_PER_MINUTE * 60
    MICROSECONDS_PER_DAY = MICROSECONDS_PER_HOUR * 24
    MICROSECONDS_PER_WEEK = MICROSECONDS_PER_DAY * 7
    MICROSECONDS_PER_NON_LEAP_YEAR = MICROSECONDS_PER_DAY * 365
    MICROSECONDS_PER_LEAP_YEAR = MICROSECONDS_PER_DAY * 366

    MILLISECONDS_PER_SECOND = 1000
    MILLISECONDS_PER_MINUTE = MILLISECONDS_PER_SECOND * 60
    MILLISECONDS_PER_HOUR = MILLISECONDS_PER_MINUTE * 60
    MILLISECONDS_PER_DAY = MILLISECONDS_PER_HOUR * 24
    MILLISECONDS_PER_WEEK = MILLISECONDS_PER_DAY * 7
    MILLISECONDS_PER_NON_LEAP_YEAR = MILLISECONDS_PER_DAY * 365
    MILLISECONDS_PER_LEAP_YEAR = MILLISECONDS_PER_DAY * 366

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


def is_zero_or_none(x: Any, /) -> bool:
    return x == 0 or x is None


def is_boolean(x: Any, /) -> bool:
    return isinstance(x, bool)


def is_positive_integer(x: Any, /) -> bool:
    return isinstance(x, int) and x > 0


def is_non_negative_integer(x: Any, /) -> bool:
    return isinstance(x, int) and x >= 0


def is_date_object(x: Any, /) -> bool:
    return isinstance(x, date) and x.__class__ is date


def is_datetime_object(x: Any, /) -> bool:
    return isinstance(x, datetime) and x.__class__ is datetime


def is_date_or_datetime_object(x: Any, /) -> bool:
    return isinstance(x, datetime) or isinstance(x, date)


def get_first_day_of_current_month(dt_obj: Union[datetime, date], /) -> Union[datetime, date]:
    return dt_obj.replace(day=1)


def get_last_day_of_current_month(dt_obj: Union[datetime, date], /) -> Union[datetime, date]:
    current_month = dt_obj.month
    if current_month in MONTHS_HAVING_30_DAYS:
        return dt_obj.replace(day=30)
    elif current_month in MONTHS_HAVING_31_DAYS:
        return dt_obj.replace(day=31)
    return dt_obj.replace(day=29) if is_leap_year(dt_obj.year) else dt_obj.replace(day=28)


def get_first_day_of_next_month(dt_obj: Union[datetime, date], /) -> Union[datetime, date]:
    if dt_obj.month == 12:
        return dt_obj.replace(year=dt_obj.year + 1, month=1, day=1)
    return dt_obj.replace(month=dt_obj.month + 1, day=1)


def is_february_29th(x: Union[datetime, date]) -> bool:
    assert is_date_or_datetime_object(x), "Param must be of type 'date' or 'datetime'"
    return x.month == 2 and x.day == 29


def compare_day_and_month(a: date, b: date, /) -> Literal["<", ">", "=="]:
    """
    Compares only the day and month of the given date objects.
        - If a < b, returns '<'
        - If a > b, returns '>'
        - If a == b, returns '=='
    """
    if a.month < b.month:
        return "<"
    if a.month > b.month:
        return ">"
    if a.day < b.day:
        return "<"
    if a.day > b.day:
        return ">"
    return "=="


def compute_date_difference(d1: date, d2: date, /) -> Tuple[int, int]:
    """Computes the absolute date-difference, and returns a tuple of (years, days)"""
    if d1 == d2:
        return (0, 0)
    if d1 > d2:
        d1, d2 = d2, d1  # ensure that d1 < d2

    d1_copy = d1.replace()
    year_difference = d2.year - d1.year
    operator = compare_day_and_month(d2, d1)
    if operator == ">":
        if is_february_29th(d1_copy):
            d1_copy = d1_copy.replace(year=d2.year, month=2, day=28)
        else:
            d1_copy = d1_copy.replace(year=d2.year)
    elif operator == "<":
        year_difference -= 1
        if is_february_29th(d1_copy):
            d1_copy = d1_copy.replace(year=d2.year - 1, month=2, day=28)
        else:
            d1_copy = d1_copy.replace(year=d2.year - 1)
    elif operator == "==":
        return (year_difference, 0)
    day_difference = (d2 - d1_copy).days
    return (year_difference, day_difference)


def is_leap_year(year: int, /) -> bool:
    assert isinstance(year, int), "Param `year` must be of type 'int'"
    if year % 4 != 0:
        return False
    if year % 100 != 0:
        return True
    return True if year % 400 == 0 else False


def get_day_of_week(
        dt_obj: Union[datetime, date],
        /,
        *,
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


class TimeTravel:
    """
    Class that represents a time-traveller.

    Instance methods:
        - add
        - copy
        - subtract

    Properties:
        - value
        - value_dtype
    """

    def __init__(self, value: Union[datetime, date], /) -> None:
        assert is_date_or_datetime_object(value), "Param must be of type 'date' or 'datetime'"
        self._value = value.replace()  # make a copy
        self._value_dtype: Literal["DATE", "DATETIME"] = (
            "DATETIME" if isinstance(self._value, datetime) else "DATE"
        )

    def __str__(self) -> str:
        if self.value_dtype == "DATETIME":
            return self.value.strftime(DATETIME_FORMAT)
        return self.value.strftime(DATE_FORMAT)

    def copy(self) -> TimeTravel:
        """Returns a copy (new instance) of the `TimeTravel` object"""
        return TimeTravel(self.value)

    @property
    def value(self) -> Union[datetime, date]:
        return self._value

    @value.setter
    def value(self, obj) -> None:
        assert is_date_or_datetime_object(obj), "Param must be of type 'date' or 'datetime'"
        self._value = obj

    @property
    def value_dtype(self) -> Literal["DATE", "DATETIME"]:
        return self._value_dtype

    @value_dtype.setter
    def value_dtype(self, obj) -> None:
        raise NotImplementedError("Not allowed to set the `value_dtype` property")

    def _add_years(self, *, years: int = 0) -> TimeTravel:
        updated_year = self.value.year + years
        if is_february_29th(self.value) and not is_leap_year(updated_year):
            self.value = self.value.replace(year=updated_year, month=3, day=1)
        else:
            self.value = self.value.replace(year=updated_year)
        return self

    def _subtract_years(self, *, years: int = 0) -> TimeTravel:
        updated_year = self.value.year - years
        if is_february_29th(self.value) and not is_leap_year(updated_year):
            self.value = self.value.replace(year=updated_year, month=3, day=1)
        else:
            self.value = self.value.replace(year=updated_year)
        return self

    def _compute_day_of_month_after_travelling(self, *, to_year: int, to_month: int, day_of_month: int) -> int:
        """
        Used for cases where day-of-month could be 29, 30, 31.
        Returns the day-of-month to use [1-31].
        """
        assert is_non_negative_integer(day_of_month) and 29 <= day_of_month <= 31, (
            "Param `day_of_month` must be one of: [29, 30, 31]"
        )
        if to_month in MONTHS_HAVING_30_DAYS:
            return day_of_month if day_of_month < 30 else 30
        elif to_month in MONTHS_HAVING_31_DAYS:
            return day_of_month
        return 29 if is_leap_year(to_year) else 28

    def _add_months(self, *, months: int = 0) -> TimeTravel:
        diff_years, diff_months = divmod(months, NUM_MONTHS_PER_YEAR)
        if diff_years > 0:
            self = self._add_years(years=diff_years)
        if diff_months == 0:
            return self
        updated_month = (
            (self.value.month + diff_months) % NUM_MONTHS_PER_YEAR
            if self.value.month + diff_months > NUM_MONTHS_PER_YEAR else
            self.value.month + diff_months
        )
        updated_year = (
            self.value.year + 1
            if self.value.month + diff_months > NUM_MONTHS_PER_YEAR else
            self.value.year
        )
        if 1 <= self.value.day <= 28:
            self.value = self.value.replace(year=updated_year, month=updated_month)
        else:
            day_of_month = self._compute_day_of_month_after_travelling(
                to_year=updated_year,
                to_month=updated_month,
                day_of_month=self.value.day,
            )
            self.value = self.value.replace(year=updated_year, month=updated_month, day=day_of_month)
        return self

    def _subtract_months(self, *, months: int = 0) -> TimeTravel:
        diff_years, diff_months = divmod(months, NUM_MONTHS_PER_YEAR)
        if diff_years > 0:
            self = self._subtract_years(years=diff_years)
        if diff_months == 0:
            return self
        updated_month = (
            NUM_MONTHS_PER_YEAR - abs(self.value.month - diff_months)
            if self.value.month - diff_months <= 0 else
            self.value.month - diff_months
        )
        updated_year = self.value.year - 1 if self.value.month - diff_months < 0 else self.value.year
        if 1 <= self.value.day <= 28:
            self.value = self.value.replace(year=updated_year, month=updated_month)
        else:
            day_of_month = self._compute_day_of_month_after_travelling(
                to_year=updated_year,
                to_month=updated_month,
                day_of_month=self.value.day,
            )
            self.value = self.value.replace(year=updated_year, month=updated_month, day=day_of_month)
        return self

    def add(
            self,
            *,
            years: int = 0,
            months: int = 0,
            weeks: int = 0,
            days: int = 0,
            hours: int = 0,
            minutes: int = 0,
            seconds: int = 0,
            milliseconds: int = 0,
            microseconds: int = 0,
        ) -> TimeTravel:
        assert is_non_negative_integer(years), "Param `years` must be a non-negative integer"
        assert is_non_negative_integer(months), "Param `months` must be a non-negative integer"
        assert is_non_negative_integer(weeks), "Param `weeks` must be a non-negative integer"
        assert is_non_negative_integer(days), "Param `days` must be a non-negative integer"
        assert is_non_negative_integer(hours), "Param `hours` must be a non-negative integer"
        assert is_non_negative_integer(minutes), "Param `minutes` must be a non-negative integer"
        assert is_non_negative_integer(seconds), "Param `seconds` must be a non-negative integer"
        assert is_non_negative_integer(milliseconds), "Param `milliseconds` must be a non-negative integer"
        assert is_non_negative_integer(microseconds), "Param `microseconds` must be a non-negative integer"
        if self.value_dtype == "DATE":
            assert is_zero_or_none(hours), "Param `hours` must not be passed when a date-object is used"
            assert is_zero_or_none(minutes), "Param `minutes` must not be passed when a date-object is used"
            assert is_zero_or_none(seconds), "Param `seconds` must not be passed when a date-object is used"
            assert is_zero_or_none(milliseconds), "Param `milliseconds` must not be passed when a date-object is used"
            assert is_zero_or_none(microseconds), "Param `microseconds` must not be passed when a date-object is used"
        self = self._add_years(years=years)
        self = self._add_months(months=months)
        self.value += timedelta(
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            milliseconds=milliseconds,
            microseconds=microseconds,
        )
        return self

    def subtract(
            self,
            *,
            years: int = 0,
            months: int = 0,
            weeks: int = 0,
            days: int = 0,
            hours: int = 0,
            minutes: int = 0,
            seconds: int = 0,
            milliseconds: int = 0,
            microseconds: int = 0,
        ) -> TimeTravel:
        assert is_non_negative_integer(years), "Param `years` must be a non-negative integer"
        assert is_non_negative_integer(months), "Param `months` must be a non-negative integer"
        assert is_non_negative_integer(weeks), "Param `weeks` must be a non-negative integer"
        assert is_non_negative_integer(days), "Param `days` must be a non-negative integer"
        assert is_non_negative_integer(hours), "Param `hours` must be a non-negative integer"
        assert is_non_negative_integer(minutes), "Param `minutes` must be a non-negative integer"
        assert is_non_negative_integer(seconds), "Param `seconds` must be a non-negative integer"
        assert is_non_negative_integer(milliseconds), "Param `milliseconds` must be a non-negative integer"
        assert is_non_negative_integer(microseconds), "Param `microseconds` must be a non-negative integer"
        if self.value_dtype == "DATE":
            assert is_zero_or_none(hours), "Param `hours` must not be passed when a date-object is used"
            assert is_zero_or_none(minutes), "Param `minutes` must not be passed when a date-object is used"
            assert is_zero_or_none(seconds), "Param `seconds` must not be passed when a date-object is used"
            assert is_zero_or_none(milliseconds), "Param `milliseconds` must not be passed when a date-object is used"
            assert is_zero_or_none(microseconds), "Param `microseconds` must not be passed when a date-object is used"
        self = self._subtract_years(years=years)
        self = self._subtract_months(months=months)
        self.value -= timedelta(
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            milliseconds=milliseconds,
            microseconds=microseconds,
        )
        return self


def offset_between_datetimes(
        *,
        start: Union[datetime, date],
        end: Union[datetime, date],
        offset_kwargs: Dict[str, int],
        ascending: Optional[bool] = True,
        as_string: Optional[bool] = False,
    ) -> Union[List[datetime], List[date], List[str]]:
    assert (
        (is_datetime_object(start) and is_datetime_object(end))
        or (is_date_object(start) and is_date_object(end))
    ), (
        "Param `start` and `end` must be either both 'datetime' or both 'date'"
    )
    assert start <= end, "Param `start` must be <= `end`"
    assert is_boolean(ascending), "Param `ascending` must be of type 'bool'"
    assert is_boolean(as_string), "Param `as_string` must be of type 'bool'"
    dt_objs = [start] if ascending else [end]
    time_travel = TimeTravel(start) if ascending else TimeTravel(end)
    while True:
        if ascending:
            time_travel.add(**offset_kwargs)
            if time_travel.value > end:
                break
            dt_objs.append(time_travel.value)
        else:
            time_travel.subtract(**offset_kwargs)
            if time_travel.value < start:
                break
            dt_objs.append(time_travel.value)
    if as_string:
        format_ = DATETIME_FORMAT if time_travel.value_dtype == "DATETIME" else DATE_FORMAT
        dt_objs = list(map(lambda x: x.strftime(format_), dt_objs))
    return dt_objs


def get_datetime_buckets(
        *,
        start: Union[datetime, date],
        num_buckets: int,
        offset_kwargs: Dict[str, int],
        ascending: Optional[bool] = True,
        as_string: Optional[bool] = False,
    ) -> Union[
        List[Tuple[datetime, datetime]],
        List[Tuple[date, date]],
        List[Tuple[str, str]],
    ]:
    assert is_date_or_datetime_object(start), "Param `start` must be of type 'date' or 'datetime'"
    assert is_positive_integer(num_buckets), "Param `num_buckets` must be a positive integer"
    assert len(offset_kwargs) == 1, "Only 1 offset can be used at a time"
    assert is_boolean(ascending), "Param `ascending` must be of type 'bool'"
    assert is_boolean(as_string), "Param `as_string` must be of type 'bool'"
    buckets = []
    num_buckets_filled = 0
    time_travel = TimeTravel(start)
    while True:
        if num_buckets_filled == num_buckets:
            break
        temp_start = time_travel.copy()
        if ascending:
            time_travel.add(**offset_kwargs)
            temp_end = time_travel.copy().subtract(days=1) if time_travel.value_dtype == "DATE" else time_travel.copy()
        else:
            time_travel.subtract(**offset_kwargs)
            temp_end = time_travel.copy().add(days=1) if time_travel.value_dtype == "DATE" else time_travel.copy()
        if buckets:
            buckets.append((temp_start.value, temp_end.value))
        else:
            buckets.append((start, temp_end.value))
        num_buckets_filled += 1
    if not ascending:
        buckets = [(y, x) for x, y in buckets][::-1]
    if as_string:
        format_ = DATETIME_FORMAT if time_travel.value_dtype == "DATETIME" else DATE_FORMAT
        buckets = [(x.strftime(format_), y.strftime(format_)) for x, y in buckets]
    return buckets



'''
def main():
    start = date(2000, 4, 1)
    offset_kwargs = dict(
        years=1,
        # months=12,
        # days=7,
        # hours=12,
        # minutes=30,
    )
    buckets = get_datetime_buckets(
        start=start,
        num_buckets=5,
        offset_kwargs=offset_kwargs,
        ascending=True,
        as_string=True,
    )
    print(
        start,
        f"offset_kwargs: {offset_kwargs}",
        buckets,
        sep="\n",
    )

    start = datetime(year=1996, month=1, day=1)
    end = datetime(year=1996, month=1, day=3, hour=8)
    offset_kwargs = dict(
        # years=1,
        # months=12,
        # weeks=10,
        # days=11,
        hours=8,
    )
    dt_objs = offset_between_datetimes(
        start=start,
        end=end,
        offset_kwargs=offset_kwargs,
        ascending=True,
        as_string=True,
    )
    print(
        dt_objs,
        f"offset_kwargs: {offset_kwargs}",
        sep="\n",
    )

    d = date(year=2020, month=3, day=1)
    # d = date(year=2020, month=1, day=29)
    tt = TimeTravel(d)
    tt.add(months=3)
    print(
        d,
        tt,
        sep="\n",
    )


if __name__ == "__main__":
    main()

'''
