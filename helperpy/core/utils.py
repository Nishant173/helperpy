from typing import Any, Dict, List, Optional, Tuple, Type
import random
from uuid import UUID

import numpy as np

from helperpy.core.exceptions import (
    raise_exception_if_invalid_option,
    raise_exception_if_invalid_type,
)
from helperpy.core.type_annotations import (
    Number,
    NumberOrString,
)


def is_instance_of_any(obj: Any, types: List[Type]) -> bool:
    return any((isinstance(obj, type_) for type_ in types))


def __get_kwarg_as_string(key: Any, value: Any) -> str:
    return f"{key}='{value}'" if is_instance_of_any(obj=value, types=[str, UUID]) else f"{key}={value}"


def __single_line_string_repr(
        *,
        class_name: str,
        kwargs_dict: Dict[str, Any],
    ) -> str:
    if not kwargs_dict:
        return f"{class_name}()"
    kwargs_dict_as_string = ", ".join(
        [__get_kwarg_as_string(key=key, value=value) for key, value in kwargs_dict.items()]
    )
    return f"{class_name}({kwargs_dict_as_string})"


def __multi_line_string_repr(
        *,
        class_name: str,
        kwargs_dict: Dict[str, Any],
    ) -> str:
    if not kwargs_dict:
        return f"{class_name}()"
    indent = 4
    kwargs_dict_as_string = ""
    for key, value in kwargs_dict.items():
        kwargs_dict_as_string += " " * indent + __get_kwarg_as_string(key=key, value=value) + "," + "\n"
    kwargs_dict_as_string = kwargs_dict_as_string.rstrip()
    return f"{class_name}(\n{kwargs_dict_as_string}\n)"


def create_string_repr(
        *,
        instance: Any,
        kwargs_dict: Optional[Dict[str, Any]] = None,
        multi_line: Optional[bool] = True,
    ) -> str:
    """
    Writes __str__ representation of a class' instance.

    ```
    class Person:
        pass

    create_string_repr(
        instance=Person(),
        kwargs_dict={
            "first_name": "James",
            "last_name": "Murphy",
            "age": 35,
            "is_developer": True,
        },
        multi_line=False,
    ) # Returns the string: "Person(first_name='James', last_name='Murphy', age=35, is_developer=True)"
    ```
    """
    kwargs_dict = {} if kwargs_dict is None else kwargs_dict
    assert isinstance(kwargs_dict, dict), f"`kwargs_dict` needs to be a dictionary, but got {type(kwargs_dict)}"
    kw = {
        "class_name": instance.__class__.__name__,
        "kwargs_dict": kwargs_dict,
    }
    return __multi_line_string_repr(**kw) if multi_line else __single_line_string_repr(**kw)


def print_docstring(obj: Any) -> None:
    """Prints the doc-string (if available). Usually of a class, method or function."""
    if hasattr(obj, "__doc__"):
        print(obj.__doc__)
    return None


def is_none(value: Any) -> bool:
    return value is None


def is_nan(value: Any) -> bool:
    """Returns True if value given is Numpy's NaN. Otherwise returns False"""
    if isinstance(value, float):
        if np.isnan(value):
            return True
    return False


def is_none_or_nan(value: Any) -> bool:
    """Returns True if value given is Python's native None or Numpy's NaN. Otherwise returns False"""
    return is_none(value=value) or is_nan(value=value)


def get_timetaken_dictionary(num_seconds: Number) -> Dict[str, Number]:
    """
    Returns dictionary having the keys: ['weeks', 'days', 'hours', 'minutes', 'seconds', 'milliseconds'] containing the time elapsed.
    
    >>> get_timetaken_dictionary(num_seconds=3725.4292)
    >>> get_timetaken_dictionary(num_seconds=885354.128129)
    """
    weeks, remainder = divmod(num_seconds, 60*60*24*7)
    days, remainder = divmod(remainder, 60*60*24)
    hours, remainder = divmod(remainder, 60*60)
    minutes, remainder = divmod(remainder, 60)
    seconds = np.floor(remainder)
    milliseconds = round((remainder - seconds) * 1000, 5)
    
    dictionary_time_taken = {
        'weeks': integerify_if_possible(weeks),
        'days': integerify_if_possible(days),
        'hours': integerify_if_possible(hours),
        'minutes': integerify_if_possible(minutes),
        'seconds': integerify_if_possible(seconds),
        'milliseconds': integerify_if_possible(milliseconds),
    }
    return dictionary_time_taken


def get_timetaken_fstring(num_seconds: Number) -> str:
    """Returns f-string containing the elapsed time"""
    dict_time_taken = get_timetaken_dictionary(num_seconds=num_seconds)
    dict_unit_shortener = {
        'weeks': 'w',
        'days': 'd',
        'hours': 'h',
        'minutes': 'm',
        'seconds': 's',
        'milliseconds': 'ms',
    }
    time_taken_fstring = " ".join([f"{value}{dict_unit_shortener.get(unit, unit)}" for unit, value in dict_time_taken.items() if value != 0]).strip()
    time_taken_fstring = '0s' if time_taken_fstring == "" else time_taken_fstring
    return time_taken_fstring


def get_random_choice_except(
        choices: List[NumberOrString],
        exception: NumberOrString,
    ) -> NumberOrString:
    """Gets random choice from an iterable, given an exception"""
    if exception not in choices:
        raise ValueError("The `exception` is not available in the given `choices`")
    choices_available = list(set(choices).difference(set([exception])))
    if not choices_available:
        raise ValueError("No choices available")
    return random.choice(choices_available)


def round_off_as_string(number: Number, round_by: int) -> str:
    """
    Rounds off the given `number` to `round_by` decimal places, and type casts
    it to a string (to retain the exact number of decimal places desired).
    """
    if round_by < 0:
        raise ValueError("The `round_by` parameter must be >= 0")
    if round_by == 0:
        return str(round(number))
    number_stringified = str(round(number, round_by))
    decimal_places_filled = len(number_stringified.split('.')[-1])
    decimal_places_to_fill = round_by - decimal_places_filled
    for _ in range(decimal_places_to_fill):
        number_stringified += '0'
    return number_stringified


def commafy_number(number: Number) -> str:
    """
    Adds commas to number for better readability.
    >>> commafy_number(number=1738183090) # Returns "1,738,183,090"
    >>> commafy_number(number=1738183090.90406) # Returns "1,738,183,090.90406"
    """
    if int(number) == number:
        return format(int(number), ",d")
    return format(number, ",f")


def is_whole_number(number: Number) -> bool:
    return int(number) == number


def integerify_if_possible(number: Number) -> Number:
    """Converts whole numbers represented as floats to integers"""
    return int(number) if is_whole_number(number=number) else number


def string_to_int_or_float(value: str) -> Number:
    """Converts stringified number to either int or float"""
    number = float(value)
    number = integerify_if_possible(number=number)
    return number


def stringify_list_of_nums(array: List[Number]) -> str:
    """Converts list of ints/floats to comma separated string of the same"""
    return ",".join(list(map(str, array)))


def listify_string_of_nums(string: str) -> List[Number]:
    """Converts string of comma separated ints/floats to list of numbers"""
    numbers = string.split(',')
    numbers = list(map(string_to_int_or_float, numbers))
    return numbers


def get_max_of_abs_values(array: List[Number]) -> Number:
    """Finds maximum of absolute values of numbers in an array"""
    array_abs = list(map(abs, array))
    return max(array_abs)


def get_min_of_abs_values(array: List[Number]) -> Number:
    """Finds minimum of absolute values of numbers in an array"""
    array_abs = list(map(abs, array))
    return min(array_abs)


def has_negative_number(array: List[Number]) -> bool:
    for number in array:
        if number < 0:
            return True
    return False


def has_positive_number(array: List[Number]) -> bool:
    for number in array:
        if number > 0:
            return True
    return False


def cumulative_aggregate(numbers: List[Number], method: str) -> List[Number]:
    """
    Returns list of cumulative aggregates.
    Options for `method` are: ['sum', 'difference', 'product', 'division'].
    """
    raise_exception_if_invalid_option(
        option_name='method',
        option_value=method,
        valid_option_values=['sum', 'difference', 'product', 'division'],
    )

    length = len(numbers)
    if length == 0:
        return []
    cumulative_array = [numbers[0]]
    if length == 1:
        return cumulative_array
    method_mapper = {
        'sum': lambda x, y: x + y,
        'difference': lambda x, y: x - y,
        'product': lambda x, y: x * y,
        'division': lambda x, y: x / y,
    }
    for number in numbers[1:]:
        cumulative_array.append(method_mapper[method](cumulative_array[-1], number))
    return cumulative_array



class Partitioner:
    """
    Class for iterable partitioning.

    >>> partitioner = Partitioner(iterable_length=100)
    >>> kwargs = {
            'num_partitions': 8,
            'distribution_method': 'uniform',
        }
    >>> partitioner.sizes_by_num_partitions(**kwargs)
    >>> partitioner.index_ranges_by_num_partitions(**kwargs)
    >>> partitioner.sizes_by_max_partition_size(max_partition_size=15)
    >>> partitioner.index_ranges_by_max_partition_size(max_partition_size=15)
    """

    def __init__(self, iterable_length: int) -> None:
        self.__validate_iterable_length(iterable_length=iterable_length)
        self.__iterable_length = iterable_length
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(iterable_length={self.__iterable_length})"
    
    def __validate_iterable_length(self, iterable_length: int) -> None:
        raise_exception_if_invalid_type(
            parameter_name='iterable_length',
            parameter_value=iterable_length,
            expected_type=int,
        )
        if iterable_length < 0:
            raise ValueError("The parameter `iterable_length` cannot be < 0")
        return None
    
    def __validate_max_partition_size(self, max_partition_size: int) -> None:
        raise_exception_if_invalid_type(
            parameter_name='max_partition_size',
            parameter_value=max_partition_size,
            expected_type=int,
        )
        if max_partition_size <= 0:
            raise ValueError("The parameter `max_partition_size` cannot be <= 0")
        if max_partition_size > self.__iterable_length:
            raise ValueError("The parameter `max_partition_size` cannot be > length of the iterable")
        return None
    
    def __validate_num_partitions(self, num_partitions: int) -> None:
        raise_exception_if_invalid_type(
            parameter_name='num_partitions',
            parameter_value=num_partitions,
            expected_type=int,
        )
        if num_partitions <= 0:
            raise ValueError("The parameter `num_partitions` cannot be <= 0")
        if num_partitions > self.__iterable_length:
            raise ValueError("The parameter `num_partitions` cannot be > length of the iterable")
        return None
    
    def sizes_by_max_partition_size(self, max_partition_size: int) -> List[int]:
        """Returns list of sizes of each partition"""
        self.__validate_max_partition_size(max_partition_size=max_partition_size)
        num_full_partitions, last_partition_size = divmod(self.__iterable_length, max_partition_size)
        if last_partition_size == 0:
            return [max_partition_size] * num_full_partitions
        return [max_partition_size] * num_full_partitions + [last_partition_size]
    
    def __sizes_by_fill_distribution(self, num_partitions: int) -> List[int]:
        full_partition_size, last_partition_size = divmod(self.__iterable_length, num_partitions)
        if last_partition_size == 0:
            return [full_partition_size] * num_partitions
        sizes = [full_partition_size] * (num_partitions - 1)
        sizes.insert(0, full_partition_size + last_partition_size)
        return sizes
    
    def __sizes_by_uniform_distribution(self, num_partitions: int) -> List[int]:
        min_partition_size, num_residuals = divmod(self.__iterable_length, num_partitions)
        return [min_partition_size + 1] * num_residuals + [min_partition_size] * (num_partitions - num_residuals)
    
    def sizes_by_num_partitions(
            self,
            num_partitions: int,
            distribution_method: Optional[str] = 'uniform',
        ) -> List[int]:
        """
        Returns list of sizes of each partition.
        Options for `distribution_method` are: ['fill', 'uniform']. Default: 'uniform'
        """
        self.__validate_num_partitions(num_partitions=num_partitions)
        raise_exception_if_invalid_option(
            option_name='distribution_method',
            option_value=distribution_method,
            valid_option_values=['fill', 'uniform'],
        )
        if distribution_method == 'fill':
            return self.__sizes_by_fill_distribution(num_partitions=num_partitions)
        if distribution_method == 'uniform':
            return self.__sizes_by_uniform_distribution(num_partitions=num_partitions)
    
    def __compute_index_ranges_from_partition_sizes(
            self,
            partition_sizes: List[int],
        ) -> List[Tuple[int, int]]:
        start_indices = [0] + list(np.cumsum(partition_sizes))
        index_ranges = [(start_indices[idx], start_indices[idx+1]) for idx in range(len(start_indices) - 1)]
        return index_ranges
    
    def index_ranges_by_max_partition_size(self, max_partition_size: int) -> List[Tuple[int, int]]:
        """
        Returns list of tuples of (start_index, end_index) that partition an iterable.
        The indices are zero-based.
        """
        sizes = self.sizes_by_max_partition_size(max_partition_size=max_partition_size)
        return self.__compute_index_ranges_from_partition_sizes(partition_sizes=sizes)
    
    def index_ranges_by_num_partitions(
            self,
            num_partitions: int,
            distribution_method: Optional[str] = 'uniform',
        ) -> List[Tuple[int, int]]:
        """
        Returns list of tuples of (start_index, end_index) that partition an iterable.
        The indices are zero-based.
        Options for `distribution_method` are: ['fill', 'uniform']. Default: 'uniform'
        """
        sizes = self.sizes_by_num_partitions(num_partitions=num_partitions, distribution_method=distribution_method)
        return self.__compute_index_ranges_from_partition_sizes(partition_sizes=sizes)
