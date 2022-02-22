from typing import Any, List, Type


class InvalidOptionError(Exception):
    """Error raised when an invalid option is passed in"""
    pass


class InvalidDataFrameError(Exception):
    """Error raised when an invalid DataFrame is encountered (based on certain expectations)"""
    pass


class MissingRequiredParameterError(Exception):
    """Error raised when a required parameter is missing"""
    pass


def raise_exception_if_invalid_option(
        option_name: str,
        option_value: Any,
        valid_option_values: List[Any],
    ) -> None:
    """Raises a ValueError if the `option_value` given is an invalid option; otherwise returns None"""
    if option_value not in valid_option_values:
        raise InvalidOptionError(f"Expected `{option_name}` to be in {valid_option_values}, but got `{option_value}`")
    return None


def raise_exception_if_invalid_type(
        parameter_name: str,
        parameter_value: Any,
        expected_type: Type,
    ) -> None:
    """Raises a TypeError if the `parameter_value` given is not of the type `expected_type`; otherwise returns None"""
    if not isinstance(parameter_value, expected_type):
        raise TypeError(f"Expected `{parameter_name}` to be of type `{expected_type}`, but got type `{type(parameter_value)}`")
    return None