import re

ALPHABETS_LOWER_CASED = list('abcdefghijklmnopqrstuvwxyz')
ALPHABETS_UPPER_CASED = list(map(str.upper, ALPHABETS_LOWER_CASED))
ALPHABETS = ALPHABETS_LOWER_CASED + ALPHABETS_UPPER_CASED


def camel_to_pascal(string: str) -> str:
    """
    Converts camel-case to pascal-case.
    >>> camel_to_pascal(string="helloAndGoodMorning") # Returns "HelloAndGoodMorning"
    """
    return string[0].upper() + string[1:]


def pascal_to_camel(string: str) -> str:
    """
    Converts pascal-case to camel-case.
    >>> pascal_to_camel(string="HelloAndGoodMorning") # Returns "helloAndGoodMorning"
    """
    return string[0].lower() + string[1:]


def pascal_to_snake(string: str) -> str:
    """
    Converts pascal-case to snake-case.
    >>> pascal_to_snake(string="HelloAndGoodMorning") # Returns "hello_and_good_morning"
    """
    words = re.findall(pattern="[A-Z][^A-Z]*", string=string)
    words_lower_cased = list(map(str.lower, words))
    return "_".join(words_lower_cased)


def camel_to_snake(string: str) -> str:
    """
    Converts camel-case to snake-case.
    >>> camel_to_snake(string="helloAndGoodMorning") # Returns "hello_and_good_morning"
    """
    string_in_pascal = camel_to_pascal(string=string)
    string_in_snake = pascal_to_snake(string=string_in_pascal)
    return string_in_snake


def snake_to_pascal(string: str) -> str:
    """
    Converts snake-case to pascal-case.
    >>> snake_to_pascal(string="hello_and_good_morning") # Returns "HelloAndGoodMorning"
    """
    words = string.split('_')
    words_capitalized = list(map(str.capitalize, words))
    return "".join(words_capitalized)


def snake_to_camel(string: str) -> str:
    """
    Converts snake-case to camel-case.
    >>> snake_to_camel(string="hello_and_good_morning") # Returns "helloAndGoodMorning"
    """
    string_in_pascal = snake_to_pascal(string=string)
    string_in_camel = pascal_to_camel(string=string_in_pascal)
    return string_in_camel


def retardify(string: str) -> str:
    """
    Converts given text to retardified text.
    >>> retardify(string="Hello, and good morning!") # Returns "hElLo, AnD gOoD mOrNiNg!"
    """
    string = string.lower().strip()
    counter = 0
    retardified_text = ""
    for character in string:
        if character in ALPHABETS:
            counter += 1
            if counter % 2 == 0:
                character = character.upper()
        retardified_text += character
    return retardified_text