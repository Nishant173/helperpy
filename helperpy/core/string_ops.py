from typing import List


def __raise_error_if_num_chars_is_negative(num_chars: int) -> None:
    if num_chars < 0:
        raise ValueError(f"Expected `num_chars` to be >= 0, but got {num_chars}")
    return None


def get_first_n_characters(text: str, num_chars: int) -> str:
    __raise_error_if_num_chars_is_negative(num_chars=num_chars)
    return text[:num_chars]


def get_last_n_characters(text: str, num_chars: int) -> str:
    __raise_error_if_num_chars_is_negative(num_chars=num_chars)
    return text[-num_chars:]


def remove_first_n_characters(text: str, num_chars: int) -> str:
    __raise_error_if_num_chars_is_negative(num_chars=num_chars)
    return text[num_chars:]


def remove_last_n_characters(text: str, num_chars: int) -> str:
    __raise_error_if_num_chars_is_negative(num_chars=num_chars)
    return text[:-num_chars]


def remove_characters_at_indices(text: str, indices: List[int]) -> str:
    """
    Removes characters present at the given `indices` in the `text`.
    Expects `indices` to be in range (0, n-1) where n is the length of the `text`.
    Raises an IndexError if any of the given `indices` are out of bounds.
    """
    if not indices:
        return text
    indices = sorted(list(set(indices)), reverse=True)
    lowest_possible_index, highest_possible_index = 0, len(text) - 1 # Must not use negative indices
    if indices[-1] < lowest_possible_index or indices[0] > highest_possible_index:
        raise IndexError(
            f"Accepted index-range for the given text is ({lowest_possible_index}, {highest_possible_index})."
            " Cannot remove character at an index outside of this range."
        )
    list_of_chars = list(text)
    for index in indices:
        list_of_chars.pop(index)
    return "".join(list_of_chars)