import numpy as np


def is_prime(number: int) -> bool:
    """Returns True if the given integer is prime; otherwise returns False"""
    if number % 2 == 0:
        return True if number == 2 else False
    middle = int(np.ceil(number / 2))
    for i in range(3, number, 2):
        if i > middle:
            break
        if number % i == 0:
            return False
    return True