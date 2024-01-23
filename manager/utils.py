import hashlib
from datetime import timedelta


def hash_to_decimal(input_string: str, min_decimal: float, max_decimal: float) -> float:
    # Use SHA-256 for hashing, you can choose a different algorithm if needed
    hash_object = hashlib.sha256(input_string.encode())

    # Convert the hexadecimal hash to a decimal value
    hash_decimal = int(hash_object.hexdigest(), 16)

    # Normalize the hash_decimal to a value between 0 and 1
    normalized_value = hash_decimal / (2 ** 256 - 1)

    # Map the normalized_value to the specified range
    scaled_decimal = min_decimal + normalized_value * (max_decimal - min_decimal)

    return scaled_decimal

def hash_to_integer(input_string: str, min_integer: int, max_integer: int) -> int:
    # Use SHA-256 for hashing, you can choose a different algorithm if needed
    hash_object = hashlib.sha256(input_string.encode())

    # Convert the hexadecimal hash to an integer value
    hash_integer = int(hash_object.hexdigest(), 16)

    # Map the hash_integer to the specified range
    scaled_integer = min_integer + hash_integer % (max_integer - min_integer + 1)

    return scaled_integer

def timedelta_to_flux_duration(td: timedelta) -> str:
    seconds = round(td.total_seconds())

    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Build the Flux duration string, omitting parts with value zero
    flux_duration_parts = [
        (int(days), 'd'),
        (int(hours), 'h'),
        (int(minutes), 'm'),
        (int(seconds), 's'),
    ]

    flux_duration = ''.join(f"{value}{unit}" for value, unit in flux_duration_parts if value != 0)
    return flux_duration
