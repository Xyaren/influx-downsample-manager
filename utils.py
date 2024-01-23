import hashlib


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
