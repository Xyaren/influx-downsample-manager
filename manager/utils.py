import hashlib
from datetime import timedelta
from fnmatch import fnmatch

from .model import FieldData, MeasurementConfig


def hash_to_decimal(input_string: str, min_decimal: float, max_decimal: float) -> float:
    # Use SHA-256 for hashing, you can choose a different algorithm if needed
    hash_object = hashlib.sha256(input_string.encode())

    # Convert the hexadecimal hash to a decimal value
    hash_decimal = int(hash_object.hexdigest(), 16)

    # Normalize the hash_decimal to a value between 0 and 1
    normalized_value = hash_decimal / (2**256 - 1)

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
        (int(days), "d"),
        (int(hours), "h"),
        (int(minutes), "m"),
        (int(seconds), "s"),
    ]

    flux_duration = "".join(f"{value}{unit}" for value, unit in flux_duration_parts if value != 0)
    return flux_duration or "0s"


def filter_fields(
    fields: dict[str, FieldData],
    measurement_config: MeasurementConfig | None,
) -> dict[str, FieldData] | None:
    """Apply measurement-level config to a field mapping.

    Returns:
    - ``None`` when the measurement is excluded (``include: false``).
    - A (possibly filtered) dict of fields otherwise.

    Field filtering rules:
    - If *measurement_config* is ``None`` or empty, all fields pass through.
    - ``include_fields`` patterns are evaluated first: only matching fields survive.
    - ``exclude_fields`` patterns are applied second: matching fields are removed.
    - Patterns use :func:`fnmatch.fnmatch` (``*``, ``?``, ``[seq]``).
    """
    if not measurement_config:
        return fields

    # Whole-measurement exclusion
    if not measurement_config.get("include", True):
        return None

    include_patterns = measurement_config.get("include_fields")
    exclude_patterns = measurement_config.get("exclude_fields")

    result = fields

    if include_patterns:
        result = {name: data for name, data in result.items() if any(fnmatch(name, pat) for pat in include_patterns)}

    if exclude_patterns:
        result = {
            name: data for name, data in result.items() if not any(fnmatch(name, pat) for pat in exclude_patterns)
        }

    return result
