from datetime import timedelta

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

# Example usage:
duration = timedelta(days=5, hours=12, minutes=30, seconds=45, microseconds=123456)
flux_duration = timedelta_to_flux_duration(duration)
print(flux_duration)
