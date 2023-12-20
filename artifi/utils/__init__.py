"""
Common function used throughout the package
"""

from typing import Union, List


def get_nested_key(d: Union[dict, List[dict]], key: str, default="UNKNOWN") -> Union[str, dict, List[str]]:
    """
    Used to Get the specific key value from dict junk
    @param d: original dict
    @param key: name of the key to be searched
    @param default: default value if key is not present
    @return: dict
    """
    if isinstance(d, list):
        for item in d:
            result = get_nested_key(item, key)
            if result is not None:
                return result
    elif isinstance(d, dict):
        for k, v in d.items():
            if k == key:
                return v
            if isinstance(v, (dict, list)):
                result = get_nested_key(v, key)
                if result is not None:
                    return result
    return default


def readable_time(seconds: int) -> str:
    """

    @param seconds: UNIX timestamp
    @return: human-readable Format
    """
    result = ""
    (days, remainder) = divmod(seconds, 86400)
    days = int(days)
    if days != 0:
        result += f"{days}d"
    (hours, remainder) = divmod(remainder, 3600)
    hours = int(hours)
    if hours != 0:
        result += f"{hours}h"
    (minutes, seconds) = divmod(remainder, 60)
    minutes = int(minutes)
    if minutes != 0:
        result += f"{minutes}m"
    seconds = int(seconds)
    result += f"{seconds}s"
    return result


def readable_size(size_in_bytes) -> str:
    """

    @param size_in_bytes: Size in bytes
    @return: human-readable format
    """
    units = ["B", "KB", "MB", "GB", "TB", "PB"]

    if size_in_bytes is None:
        return "0B"
    index = 0
    while size_in_bytes >= 1024:
        size_in_bytes /= 1024
        index += 1
    try:
        return f"{round(size_in_bytes, 2)}{units[index]}"
    except IndexError:
        return "File too large"


def speed_convert(size):
    """Hi human, you can't read bytes?"""
    power = 2 ** 10
    zero = 0
    units = {0: "", 1: "Kb/s", 2: "MB/s", 3: "Gb/s", 4: "Tb/s"}
    while size > power:
        size /= power
        zero += 1
    return f"{round(size, 2)} {units[zero]}"
