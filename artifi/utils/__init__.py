"""Common function used throughout the package"""
import mimetypes
import os
from typing import List, Union


def sanitize_name(string) -> str:
    """
    Remove any chars which are not allowed to be named for a folder
    Remove double spaces and replace spaces with underscores
    @param string: name of the folder to be created
    @return: sanitized string
    """
    forbidden_chars_windows = {"<", ">", ":", '"', "/", "\\", "|", "?", "*", "&"}

    if os.name == "nt":
        forbidden_chars = forbidden_chars_windows
        # Remove forbidden characters
        sanitized_string = "".join(
            char for char in string if char not in forbidden_chars)

        # Remove double spaces and replace spaces with underscores
        sanitized_string = "_".join(sanitized_string.split())
    else:
        sanitized_string = string.replace('/', ' ')

    return sanitized_string.strip()


def get_nested_key(
        d: Union[dict, List[dict]], key: str, default="UNKNOWN"
) -> Union[str, dict, List[str]]:
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


def readable_time(seconds) -> str:
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

    if not size_in_bytes:
        return "0B"
    index = 0
    while size_in_bytes >= 1024:
        size_in_bytes /= 1024
        index += 1
    try:
        return f"{round(size_in_bytes, 2)}{units[index]}"
    except IndexError:
        return "Opps! To Large"


def path_size(path):
    """

    @param path:
    @return:
    """
    if os.path.isfile(path):
        return os.path.getsize(path)
    total_size = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            abs_path = os.path.join(root, f)
            total_size += os.path.getsize(abs_path)
    return total_size


def speed_convert(size):
    """Hi human, you can't read bytes?"""
    power = 2 ** 10
    zero = 0
    units = {0: "", 1: "Kb/s", 2: "MB/s", 3: "Gb/s", 4: "Tb/s"}
    while size > power:
        size /= power
        zero += 1
    return f"{round(size, 2)} {units[zero]}"


def fetch_mime_type(file_path):
    """

    @param file_path:
    @return:
    """
    mime_type, encoding = mimetypes.guess_type(file_path)
    return mime_type or "text/plain"
