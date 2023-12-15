import mimetypes
import os
import shutil
from datetime import datetime
from pathlib import Path


def readable_time(seconds: int) -> str:
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
    SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]

    if size_in_bytes is None:
        return "0B"
    index = 0
    while size_in_bytes >= 1024:
        size_in_bytes /= 1024
        index += 1
    try:
        return f"{round(size_in_bytes, 2)}{SIZE_UNITS[index]}"
    except IndexError:
        return "File too large"


def file_type(file_path: Path):
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type


def speed_convert(size):
    """Hi human, you can't read bytes?"""
    power = 2**10
    zero = 0
    units = {0: "", 1: "Kb/s", 2: "MB/s", 3: "Gb/s", 4: "Tb/s"}
    while size > power:
        size /= power
        zero += 1
    return f"{round(size, 2)} {units[zero]}"


def clean_cache():
    curr_path = os.getcwd()
    for root, dirs, files in os.walk(curr_path):
        for cache in dirs:
            if "__pycache__" in cache:
                path = os.path.join(root, cache)
                shutil.rmtree(path)


def date_fmt(original_date_str):
    if not original_date_str:
        return "N/A"
    original_date = datetime.strptime(str(original_date_str), "%Y-%m-%d %H:%M:%S")

    formatted_date_str = original_date.strftime("%m-%d-%Y")
    return formatted_date_str
