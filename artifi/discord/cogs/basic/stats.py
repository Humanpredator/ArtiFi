import platform
import shutil
import time
from datetime import datetime

import psutil
from cpuinfo import cpuinfo
from discord import Embed
from discord.ext.commands import Cog, command

from artifi.discord import Discord, send_message
from artifi.discord.misc.custom_function import edit_message
from artifi.utils.func import readable_size, readable_time


class Stats(Cog):
    def __init__(self, bot):
        self._bot: Discord = bot

    @command("stats", help="Fetch All Possible Detail About The Hosted Server.")
    async def server_status(self, ctx):
        if not self._bot.sudo_only(ctx):
            return await send_message(ctx, "Access Denied...!")
        msg = await send_message(ctx, content="Getting Server Stats...!", reply=True)
        current_time = readable_time(time.time() - self._bot.bot_start_time)
        total, used, free = shutil.disk_usage("/")
        total_disk = readable_size(total)
        used_disk = readable_size(used)
        free_disk = readable_size(free)
        sent = readable_size(psutil.net_io_counters().bytes_sent)
        recv = readable_size(psutil.net_io_counters().bytes_recv)
        memory = psutil.virtual_memory()
        total_ram = readable_size(memory.total)  # Total RAM
        free_ram = readable_size(memory.available)  # Available (Free) RAM
        used_ram = readable_size(memory.used)  # Used RAM
        memory_percent = memory.percent
        disk = psutil.disk_usage("/").percent
        emd = Embed(timestamp=datetime.now())
        os_data = f"""
System: ***{platform.system()}***
Release: ***{platform.release()}***
Version: ***{platform.version()}***
Python Version: ***{platform.python_version()}***
"""
        cpu_info = cpuinfo.get_cpu_info()
        cpu_data = f"""
Arch: ***{cpu_info['arch']}({cpu_info['arch_string_raw']})***
Bits: ***{cpu_info["bits"]}***
Name: ***{cpu_info["brand_raw"]}***
Vendor: ***{cpu_info["vendor_id_raw"]}***
Cores: ***{cpu_info["count"]}***
CPU Usage: ***{psutil.cpu_percent()}%***
"""
        disk_space = f"""
Total Space: ***{total_disk}***
Used Space: ***{used_disk}***
Available Space: ***{free_disk}***
Disk Usage: ***{disk}%***
"""
        ram = f"""
Total RAM: ***{total_ram}***
Used RAM: ***{used_ram}***
Available RAM: ***{free_ram}***
RAM Usage: ***{memory_percent}%***
"""
        network = f"""
Total Upload: ***{sent}***
Total Download: ***{recv}***
"""
        emd.add_field(name="Up Time", value=f"***{current_time}***")
        emd.add_field(name="OS", value=os_data)
        emd.add_field(name="CPU", value=cpu_data)

        emd.add_field(name="Disk Space", value=disk_space)
        emd.add_field(name="Ram Space", value=ram)
        emd.add_field(name="Network", value=network)

        await edit_message(msg, embed=emd)


async def setup(bot):
    await bot.add_cog(Stats(bot))
