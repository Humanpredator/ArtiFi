import time

from discord.ext.commands import Cog, command

from artifi.discord import Discord, send_message
from artifi.discord.misc.custom_function import edit_message


class Ping(Cog):
    def __init__(self, bot):
        self._bot: Discord = bot

    @command('ping', help="Calculate The Latency Of The Server.")
    async def ping_command(self, ctx):
        if not self._bot.sudo_only(ctx):
            return await send_message(ctx, "Access Denied...!")
        start_time = int(round(time.time() * 1000))
        msg = await send_message(ctx, "Starting Ping Test...!")
        end_time = int(round(time.time() * 1000))
        await edit_message(msg, content=f"{end_time - start_time} ms")


async def setup(bot):
    await bot.add_cog(Ping(bot))
