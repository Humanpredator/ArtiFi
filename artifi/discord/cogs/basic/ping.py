"""Latency Cog"""
import time

from discord.ext.commands import Cog, command

from artifi.discord import Discord
from artifi.discord.misc.discord_func import edit_message, send_message


class Ping(Cog):
    """Get network Latency between you and server"""

    def __init__(self, bot):
        """@param bot:"""
        self._bot: Discord = bot

    @command("ping", help="Calculate The Latency Of The Server.")
    async def ping_command(self, ctx):
        """

        @param ctx:
        @return:
        """
        if not self._bot.sudo_only(ctx):
            return await send_message(ctx, "Access Denied...!")
        start_time = int(round(time.time() * 1000))
        msg = await send_message(ctx, "Starting Ping Test...!")
        end_time = int(round(time.time() * 1000))
        await edit_message(msg, content=f"{end_time - start_time} ms")


async def setup(bot):
    """@param bot:"""
    await bot.add_cog(Ping(bot))
