"""Speedtest Cog"""
from datetime import datetime

from discord import Embed
from discord.ext.commands import Cog, command
from speedtest import Speedtest

from artifi.discord import Discord
from artifi.discord.misc.discord_func import edit_message, send_message
from artifi.utils import speed_convert


class NetworkTest(Cog):
    """To Test the Network Speed"""

    def __init__(self, bot):
        """@param bot:"""
        self._bot: Discord = bot

    @command("speedtest", help="Check The Internet Speed Of The Hosted Server.")
    async def speedtest(self, ctx):
        """

        @param ctx:
        @return:
        """
        if not self._bot.sudo_only(ctx):
            return await send_message(ctx, "Access Denied...!")
        msg = await send_message(ctx, "Starting Network Test...!", reply=True)
        test = Speedtest()
        test.get_best_server()
        test.download()
        test.upload()
        test.results.share()
        result = test.results.dict()
        emd = Embed(timestamp=datetime.now())
        network_string = f"""
Name:***{result['server']['name']}***
Country: ***{result['server']['country']}, {result['server']['cc']}***
Sponsor: ***{result['server']['sponsor']}***
ISP: ***{result['client']['isp']}***"""
        emd.add_field(name="Network Detail", value=network_string)
        result_string = f"""
Upload: ***{speed_convert(result['upload'] / 8)}***
Download: ***{speed_convert(result['download'] / 8)}***
Ping: ***{result['ping']} ms***
ISP Rating: ***{result['client']['isprating']}***
"""
        emd.add_field(name="Network Result", value=result_string)
        await edit_message(msg, embed=emd)


async def setup(bot):
    """@param bot:"""
    await bot.add_cog(NetworkTest(bot))
