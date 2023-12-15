from discord.ext.commands import Cog

from artifi.discord import Discord


class ErrorHandler(Cog):
    def __init__(self, bot):
        self._bot: Discord = bot

    @Cog.listener(name="on_error")
    async def discord_error(self, event, *args, **kwargs):
        self._bot.context.logger.error(f"Something Went Wrong On Discord: | {event} |, | {args} |, | {kwargs} |")


async def setup(bot):
    await bot.add_cog(ErrorHandler(bot))
