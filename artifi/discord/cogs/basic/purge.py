from discord.ext.commands import Cog, Context, command

from artifi.discord import Discord, send_message


class Purge(Cog):

    def __init__(self, bot):
        self._bot: Discord = bot

    @command('purge', help="Delete The Given Amount Of Message.")
    async def purge_messages(self, ctx: Context, *args):
        if not self._bot.owner_only(ctx):
            return await send_message(ctx, "Access Denied...!")
        elif not args:
            return await send_message(ctx, "Amount is Required..!")
        else:
            count = args[0]
            await send_message(ctx, f"Purging {count} Messages")
            await ctx.channel.purge(limit=int(count) + 1)


async def setup(bot):
    await bot.add_cog(Purge(bot))
