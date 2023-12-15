from datetime import datetime

from discord import Embed
from discord.ext.commands import Cog, command

from artifi.discord import DiscordSudoModel, Discord
from artifi.discord.misc.custom_function import send_message


class Auth(Cog):
    def __init__(self, bot):
        super().__init__()
        self._bot: Discord = bot

    @command("promote", help="Reply To The Message Of User, Who Want To Promote.")
    async def promote_user(self, ctx):
        if not self._bot.owner_only(ctx):
            return await send_message(ctx, "Access Denied...!")
        elif ctx.message.reference:
            original_message = await ctx.fetch_message(ctx.message.reference.message_id)
            original_author_id = original_message.author.id
            with self._bot.context.db_session() as session:
                user_data = session.query(DiscordSudoModel).filter(
                    DiscordSudoModel.user_id == str(original_author_id)).first()
                if user_data:
                    response = "User Already Sudo"
                else:
                    user_data = DiscordSudoModel(self._bot.context)
                    user_data.user_id = str(original_author_id).strip()
                    user_data.created_at = datetime.now()
                    response = "User Promoted To Sudo"
                    session.add(user_data)
                    session.commit()

            await send_message(ctx, response)
        else:
            await send_message(ctx, 'Usage !addsudo reply to this to user...!')

    @command("demote", help="Reply To The Message Of User, Who Want To De-Promote.")
    async def demote_user(self, ctx):
        if not self._bot.owner_only(ctx):
            return await send_message(ctx, "Access Denied...!")
        elif ctx.message.reference:
            original_message = await ctx.fetch_message(ctx.message.reference.message_id)
            original_author_id = original_message.author.id
            with self._bot.context.db_session() as session:
                user_data = session.query(DiscordSudoModel).filter(
                    DiscordSudoModel.user_id == str(original_author_id)).first()
                if not user_data:
                    response = "Users Not A Sudo!"
                else:
                    session.delete(user_data)
                    response = "Users De-Promoted"
                    session.commit()
            await send_message(ctx, response)
        else:
            await send_message(ctx, 'Usage !rmvsudo reply to this to user...!')

    @command("showsudo", help="Get All Sudo Users ID's.")
    async def show_sudo(self, ctx):
        if not self._bot.owner_only(ctx):
            return await send_message(ctx, "Access Denied...!")
        user_data = self._bot.get_all_users()
        emd = Embed(timestamp=datetime.now())
        emd.add_field(name="Sudo Users", value="\n".join(str(user.user_id) for user in user_data), inline=False)
        await send_message(ctx, embed=emd)


async def setup(bot):
    await bot.add_cog(Auth(bot))
