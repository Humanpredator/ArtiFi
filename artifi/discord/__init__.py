"""Discord Bot Using Discord Py"""
import os
import time
from typing import Any, List, Optional

import discord
import wavelink
from discord.ext.commands import Bot, Context

from artifi import Artifi
from artifi.discord.misc.discord_command import MyHelpCommand
from artifi.discord.misc.discord_model import DiscordSudoModel


class Discord(Bot):
    """Discord Bot"""

    def __init__(
        self,
        context,
        command_prefix="!",
        *,
        intents=discord.Intents.all(),
        **options: Any,
    ):
        """

        @param context: pass :class Artifi
        @param command_prefix: Symbol used to invoke the commands
        @param intents: Scope to be used
        @param options: optional key=value
        """
        super().__init__(command_prefix, intents=intents, **options)
        self.bot_start_time = time.time()
        self.context: Artifi = context
        self.load_default = True
        self.help_command = MyHelpCommand()
        self.db_tables: Optional[List[Artifi.dbmodel]] = None
        self.context.create_db_table(self.db_tables)

    def get_all_users(self) -> list:
        """
        Get all user who have access to invoke command
        @return:
        """
        with self.context.db_session() as session:
            user_data = session.query(DiscordSudoModel).all()
        return [user.user_id for user in user_data]

    def owner_only(self, ctx: Context) -> bool:
        """
        Check the command invoked by Owner
        @param ctx: discord context
        @return:
        """
        author_id = ctx.author.id
        return bool(author_id == self.context.DISCORD_OWNER_ID)

    def sudo_only(self, ctx: Context) -> bool:
        """
        Check the command invoked by Sudo user on DB
        @param ctx: discord context
        @return:
        """
        if isinstance(ctx, Context):
            author_id = ctx.author.id
            return bool(
                author_id in self.get_all_users()
                or author_id == self.context.DISCORD_OWNER_ID
            )
        if isinstance(ctx, int):
            return bool(
                ctx in self.get_all_users() or ctx == self.context.DISCORD_OWNER_ID
            )
        return bool(0)

    async def _load_default(self) -> None:
        """@return:"""
        if self.load_default:
            self.context.create_db_table([DiscordSudoModel])
            self.context.logger.info("Loading Default Cogs, Please Wait...!")
            cog_dir = os.path.join(self.context.module_path, "discord", "cogs")
            for root, _, files in os.walk(cog_dir):
                for filename in files:
                    if filename.endswith(".py") and filename != "__init__.py":
                        rel_path = os.path.relpath(
                            os.path.join(root, filename), self.context.module_path
                        )
                        cog_module = os.path.splitext(rel_path)[0].replace(
                            os.path.sep, "."
                        )
                        cog_module = f"artifi.{cog_module}"
                        await self.load_extension(cog_module)
            self.context.logger.info("All Cogs Were Loaded..!")
            wave_link_node: wavelink.Node = wavelink.Node(
                uri=self.context.DISCORD_LAVALINK_URI,
                password=self.context.DISCORD_LAVALINK_PASSWORD,
            )
            wave_link_status = await wavelink.Pool.connect(
                client=self, nodes=[wave_link_node]
            )
            self.context.logger.info(f"WaveLink Status: {wave_link_status}")
        self.context.logger.info("Discord Bot Online...!")

    def run_bot(self):
        """
        Run the Discord bot with some default cogs
        @return:
        """
        self.add_listener(self._load_default, "on_ready")
        return self.run(self.context.DISCORD_BOT_TOKEN, log_handler=None)
