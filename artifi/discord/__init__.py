import os
import time
from typing import Any

import discord
import wavelink
from discord.ext.commands import Bot, Context
from wavelink import Node
from wavelink.ext.spotify import SpotifyClient

from artifi import Artifi
from artifi.discord.misc import custom_command, custom_function, discord_model
from artifi.discord.misc.custom_command import MyHelpCommand
from artifi.discord.misc.custom_function import send_message
from artifi.discord.misc.discord_model import DiscordSudoModel


class Discord(Bot):
    def __init__(
        self,
        context,
        command_prefix="!",
        *,
        intents=discord.Intents.all(),
        **options: Any,
    ):
        super().__init__(command_prefix, intents=intents, **options)
        self.bot_start_time = time.time()
        self.context: Artifi = context
        self.load_default = True
        self.help_command = MyHelpCommand()
        self.spotify_client: SpotifyClient = SpotifyClient(
            client_id=self.context.DISCORD_SPOTIFY_CLIENT,
            client_secret=self.context.DISCORD_SPOTIFY_SECRET,
        )
        self.wave_link_node: Node = Node(
            uri=self.context.DISCORD_LAVALINK_URI,
            password=self.context.DISCORD_LAVALINK_PASSWORD,
        )
        DiscordSudoModel(self.context).__table__.create(
            self.context.db_engine, checkfirst=True
        )

    def get_all_users(self) -> list:
        with self.context.db_session() as session:
            user_data = session.query(DiscordSudoModel).all()
        return [user.user_id for user in user_data]

    def owner_only(self, ctx: Context) -> bool:
        author_id = ctx.author.id
        return bool(author_id == self.context.DISCORD_OWNER_ID)

    def sudo_only(self, ctx: Context) -> bool:
        if isinstance(ctx, Context):
            author_id = ctx.author.id
            return bool(
                author_id in self.get_all_users()
                or author_id == self.context.DISCORD_OWNER_ID
            )
        elif isinstance(ctx, int):
            return bool(
                ctx in self.get_all_users() or ctx == self.context.DISCORD_OWNER_ID
            )
        else:
            return bool(0)

    async def _load_default(self) -> None:
        if self.load_default:
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
            wave_link_status = await wavelink.NodePool.connect(
                client=self, nodes=[self.wave_link_node], spotify=self.spotify_client
            )
            self.context.logger.info(f"WaveLink Status: {wave_link_status}")
        self.context.logger.info(f"Discord Bot Online...!")

    def run_bot(self):
        self.add_listener(self._load_default, "on_ready")
        return self.run(self.context.DISCORD_BOT_TOKEN, log_handler=None)
