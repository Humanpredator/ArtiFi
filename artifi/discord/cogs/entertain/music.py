import datetime
import re
import uuid

import wavelink
from discord import ButtonStyle, Embed, Message, GroupChannel, NotFound
from discord.ext import tasks
from discord.ext.commands import Cog, Context, command
from discord.ui import View, button
from wavelink.ext import spotify

from artifi.discord import Discord
from artifi.discord.misc.custom_function import edit_message, send_message, delete_message


class PlayerControl(View):
    def __init__(self, bot, voice_client=None, channel=None, message=None):
        super().__init__()
        self._bot: Discord = bot
        self.voice_client: wavelink.player = voice_client
        self.channel: GroupChannel = channel
        self.message: Message = message
        self.update_status_task.start()
        self._control_id = str(uuid.uuid4())
        self._bot.context.logger.info(f"{self._control_id}: Music player Status Updating Task Started...!")

    def _vc_status(self):
        if self.voice_client.is_paused():
            return "Paused"
        elif self.voice_client.is_playing():
            return "Playing"
        else:
            return "Stopped"

    async def verify_message(self):
        try:
            return await self.channel.fetch_message(self.message.id)
        except NotFound:
            return None

    @tasks.loop(seconds=5)
    async def update_status_task(self, _delete=None):
        if not (message := await self.verify_message()) or not self.voice_client.is_connected():
            self._bot.context.logger.info(f"{self._control_id}: Music player Status Updating Task Disposed...!")
            if self.update_status_task.is_running():
                self.update_status_task.stop()
            return True
        elif current_track := self.voice_client.current:
            embed = Embed(title="Player Control", timestamp=datetime.datetime.now())
            current_time = datetime.timedelta(milliseconds=self.voice_client.position)
            total_time = datetime.timedelta(milliseconds=current_track.duration)
            current_time_str = str(current_time).split(".")[0]
            total_time_str = str(total_time).split(".")[0]
            embed.add_field(name="Now Playing", value=f"**{current_track.title}**", inline=False)
            embed.add_field(name="Status", value=self._vc_status(), inline=True)
            embed.add_field(name="Time", value=f"{current_time_str}/{total_time_str}", inline=True)
            embed.add_field(name="Volume", value=f"{self.voice_client.volume}%", inline=True)
            return await edit_message(message, embed=embed, markup=self, delete=_delete)

    @button(label="Play/Pause", style=ButtonStyle.green)
    async def play_button(self, *args):
        interaction = args[0]
        if not self._bot.sudo_only(interaction.user.id):
            return await interaction.response.defer()
        if self.voice_client.is_paused():
            await self.voice_client.resume()
        else:
            await self.voice_client.pause()
        await self.update_status_task()
        return await interaction.response.defer()

    @button(label="Next", style=ButtonStyle.grey)
    async def next_button(self, *args):
        interaction = args[0]
        if not self._bot.sudo_only(interaction.user.id):
            return await interaction.response.defer()
        if not self.voice_client.queue.is_empty:
            next_track = self.voice_client.queue.get()
            await self.voice_client.play(next_track)
        await self.update_status_task()
        return await interaction.response.defer()

    @button(label="Stop", style=ButtonStyle.red)
    async def stop_button(self, *args):
        interaction = args[0]
        if not self._bot.sudo_only(interaction.user.id):
            return await interaction.response.defer()
        if self.update_status_task.is_running():
            self.update_status_task.stop()
        await self.voice_client.stop()

        self.voice_client.queue.clear()
        await self.update_status_task(_delete=2)
        await self.voice_client.disconnect()
        return await interaction.response.defer()

    @button(label="Vol +", style=ButtonStyle.primary, row=2)
    async def increase_volume(self, *args):
        interaction = args[0]
        if not self._bot.sudo_only(interaction.user.id):
            return await interaction.response.defer()
        volume = int(self.voice_client.volume) + 10
        await self.voice_client.set_volume(volume)
        await self.update_status_task()
        return await interaction.response.defer()

    @button(label="Vol -", style=ButtonStyle.primary, row=2)
    async def decrease_volume(self, *args):
        interaction = args[0]
        if not self._bot.sudo_only(interaction.user.id):
            return await interaction.response.defer()
        volume = int(self.voice_client.volume) - 10
        await self.voice_client.set_volume(volume)
        await self.update_status_task()
        return await interaction.response.defer()


class Music(Cog):
    def __init__(self, bot):
        self._bot: Discord = bot
        self.music_player: dict = {}

    @staticmethod
    def is_url(url) -> bool:
        # Regular expression to check if the input is a URL
        url_pattern = r"^(https?://)?[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}(/\S*)?$"
        return bool(re.match(url_pattern, url))

    def check_url_service(self, url) -> str:
        spotify_pattern = r"^(https?://)?(www\.)?open\.spotify\.com/.*"
        youtube_pattern = r"^(https?://)?(www\.)?youtube\.com/.*"

        if self.is_url(url):
            if re.match(spotify_pattern, url):
                return "SPOTIFY"
            elif re.match(youtube_pattern, url):
                return "YOUTUBE"
            else:
                return "OTHERURL"
        else:
            return "SEARCH"

    @command('play', help="Send YT URL or Spotify URL or Track Name Followed By Command.")
    async def play(self, ctx: Context, *args: str):
        if not self._bot.sudo_only(ctx):
            return await send_message(ctx, "Access Denied...!")

        message = await send_message(ctx, "Checking The Track, Please Wait..!", reply=True)
        track_name_url = ' '.join(args)

        if not track_name_url:
            return await edit_message(message, "Track Name Or URL Is Required....!")
        if not ctx.author.voice:
            return await edit_message(message, "You are not in a voice channel. Please join one to use this command.")

        search_track = self.check_url_service(track_name_url)

        if search_track in ["YOUTUBE", "SEARCH"]:
            tracks = await wavelink.YouTubeTrack.search(track_name_url)
            if search_track == "SEARCH" and len(tracks) > 0:
                tracks = [tracks[0]]
        elif search_track in ["SPOTIFY"]:
            tracks = await spotify.SpotifyTrack.search(track_name_url)
        else:
            return await edit_message(message, "Unable To Find The Song...!")

        if not tracks:
            return await edit_message(message, 'This is not a valid URL.')

        music_player: PlayerControl = self.music_player.get(ctx.guild.id)
        if music_player:
            await delete_message(music_player.message)
            music_player.message = message
            music_player.channel = ctx.channel
        else:
            # noinspection PyTypeChecker
            voice_client = await ctx.author.voice.channel.connect(cls=wavelink.Player)
            voice_client.autoplay = True
            await voice_client.set_volume(10)
            music_player = PlayerControl(self._bot, voice_client=voice_client, channel=ctx.channel, message=message)

        for track in tracks:
            await music_player.voice_client.queue.put_wait(track)
            if not music_player.voice_client.is_playing() and not music_player.voice_client.is_paused():
                await music_player.voice_client.play(track, populate=True)

        self.music_player[ctx.guild.id] = music_player
        await music_player.update_status_task()


async def setup(bot):
    await bot.add_cog(Music(bot))
