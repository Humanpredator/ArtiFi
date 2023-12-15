import os
import pickle
import time
from glob import glob
from pathlib import Path
from platform import system
from sqlite3 import OperationalError, connect

from instaloader import (
    Highlight,
    Instaloader,
    InstaloaderContext,
    Post,
    Profile,
    StoryItem,
)

from artifi import Artifi
from artifi.instagram.misc.custom_func import CustomContext


class Instagram(Instaloader):
    def __init__(self, context, ig_username, ig_password):
        super().__init__()
        self.acontext: Artifi = context
        self.context: InstaloaderContext = CustomContext(self.acontext)
        self.download_video_thumbnails: bool = False
        self.save_metadata: bool = False
        self.compress_json: bool = False
        self._ig_username: str = ig_username
        self._ig_password: str = ig_password
        self._session_file: str = os.path.join(
            self.acontext.cwd, f"{ig_username}_ig.session"
        )
        self._status: bool = self._insta_session()

    @staticmethod
    def _get_cookie_path() -> str:
        default_cookie_path = {
            "Windows": "~/AppData/Roaming/Mozilla/Firefox/Profiles/*/cookies.sqlite",
            "Darwin": "~/Library/Application Support/Firefox/Profiles/*/cookies.sqlite",
        }.get(system(), "~/.mozilla/firefox/*/cookies.sqlite")

        cookie_paths = glob(os.path.expanduser(default_cookie_path))
        return cookie_paths[0] if cookie_paths else None

    def _fetch_and_save_cookies(self) -> str:
        cookie_path = self._get_cookie_path()

        if cookie_path:
            conn = connect(f"file:{cookie_path}?immutable=1", uri=True)

            try:
                cursor = conn.execute(
                    "SELECT name, value FROM moz_cookies WHERE baseDomain='instagram.com'"
                )
                cookie_data = cursor.fetchall()
            except OperationalError:
                cursor = conn.execute(
                    "SELECT name, value FROM moz_cookies WHERE host LIKE '%instagram.com'"
                )
                cookie_data = cursor.fetchall()

            with open(self._session_file, "wb") as file:
                cookies_dict = dict(cookie_data)
                pickle.dump(cookies_dict, file)

            self.save_session_to_file(self._ig_username, self._session_file)

        return cookie_path

    def _insta_session(self) -> bool:
        try:
            cookie = self._fetch_and_save_cookies()
            if not cookie:
                return bool(0)
            self.load_session_from_file(self._ig_username, self._session_file)
            return bool(self.test_login())
        except:
            self.acontext.logger.info("Unable to Login Using Session File")
            return bool(0)

    @staticmethod
    def file_name(name: str, post: Post | StoryItem) -> str:
        post_time = post.date
        year = post_time.year % 100
        month = post_time.month
        day = post_time.day
        hour = post_time.hour
        minute = post_time.minute
        postdt = f"{year:02d}{month:02d}{day:02d}{hour:02d}{minute:02d}"
        post_sid = post.shortcode[:5]
        file_pattern = f"{name[:5]}{postdt}{post_sid}"
        return file_pattern

    @staticmethod
    def sanitize(string) -> str:
        forbidden_chars_windows = {"<", ">", ":", '"', "/", "\\", "|", "?", "*"}
        forbidden_chars_linux = {"/"}
        if os.name == "nt":
            forbidden_chars = forbidden_chars_windows
        else:
            forbidden_chars = forbidden_chars_linux
        sanitize_string = "".join(
            char for char in string if char not in forbidden_chars
        )
        return sanitize_string

    def download_posts(self, user_name) -> None:
        profile: Profile = Profile.from_username(self.context, user_name.strip())
        post_path = os.path.join(self.acontext.directory, str(profile.userid), "Posts")
        os.makedirs(post_path, exist_ok=True)
        user_posts = profile.get_posts()
        for post in user_posts:
            self.filename_pattern = self.file_name(profile.full_name, post)
            time.sleep(2)
            self.download_post(post, target=Path(post_path))
        self.acontext.logger.info(f"{profile.username} Post Was Downloaded...!")

    def download_album(self, user_name) -> None:
        profile: Profile = Profile.from_username(self.context, user_name.strip())
        highlight_path = os.path.join(
            self.acontext.directory, str(profile.userid), "Highlights"
        )
        os.makedirs(highlight_path, exist_ok=True)
        for user_highlight in self.get_highlights(profile):
            user_highlight: Highlight = user_highlight
            album_name = str(user_highlight.title)
            album_path = os.path.join(highlight_path, self.sanitize(album_name))
            os.makedirs(album_path, exist_ok=True)
            try:
                for index, highlights in enumerate(user_highlight.get_items()):
                    self.filename_pattern = self.file_name(
                        profile.full_name, highlights
                    )
                    time.sleep(2)
                    self.download_storyitem(highlights, target=Path(album_path))
                self.acontext.logger.info(f"{album_name} Was Downloaded...!")
            except Exception as e:
                self.acontext.logger.error(f"Something Went Wrong: {e}")
                time.sleep(10)
        self.acontext.logger.info(f"{profile.full_name} Highlights Was Downloaded...!")
