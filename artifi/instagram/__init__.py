"""Collection Of Unofficial Instagram API's"""
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
from artifi.instagram.ext import CustomContext
from artifi.utils import sanitize_name


class Instagram(Instaloader):
    """Download instagram user post and highlights using Instaloader"""

    def __init__(self, context, ig_username, ig_password):
        """
        Logging in on instagram using instaloader will cause temporary ban if your
        using on cloud server of another ip.So It's recommended to first logging the
        instagram on the firefox. Currently, this script will access the
        firefox and fetch the instagram cookie from there.
        @note If something not working then login again on firefox and run this script
              again.
        @param context: Pass :class Artifi
        @param ig_username: Instagram username
        @param ig_password: Instagram password
        """
        super().__init__()
        self.acontext: Artifi = context
        self.context: InstaloaderContext = CustomContext(self.acontext)
        self.download_video_thumbnails: bool = False
        self.save_metadata: bool = False
        self.compress_json: bool = False
        self.filename_pattern = "{profile}_UTC_{date_utc}"
        self._ig_username: str = ig_username
        self._ig_password: str = ig_password
        self._session_file: str = os.path.join(
            self.acontext.cwd, f"{ig_username}_ig.session"
        )
        self._status: bool = self._insta_session()

    @staticmethod
    def _get_cookie_path() -> str:
        """
        Fetch the instagram cookies path from firefox
        @return:  cookie path
        """
        winp = "~/AppData/Roaming/Mozilla/Firefox/Profiles/*/cookies.sqlite"
        linp = "~/Library/Application Support/Firefox/Profiles/*/cookies.sqlite"
        browser_cookie_path = {
            "Windows": winp,
            "Darwin": linp,
        }.get(system(), "~/.mozilla/firefox/*/cookies.sqlite")

        cookie_paths = glob(os.path.expanduser(browser_cookie_path))
        return cookie_paths[0] if cookie_paths else None

    def _fetch_and_save_cookies(self) -> str:
        """
        Save cookie file from firefox
        @return: saved cookie path
        """
        cookie_path = self._get_cookie_path()
        qry1 = "SELECT name, value FROM moz_cookies WHERE baseDomain='instagram.com'"
        qry2 = "SELECT name, value FROM moz_cookies WHERE host LIKE '%instagram.com'"
        if cookie_path:
            conn = connect(f"file:{cookie_path}?immutable=1", uri=True)
            try:
                cursor = conn.execute(qry1)
                cookie_data = cursor.fetchall()
            except OperationalError:
                cursor = conn.execute(qry2)
                cookie_data = cursor.fetchall()

            with open(self._session_file, "wb") as file:
                cookies_dict = dict(cookie_data)
                pickle.dump(cookies_dict, file)

            self.save_session_to_file(self._ig_username, self._session_file)

        return cookie_path

    def _insta_session(self) -> bool:
        """
        Used to validate saved cookie
        @return: True = valid, False = Invalid
        """
        cookie = self._fetch_and_save_cookies()
        if not cookie:
            return bool(0)
        self.load_session_from_file(self._ig_username, self._session_file)
        return bool(self.test_login())

    @staticmethod
    def file_name(name: str, post: Post | StoryItem) -> str:
        """
        Create unique filename for the file
        @param name: prefix name for the filename first 5-letter will be used
        @param post: post object
        @return: file name :example Name_YYMMDDHHSS_post.id
        """
        post_time = post.date
        year = post_time.year % 100
        month = post_time.month
        day = post_time.day
        hour = post_time.hour
        minute = post_time.minute
        post_dt = f"{year:02d}{month:02d}{day:02d}{hour:02d}{minute:02d}"
        post_sid = post.shortcode[:5]
        file_pattern = f"{name[:5]}{post_dt}{post_sid}"
        return file_pattern

    def download_posts(self, user_name) -> None:
        """
        Download all the instagram posts of the given username
        @param user_name: Instagram username
        """
        profile: Profile = Profile.from_username(self.context, user_name.strip())
        post_path = os.path.join(self.acontext.directory, str(profile.userid),
                                 "Posts")
        os.makedirs(post_path, exist_ok=True)
        user_posts = profile.get_posts()
        for post in user_posts:
            self.filename_pattern = self.file_name(profile.full_name, post)
            time.sleep(2)
            self.download_post(post, target=Path(post_path))
        self.acontext.logger.info(f"{profile.username} Post Was Downloaded!")

    def download_album(self, user_name) -> None:
        """
        Download all highlights of given username
        @param user_name: Instagram username
        """
        profile: Profile = Profile.from_username(self.context, user_name.strip())
        highlight_path = os.path.join(
            self.acontext.directory, str(profile.userid), "Highlights"
        )
        os.makedirs(highlight_path, exist_ok=True)
        for user_highlight in self.get_highlights(profile):
            user_highlight: Highlight = user_highlight
            album_name = str(user_highlight.title)
            album_path = os.path.join(
                highlight_path, sanitize_name(album_name)
            )
            os.makedirs(album_path, exist_ok=True)
            for highlights in user_highlight.get_items():
                self.filename_pattern = self.file_name(profile.full_name, highlights)
                time.sleep(2)
                self.download_storyitem(highlights, target=Path(album_path))
            self.acontext.logger.info(f"{album_name} Was Downloaded...!")
        self.acontext.logger.info(f"{profile.full_name} Highlights Was Downloaded!")
