import importlib.util
import os
import sys

from dotenv import load_dotenv

from artifi.config.exception import ConfigFileError


class BaseConfig:
    def __init__(self, import_name, config_path):
        self._import_name = import_name
        self._env_path: str = config_path
        if not self._env_path:
            self._env_path = os.path.join(self.get_root_path(), "config.env")
        if (file_state := os.path.exists(self._env_path)) and not load_dotenv(
            self._env_path
        ):
            raise ConfigFileError("Failed to Load Config.env File")
        """Basic Config"""
        self.SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")
        self.API_SECRET_KEY = os.getenv("API_SECRET_KEY")
        """CloudFlare Config"""
        self.CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID")
        self.CLOUDFLARE_ACCOUNT_TOKEN = os.getenv("CLOUDFLARE_ACCOUNT_TOKEN")
        """Discord Config"""
        self.DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
        self.DISCORD_OWNER_ID = int(os.getenv("DISCORD_OWNER_ID", 1))
        self.DISCORD_SPOTIFY_CLIENT = os.getenv("DISCORD_SPOTIFY_CLIENT_ID")
        self.DISCORD_SPOTIFY_SECRET = os.getenv("DISCORD_SPOTIFY_CLIENT_SECRET")
        self.DISCORD_LAVALINK_URI = os.getenv("DISCORD_LAVALINK_URI")
        self.DISCORD_LAVALINK_PASSWORD = os.getenv("DISCORD_LAVALINK_PASSWORD")
        """Instagram Config"""
        self.INSTAGRAM_USERNAME = os.getenv("INSTAGRAM_USERNAME")
        self.INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASSWORD")
        """Whatsapp Config"""
        self.WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
        self.WHATSAPP_WEBHOOK_SECRET = os.getenv("WHATSAPP_WEBHOOK_SECRET")
        self.WHATSAPP_NUMBER_ID = os.getenv("WHATSAPP_NUMBER_ID")

        if not file_state:
            self._generate_config_file()
            raise Exception(
                "config.env File Is Missing On Your Current Directory, So We Have Created One For you, Please Fill Up And Re-Run...!"
            )

    def _generate_config_file(self) -> None:
        keys = [
            f"{config_key} = ''"
            for config_key in list(self.__dict__.keys())
            if not config_key.startswith("_")
        ]
        key = "\n".join(keys)
        with open(os.path.join(self._env_path), "w+") as f:
            f.write(key)
        f.close()

    def get_root_path(self) -> str:
        if not self._import_name:
            raise Exception("App Name Required")
        mod = sys.modules.get(self._import_name)
        if mod is not None and hasattr(mod, "__file__") and mod.__file__ is not None:
            return os.path.dirname(os.path.abspath(mod.__file__))
        try:
            spec = importlib.util.find_spec(self._import_name)
            if spec is None:
                raise ValueError
        except (ImportError, ValueError):
            loader = None
        else:
            loader = spec.loader
        if loader is None:
            return os.getcwd()
        if hasattr(loader, "get_filename"):
            filepath = loader.get_filename(self._import_name)
        else:
            __import__(self._import_name)
            mod = sys.modules[self._import_name]
            filepath = getattr(mod, "__file__", None)
            if filepath is None:
                raise RuntimeError(
                    f" {self._import_name!r} the root path needs to be explicitly provided."
                )
        return os.path.dirname(os.path.abspath(filepath))
