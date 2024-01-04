"""Collection of Cloudflare API's"""

from requests import Session

from artifi import Artifi


class CloudFlare:
    """Base Class for All Cloudflare App's"""

    def __init__(self, context):
        """
        @param context : pass the Artifi
         example_usage: arti = Artifi(__name__)
                        cf = Cloudflare(arti)
        """
        self._base_url: str = "https://api.cloudflare.com"
        self.service: str = "client"
        self.version: str = "v4"
        self.context: Artifi = context
        self.account_id: str = self.context.CLOUDFLARE_ACCOUNT_ID
        self.account_token: str = self.context.CLOUDFLARE_ACCOUNT_TOKEN
        self._chat_data: dict = {}

    @property
    def _cfrequest(self) -> Session:
        """@return: return the requests session"""
        _session = Session()
        _session.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.account_token}",
        }
        return _session
