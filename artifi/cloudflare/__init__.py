from requests import Session

from artifi import Artifi


class CloudFlare:
    def __init__(self, context):
        self._base_url: str = "https://api.cloudflare.com"
        self.service: str = "client"
        self.version: str = "v4"
        self.context: Artifi = context
        self.account_id: str = self.context.CLOUDFLARE_ACCOUNT_ID
        self.account_token: str = self.context.CLOUDFLARE_ACCOUNT_TOKEN
        self._chat_data: dict = {}

    @property
    def _request(self) -> Session:
        _session = Session()
        _session.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.account_token}",
        }
        return _session
