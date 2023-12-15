import os
import pickle

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

from artifi import Artifi


class Google:
    def __init__(self, context):
        self.context: Artifi = context
        self._default_scopes: list = [
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/contacts",
        ]
        self._creds = self._get_auth()

    def _get_auth(self, scope=None):
        if not scope:
            scope: list = self._default_scopes
        credential_path = os.path.join(self.context.cwd, "credentials.json")
        token_path = os.path.join(self.context.cwd, "token.pickle")
        creds = None
        if os.path.exists(token_path):
            with open(token_path, "rb") as token:
                creds = pickle.load(token)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(credential_path):
                    self.context.logger.error("Opps!, credentials.json Not Found...!")
                    raise FileNotFoundError
                flow = InstalledAppFlow.from_client_secrets_file(credential_path, scope)
                creds = flow.run_local_server(port=0)
            with open(token_path, "wb") as token:
                pickle.dump(creds, token)
        self.context.logger.info("Token Fetched Successfully")
        return creds

    def authorize(self, scope: list):
        return self._get_auth(scope)
