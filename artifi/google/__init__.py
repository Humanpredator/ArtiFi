"""Collection Of Google API's"""
import os
import pickle

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

from artifi import Artifi


class Google:
    """Base class for Google API's"""

    def __init__(self, context):
        """@param context: pass :class Artifi"""
        self.context: Artifi = context

    def oauth_creds(self, scope):
        """
        This method used to gain access via Oauth-client
        @param scope: list access scope to get access for specific resource
        @return: token pickle
        """
        if not scope:
            raise ValueError("Scope Required...!")
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
                    raise FileNotFoundError("Opps!, credentials.json Not Found...!")
                flow = InstalledAppFlow.from_client_secrets_file(credential_path, scope)
                creds = flow.run_local_server(port=0)
            with open(token_path, "wb") as token:
                pickle.dump(creds, token)
        self.context.logger.info("Token Fetched Successfully")
        return creds
