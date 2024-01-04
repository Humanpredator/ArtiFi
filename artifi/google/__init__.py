"""Collection Of Google API's"""
import os
from random import randrange

from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from artifi import Artifi


class Google:
    """Base class for Google API's"""

    def __init__(self, context):
        """@param context: pass :class Artifi"""
        self.context: Artifi = context

    def oauth_creds(self, scope, cname='token', service_user=False):
        """
        This method used to gain access via Oauth-client
        @param service_user:
        @param cname:
        @param scope: list access scope to get access for specific resource
        @return: token pickle
        """
        if not scope:
            raise ValueError("Scope Required...!")
        credential_path = os.path.join(self.context.cwd, "credentials.json")
        token_dir = os.path.join(self.context.directory, '.gtoken')
        os.makedirs(token_dir, exist_ok=True)
        token_path = os.path.join(token_dir, f"{cname}.json")
        creds = None

        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, scope)

        if service_user:
            accounts = randrange(len(os.listdir("accounts")))
            creds = service_account.Credentials.from_service_account_file(
                f'accounts/{accounts}.json')

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(credential_path):
                    raise FileNotFoundError("Opps!, credentials.json Not Found...!")

                flow = InstalledAppFlow.from_client_secrets_file(
                    credential_path, scope
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_path, "w") as token:
                token.write(creds.to_json())
        self.context.logger.info(f"{cname} Token loaded Successfully")

        return creds
