import asyncio
import hashlib
import json
import os
import pickle
import time
from contextlib import suppress
from typing import Optional

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from playwright.sync_api import sync_playwright
from requests import Session

from artifi import Artifi


class Google:
    def __init__(self, context):
        self.context: Artifi = context

    def oauth_creds(self, scope):
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


class GoogleWebSession(Google):
    def __init__(self, context, email, password, headless, param_key, user_agent):
        super().__init__(context)
        self.context: Artifi = context
        self._chrome_path = self.context.CHROMEDRIVE_PATH
        self._headless = headless
        self._email: str = email
        self._session_path = os.path.join(self.context.directory, f"{self._email}.json")
        self._password: str = password
        self.auth_key: str = param_key
        self._user_agent: str = user_agent
        self._session_token: str = Optional[str]
        self._channel_id: str = Optional[str]
        self.gauth_id = "0"

    def _header_cookies(self):
        required_cookie_field = [
            "__Secure-3PAPISID",
            "__Secure-3PSIDTS",
            "__Secure-3PSID",
        ]
        cookie_field = []
        sapid_value = None
        cd = self._load_session()
        for data in cd.get("cookies", []):
            if data["name"] in required_cookie_field:
                cookie_field.append(f"{data['name']}={data['value']}")
            if data["name"] == "SAPISID":
                sapid_value = data["value"]
        return "; ".join(set(cookie_field)), sapid_value

    def _load_session(self):
        with suppress(Exception):
            with open(self._session_path, "r") as f:
                return json.load(f)
        return None

    def _save_session(self, data):
        with suppress(Exception):
            with open(self._session_path, "w") as f:
                json.dump(data, f)

    @staticmethod
    async def _sapisid_hash(sapisid):
        origin = "https://studio.youtube.com"
        timestamp_ms = int(time.time() * 1000)
        data_to_hash = f"{timestamp_ms} {sapisid} {origin}"
        encoded_str = data_to_hash.encode("utf-8")
        digest = await asyncio.to_thread(hashlib.sha1, encoded_str)
        return f"{timestamp_ms}_{digest.hexdigest()}"

    def _intercept_response(self, response):
        session_token_url = f"https://studio.youtube.com/youtubei/v1/ars/grst?alt=json&key={self.auth_key}"
        if session_token_url in response.url:
            data = response.json()
            self._session_token = data.get("sessionToken")

    def _fetch_cookie_session(self):
        self.context.logger.info(
            f"Setting Up Session For {self._email}, Please Wait...!"
        )
        with sync_playwright() as p:
            browser = p.chromium.launch(
                executable_path=self._chrome_path,
                headless=self._headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            session_data = self._load_session()
            browser_context = browser.new_context(
                storage_state=session_data,
                user_agent=self._user_agent,
                java_script_enabled=True,
            )
            page = browser_context.new_page()
            page.on("response", self._intercept_response)

            page.goto("https://studio.youtube.com")
            if "accounts.google.com" in page.url:
                self.context.logger.info("Logging In With Given Credentials")
                page.fill('input[type="email"]', f"{self._email}")
                page.click("div#identifierNext")

                page.wait_for_selector('input[type="password"]', state="visible")
                page.fill('input[type="password"]', f"{self._password}")
                page.click("div#passwordNext")
                time.sleep(5)
                page.wait_for_selector(
                    '[id="menu-paper-icon-item-1"]', state="attached"
                )
                session_data = browser_context.storage_state()
                self._save_session(session_data)
                self.context.logger.info("Login Successfully...!")

            browser_context.storage_state = session_data
            self.context.logger.info("Almost There Just Validating Session...!")
            page.wait_for_timeout(10000)
            self._channel_id = page.url.split("/")[-1]
            browser_context.close()

    def google_websession(self) -> Session:
        self._fetch_cookie_session()
        default_session = Session()
        header_cookie, sapid_value = self._header_cookies()
        if not isinstance(header_cookie or sapid_value, str):
            raise ValueError("Failed To Get Valid Cookies")

        default_session.headers = {
            "authority": "api.youtube.com",
            "authorization": f"SAPISIDHASH {asyncio.run(self._sapisid_hash(sapid_value))}",
            "studio-type": "application/json",
            "cookie": header_cookie.strip(),
            "user-agent": self._user_agent,
            "x-goog-authuser": self.gauth_id,
            "x-origin": "https://studio.youtube.com",
        }
        default_session.params = {"alt": "json", "key": self.auth_key}
        return default_session

    @property
    def get_session_token(self):
        return self._session_token
