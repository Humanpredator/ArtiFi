"""Ext for Google"""
import json
import os
import time
from contextlib import suppress

from playwright.sync_api import sync_playwright

from artifi import Artifi
from artifi.google import Google


class GoogleWebSession(Google):
    """To use the Google web API unofficial method using web session and playwright"""

    def __init__(self, context, email, password, headless, user_agent):
        """

        @param context: Pass :class Artifi
        @param email: Google email to be logged in
        @param password: Google email password
        @param headless: Set 'True' to do on background, 'False' to open browser
        @param user_agent: set user agent compatible with chrome drive
        """
        super().__init__(context)
        self.context: Artifi = context
        self._chrome_path = self.context.CHROMEDRIVE_PATH
        self._headless = headless
        self._email: str = email
        self._session_path = os.path.join(self.context.directory,
                                          f"{self._email}.json")
        self._password: str = password
        self._user_agent: str = user_agent

    def load_session(self):
        """
        Check and return the data of the session
        @return:  file with the name of email
        """
        with suppress(Exception), open(self._session_path, "r") as f:
            return json.load(f)
        return None

    def save_session(self, data):
        """
        Save session json data
        @param data: session data
        """
        with suppress(Exception), open(self._session_path, "w") as f:
            json.dump(data, f)

    def fetch_save_gsession(self, goto_url, intercept_func: callable):
        """

        @param goto_url: URL to go and save the session of the site
        @param intercept_func: Optional if upu want to see the network request
        @return Current page url
        """
        self.context.logger.info(
            f"Setting Up Session For {self._email}, Please Wait...!"
        )
        with sync_playwright() as p:
            browser = p.chromium.launch(
                executable_path=self._chrome_path,
                headless=self._headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            session_data = self.load_session()
            browser_context = browser.new_context(
                storage_state=session_data,
                user_agent=self._user_agent,
                java_script_enabled=True,
            )
            page = browser_context.new_page()
            if intercept_func:
                page.on("response", intercept_func)

            page.goto(goto_url)
            if "accounts.google.com" in page.url:
                self.context.logger.info("Logging In With Given Credentials")
                page.fill('input[type="email"]', f"{self._email}")
                page.click("div#identifierNext")

                page.wait_for_selector('input[type="password"]',
                                       state="visible")
                page.fill('input[type="password"]', f"{self._password}")
                page.click("div#passwordNext")
                time.sleep(5)
                page.wait_for_selector(
                    '[id="menu-paper-icon-item-1"]', state="attached"
                )
                session_data = browser_context.storage_state()
                self.save_session(session_data)
                self.context.logger.info("Login Successfully...!")

            browser_context.storage_state = session_data
            self.context.logger.info("Almost There Just Validating Session...!")
            page.wait_for_timeout(10000)
            current_page_url = page.url
            browser_context.close()
        return current_page_url
