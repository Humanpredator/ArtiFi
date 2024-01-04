"""Google Gmail API"""
from googleapiclient.discovery import build

from artifi.google import Google


class GoogleMail(Google):
    def __init__(self, context, scope):
        super().__init__(context)
        self._creds = self.oauth_creds(scope, 'gmail')
        self._service = build("gmail", "v1", credentials=self._creds)

    def list_messages(self,
                      query: str = None,
                      fetch_limit: int = None
                      ):
        """

        @param query:
        @param fetch_limit:
        @return:
        """
        count = 1
        messages = []
        page_token = None
        while True:
            results = self._service.users().messages().list(userId="me",
                                                            q=query,
                                                            pageToken=page_token,
                                                            maxResults=fetch_limit).execute()
            messages.extend(results.get('messages', []))
            if not (page_token := results.get('nextPageToken')) or count == fetch_limit:
                break
            count += len(results.get('messages', []))

        return messages

    def view_mail(self,
                  msg_id):
        """

        @param msg_id:
        @return:
        """
        message = self._service.users().messages().get(userId="me",
                                                       id=msg_id).execute()
        return message
