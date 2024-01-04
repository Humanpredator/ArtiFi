"""Calender API"""
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from artifi.google import Google


class GoogleCalendar(Google):
    def __init__(self, context, scope, calendar_id="primary"):
        super().__init__(context)
        self._creds = self.oauth_creds(scope, 'calender')
        self.calendar_id = calendar_id
        self._service = build('calendar', 'v3', credentials=self._creds)

    def get_event(self, event_id):
        """

        @param event_id:
        @return:
        """
        try:
            events = self._service.events().get(calendarId=self.calendar_id,
                                                eventId=event_id).execute()
        except HttpError:
            events = None
        return events

    def remove_event(self, event_id):
        """

        @param event_id:
        @return:
        """
        events = self._service.events().delete(calendarId=self.calendar_id,
                                               eventId=event_id).execute()
        return events

    def add_event(self, event_id, summary, start_time, end_time, location='',
                  description=''):
        """

        @param event_id:
        @param summary:
        @param start_time:
        @param end_time:
        @param location:
        @param description:
        @return:
        """
        event = {
            'id': event_id,
            'summary': summary,
            'location': location,
            'description': description,
            'start': {'dateTime': start_time.isoformat(),
                      'timeZone': self.context.tz.zone},
            'end': {'dateTime': end_time.isoformat(),
                    'timeZone': self.context.tz.zone},
            'reminders': {'useDefault': False,
                          'overrides': [{'method': 'popup', 'minutes': 60}]},
        }

        if self.get_event(event_id):
            created_event = self._service.events().patch(
                eventId=event_id,
                calendarId=self.calendar_id,
                body=event).execute()
        else:
            created_event = self._service.events().insert(
                calendarId=self.calendar_id,
                body=event).execute()
        return created_event.get("htmlLink")
