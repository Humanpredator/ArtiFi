"""Google People API"""
from typing import Generator, Optional

from googleapiclient.discovery import build

from artifi.google import Google


class GoogleContactObj:
    """Contacts Details"""

    def __init__(self, obj):
        """@param obj:"""
        self._contact_obj: Optional[dict] = obj
        self._profile_name: Optional[str] = None
        self._profile_dob: Optional[str] = None
        self._mobile_number: Optional[list] = None
        self._profile_url: Optional[str] = None
        self.__call__()

    def __call__(self, *args, **kwargs):
        """

        @param args:
        @param kwargs:
        @return:
        """
        if names := self._contact_obj.get("names"):
            self._profile_name = names[0]["displayName"]
        if birthdays := self._contact_obj.get("birthdays"):
            date = birthdays[0]["date"]
            self._profile_dob = f"{date['year']}-{date['month']}-{date['day']}"
        if phone_number := self._contact_obj.get("phoneNumbers"):
            self._mobile_number = [
                (data["value"].replace(" ", ""), data["type"]) for data in phone_number
            ]
        if photos := self._contact_obj.get("photos"):
            self._profile_url = photos[0]["url"]

    @property
    def profile_name(self) -> str:
        """
        Contact Name
        @return:
        """
        return self._profile_name

    @property
    def profile_dob(self) -> str:
        """
        Contact Date Of Birth
        @return:
        """
        return self._profile_dob

    @property
    def mobile_numbers(self) -> list:
        """
        Mobile number
        @return:
        """
        return self._mobile_number

    @property
    def profile_url(self) -> str:
        """
        Profile Pic URL
        @return:
        """
        return self._profile_url


class GooglePeople(Google):
    """
    To get google contacts
    uses Oauth
    """

    def __init__(self, context, scope):
        """

        @param context:
        @param scope:
        """
        super().__init__(context)
        self._creds = self.oauth_creds(scope, 'people')
        self._service = build("people", "v1",
                              credentials=self._creds)

    def get_contacts(self) -> Generator[GoogleContactObj, None, None]:
        """fetch all available contacts"""
        fields = [
            "addresses",
            "ageRanges",
            "biographies",
            "birthdays",
            "calendarUrls",
            "clientData",
            "coverPhotos",
            "emailAddresses",
            "events",
            "externalIds",
            "genders",
            "imClients",
            "interests",
            "locales",
            "locations",
            "memberships",
            "metadata",
            "miscKeywords",
            "names",
            "nicknames",
            "occupations",
            "organizations",
            "phoneNumbers",
            "photos",
            "relations",
            "sipAddresses",
            "skills",
            "urls",
            "userDefined"
        ]
        results = (
            self._service.people()
            .connections()
            .list(
                resourceName="people/me",
                pageSize=1000,
                personFields=','.join(fields)
            )
            .execute()
        )
        connections = results.get("connections", [])
        for people in connections:
            yield GoogleContactObj(people)
