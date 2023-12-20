from typing import Generator, List, Optional, Union

from artifi.google import GoogleWebSession


def _get_nested_key(
    d: Union[dict, List[dict]], key: str, default="UNKNOWN"
) -> Union[str, dict, List[str]]:
    if isinstance(d, list):
        for item in d:
            result = _get_nested_key(item, key)
            if result is not None:
                return result
    elif isinstance(d, dict):
        for k, v in d.items():
            if k == key:
                return v
            if isinstance(v, (dict, list)):
                result = _get_nested_key(v, key)
                if result is not None:
                    return result
    return default


def _format_ms_to_time(ms):
    seconds = ms / 1000
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"


class StudioVideoObj:
    def __init__(self, video: dict):
        self._video = video
        self._video_id: Optional[str] = None
        self._channel_id: Optional[str] = None
        self._video_title: Optional[str] = None
        self._video_length: Optional[str] = None
        self._description: Optional[str] = None
        self._download_url: Optional[str] = None
        self._restriction: Optional[list] = None
        self._is_private: Optional[bool] = None
        self._is_drafted: bool = False
        self._edit_processing_status: Optional[str] = None
        self._video_status: Optional[str] = None
        self._insights: Optional[dict] = None
        self.__call__()

    def __call__(self, *args, **kwargs):
        if self._video:
            self._video_id = self._video.get("videoId")
            self._channel_id = self._video.get("channelId")
            self._video_title = self._video.get("title")
            self._video_length = self._video.get("lengthSeconds")
            self._description = self._video.get("description")
            self._download_url = self._video.get("downloadUrl")
            self._restriction = self._restriction_state(
                _get_nested_key(self._video.get("allRestrictions"), "reason")
            )
            self._is_private = self._video.get("privacy") == "VIDEO_PRIVACY_PRIVATE"
            self._is_drafted = self._video.get("draftStatus") == "DRAFT_STATUS_NONE"
            self._insights = {
                "total_comments": self._video.get("metrics").get("commentCount"),
                "total_dislike": self._video.get("metrics").get("dislikeCount"),
                "total_like": self._video.get("metrics").get("likeCount"),
                "total_view": self._video.get("metrics").get("viewCount"),
            }
            self._edit_processing_status = self._edit_processing_state(
                self._video.get("inlineEditProcessingStatus")
            )
            self._video_status = self._video_state(self._video.get("status"))

        return self

    @staticmethod
    def _restriction_state(option: str):
        mapping = {
            "VIDEO_RESTRICTION_REASON_COPYRIGHT": "COPYRIGHT",
        }

        return mapping.get(option, "NO_RESTRICTION")

    @staticmethod
    def _edit_processing_state(option: str):
        mapping = {
            "VIDEO_PROCESSING_STATUS_EDITED": "EDITED",
            "VIDEO_PROCESSING_STATUS_UNEDITED": "UNEDITED",
            "VIDEO_PROCESSING_STATUS_PROCESSING": "PROCESSING",
        }

        return mapping.get(option, "UNKNOWN")

    @staticmethod
    def _video_state(option: str):
        mapping = {
            "VIDEO_STATUS_UPLOADED": "UPLOADED_CHECKING",
            "VIDEO_STATUS_PROCESSED": "PROCESSED",
        }

        return mapping.get(option, "UNKNOWN")

    @property
    def video_id(self) -> Optional[str]:
        return self._video_id

    @property
    def channel_id(self) -> Optional[str]:
        return self._channel_id

    @property
    def video_title(self) -> Optional[str]:
        return self._video_title

    @property
    def description(self) -> Optional[str]:
        return self._description

    @property
    def download_url(self) -> Optional[str]:
        return self._download_url

    @property
    def restriction(self) -> Optional[list]:
        return self._restriction

    @property
    def is_private(self) -> Optional[bool]:
        return self._is_private

    @property
    def is_drafted(self) -> bool:
        return self._is_drafted

    @property
    def insights(self) -> Optional[dict]:
        return self._insights

    @property
    def edit_processing_status(self) -> Optional[str]:
        return self._edit_processing_status

    @property
    def video_status(self) -> Optional[str]:
        return self._video_status

    @property
    def video_length(self) -> Optional[str]:
        return self._video_length


class StudioVideoClaimsObj:
    def __init__(self, claim: dict):
        self._claim = claim
        self._claim_id: Optional[str] = None
        self._video_id: Optional[str] = None
        self._type: Optional[str] = None
        self._duration: Optional[str] = None
        self._resolve_option: Optional[List[str]] = None
        self._claim_title: Optional[str] = None
        self._status: Optional[str] = None
        self._artists: Optional[List[str]] = None
        self.__call__()

    def __call__(self, *args, **kwargs):
        self._claim_id = self._claim.get("claimId")
        self._video_id = self._claim.get("videoId")
        self._type = self._claim.get("type")

        start_time_seconds = int(
            self._claim.get("matchDetails", {}).get("longestMatchStartTimeSeconds", 0)
        )
        duration_seconds = int(
            self._claim.get("matchDetails", {}).get("longestMatchDurationSeconds", 0)
        )
        end_time_seconds = start_time_seconds + duration_seconds

        start_time_minutes, start_time_seconds = divmod(start_time_seconds, 60)
        end_time_minutes, end_time_seconds = divmod(end_time_seconds, 60)
        self._duration = f"{start_time_minutes:02d}:{start_time_seconds:02d} - {end_time_minutes:02d}:{end_time_seconds:02d}"

        self._resolve_option = self._available_option(
            self._claim.get("nontakedownClaimActions", {}).get("options")
        )

        meta_data = self._claim.get("asset", {}).get("srMetadata") or self._claim.get(
            "asset", {}
        ).get("metadata", {})
        self._claim_title = _get_nested_key(meta_data, "title")
        self._status = self._claim.get("status")
        self._artists = _get_nested_key(meta_data, "artists")

        return self

    @staticmethod
    def _available_option(options: list):
        mapping = {
            "NON_TAKEDOWN_CLAIM_OPTION_ERASE_SONG": "MUTE_SONG",
            "NON_TAKEDOWN_CLAIM_OPTION_TRIM": "TRIM_SEGMENT",
        }

        return [
            mapping.get(option, "UNAVAILABLE")
            for option in options
            if option in mapping
        ] or ["UNAVAILABLE"]

    @property
    def claim_id(self) -> Optional[str]:
        return self._claim_id

    @property
    def video_id(self) -> Optional[str]:
        return self._video_id

    @property
    def type(self) -> Optional[str]:
        return self._type

    @property
    def duration(self) -> Optional[str]:
        return self._duration

    @property
    def resolve_option(self) -> Optional[List[str]]:
        return self._resolve_option

    @property
    def claim_title(self) -> Optional[str]:
        return self._claim_title

    @property
    def status(self) -> Optional[str]:
        return self._status

    @property
    def artists(self) -> Optional[List[str]]:
        return self._artists


class GoogleYouTubeStudio(GoogleWebSession):
    def __init__(
        self,
        context,
        email: str,
        password: str,
        headless: bool = True,
        param_key: str = "AIzaSyBUPetSUmoZL-OhlxA7wSac5XinrygCqMo",
        user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    ):
        super().__init__(context, email, password, headless, param_key, user_agent)
        self._session = self.google_websession()
        self._base_url = "https://studio.youtube.com"
        self._version = "v1"
        self._service = "youtubei"

    def list_videos(self) -> Optional[Generator[StudioVideoObj, None, None]]:
        payload = {
            "filter": {
                "and": {
                    "operands": [
                        {"channelIdIs": {"value": self._channel_id}},
                        {
                            "and": {
                                "operands": [
                                    {"videoOriginIs": {"value": "VIDEO_ORIGIN_UPLOAD"}},
                                    {
                                        "not": {
                                            "operand": {
                                                "or": {
                                                    "operands": [
                                                        {
                                                            "contentTypeIs": {
                                                                "value": "CREATOR_CONTENT_TYPE_SHORTS"
                                                            }
                                                        },
                                                        {
                                                            "and": {
                                                                "operands": [
                                                                    {
                                                                        "not": {
                                                                            "operand": {
                                                                                "statusIs": {
                                                                                    "value": "VIDEO_STATUS_PROCESSED"
                                                                                }
                                                                            }
                                                                        }
                                                                    },
                                                                    {
                                                                        "isShortsEligible": {}
                                                                    },
                                                                ]
                                                            }
                                                        },
                                                    ]
                                                }
                                            }
                                        }
                                    },
                                ]
                            }
                        },
                    ]
                }
            },
            "order": "VIDEO_ORDER_DISPLAY_TIME_DESC",
            "pageSize": 30,
            "mask": {
                "channelId": True,
                "videoId": True,
                "lengthSeconds": True,
                "livestream": {"all": True},
                "publicLivestream": {"all": True},
                "origin": True,
                "premiere": {"all": True},
                "publicPremiere": {"all": True},
                "status": True,
                "thumbnailDetails": {"all": True},
                "title": True,
                "draftStatus": True,
                "downloadUrl": True,
                "watchUrl": True,
                "shareUrl": True,
                "permissions": {"all": True},
                "features": {"all": True},
                "timeCreatedSeconds": True,
                "timePublishedSeconds": True,
                "privacy": True,
                "contentOwnershipModelSettings": {"all": True},
                "contentType": True,
                "publicShorts": {"all": True},
                "podcastRssMetadata": {"all": True},
                "videoLinkageShortsAttribution": {"all": True},
                "responseStatus": {"all": True},
                "statusDetails": {"all": True},
                "description": True,
                "metrics": {"all": True},
                "thumbnailEditorState": {"all": True},
                "titleFormattedString": {"all": True},
                "descriptionFormattedString": {"all": True},
                "titleDetails": {"all": True},
                "descriptionDetails": {"all": True},
                "audienceRestriction": {"all": True},
                "releaseInfo": {"all": True},
                "allRestrictions": {"all": True},
                "inlineEditProcessingStatus": True,
                "videoPrechecks": {"all": True},
                "shorts": {"all": True},
                "selfCertification": {"all": True},
                "videoResolutions": {"all": True},
                "scheduledPublishingDetails": {"all": True},
                "visibility": {"all": True},
                "privateShare": {"all": True},
                "sponsorsOnly": {"all": True},
                "unlistedExpired": True,
                "videoTrailers": {"all": True},
                "remix": {"isSource": True},
                "isPaygated": True,
            },
            "context": {
                "client": {
                    "clientName": 62,
                    "clientVersion": "1.20231122.03.00",
                    "hl": "en",
                    "gl": "IN",
                    "experimentsToken": "",
                    "utcOffsetMinutes": 330,
                    "userInterfaceTheme": "USER_INTERFACE_THEME_DARK",
                    "screenWidthPoints": 1366,
                    "screenHeightPoints": 157,
                    "screenPixelDensity": 1,
                    "screenDensityFloat": 1,
                },
                "request": {
                    "returnLogEntry": True,
                    "internalExperimentFlags": [],
                    "consistencyTokenJars": [],
                },
            },
        }
        _url = f"{self._base_url}/{self._service}/{self._version}/creator/list_creator_videos"
        all_set = True
        while all_set:
            response = self._session.post(_url, json=payload)
            response.raise_for_status()
            data = response.json()
            if response.status_code == 402:
                self.context.logger.info(
                    "Google Session Expired, Trying Again Please Wait...!"
                )
                self._session = self.google_websession()
                return self.list_videos()
            if page_token := data.get("nextPageToken"):
                payload["pageToken"] = page_token
            else:
                all_set = False
            content_videos = data.get("videos", [])
            for ids, video_data in enumerate(content_videos, start=1):
                yield StudioVideoObj(video_data)

    def list_video_claims(
        self, video: StudioVideoObj
    ) -> Optional[Generator[StudioVideoClaimsObj, None, None]]:
        payload = {
            "context": {
                "client": {
                    "clientName": 62,
                    "clientVersion": "1.20231122.03.00",
                    "hl": "en",
                    "gl": "IN",
                    "experimentsToken": "",
                    "utcOffsetMinutes": 330,
                    "userInterfaceTheme": "USER_INTERFACE_THEME_DARK",
                    "screenWidthPoints": 1366,
                    "screenHeightPoints": 342,
                    "screenPixelDensity": 1,
                    "screenDensityFloat": 1,
                },
                "request": {
                    "returnLogEntry": True,
                    "internalExperimentFlags": [],
                    "consistencyTokenJars": [],
                },
                "user": {
                    "delegationContext": {
                        "externalChannelId": self._channel_id,
                        "roleType": {
                            "channelRoleType": "CREATOR_CHANNEL_ROLE_TYPE_OWNER"
                        },
                    }
                },
            },
            "videoId": video.video_id,
            "criticalRead": False,
            "includeLicensingOptions": False,
        }
        _url = f"{self._base_url}/{self._service}/{self._version}/creator/list_creator_received_claims?alt=json&key={self.auth_key}"

        response = self._session.post(_url, json=payload)
        response.raise_for_status()
        if response.status_code == 402:
            self.context.logger.info(
                "Google Session Expired, Trying Again Please Wait...!"
            )
            self._session = self.google_websession()
            return self.list_video_claims(video)
        data = response.json().get("receivedClaims", [])

        for ids, claim in enumerate(data, start=1):
            yield StudioVideoClaimsObj(claim)

    def _get_claimed_duration(self, claim: StudioVideoClaimsObj):
        _url = f"{self._base_url}/{self._service}/{self._version}/copyright/get_creator_received_claim_matches"
        payload = {
            "videoId": claim.video_id,
            "claimId": claim.claim_id,
            "channelId": self._channel_id,
            "context": {
                "client": {
                    "clientName": 62,
                    "clientVersion": "1.20231128.04.00",
                    "hl": "en",
                    "gl": "IN",
                    "experimentsToken": "",
                    "utcOffsetMinutes": 330,
                    "userInterfaceTheme": "USER_INTERFACE_THEME_DARK",
                    "screenWidthPoints": 811,
                    "screenHeightPoints": 629,
                    "screenPixelDensity": 1,
                    "screenDensityFloat": 1,
                },
                "request": {"returnLogEntry": True, "internalExperimentFlags": []},
                "user": {
                    "delegationContext": {
                        "externalChannelId": self._channel_id,
                        "roleType": {
                            "channelRoleType": "CREATOR_CHANNEL_ROLE_TYPE_OWNER"
                        },
                    }
                },
            },
        }

        response = self._session.post(_url, json=payload)
        response.raise_for_status()
        if response.status_code == 402:
            self.context.logger.info(
                "Google Session Expired, Trying Again Please Wait...!"
            )
            self._session = self.google_websession()
            return self._get_claimed_duration(claim)
        res_data = response.json()
        claim_matches = res_data.get("matches").get("claimMatches")
        data = []
        for item in claim_matches:
            data.append(item.get("videoSegment"))
        return data

    def trim_out(self, claim: StudioVideoClaimsObj):
        _url = (
            f"{self._base_url}/{self._service}/{self._version}/video_editor/edit_video"
        )

        payload = {
            "externalVideoId": claim.video_id,
            "claimEditChange": {
                "addRemoveSongEdit": {
                    "claimId": claim.claim_id,
                    "method": "REMOVE_SONG_METHOD_TRIM",
                    "muteSegments": self._get_claimed_duration(claim),
                    "allKnownMatchesCovered": False,
                }
            },
            "context": {
                "client": {
                    "clientName": 62,
                    "clientVersion": "1.20231128.04.00",
                    "hl": "en",
                    "gl": "IN",
                    "experimentsToken": "",
                    "utcOffsetMinutes": 330,
                    "userInterfaceTheme": "USER_INTERFACE_THEME_DARK",
                    "screenWidthPoints": 811,
                    "screenHeightPoints": 629,
                    "screenPixelDensity": 1,
                    "screenDensityFloat": 1,
                },
                "request": {
                    "returnLogEntry": True,
                    "internalExperimentFlags": [],
                    "sessionInfo": {"token": self.get_session_token},
                    "consistencyTokenJars": [],
                },
                "user": {
                    "delegationContext": {
                        "externalChannelId": self._channel_id,
                        "roleType": {
                            "channelRoleType": "CREATOR_CHANNEL_ROLE_TYPE_OWNER"
                        },
                    }
                },
            },
        }

        response = self._session.post(_url, json=payload)
        if response.status_code == 409:
            return {
                "status": "wait till existing edit process to complete...!",
                "code": "WAITING_FOR_COMPLETE",
            }
        if response.status_code == 402:
            self.context.logger.info(
                "Google Session Expired, Trying Again Please Wait...!"
            )
            self._session = self.google_websession()
            return self.trim_out(claim)
        response.raise_for_status()
        res_data = response.json()
        return {"status": res_data.get("executionStatus"), "code": "INITIATED_FOR_EDIT"}

    def mute_segment_songs(self, claim: StudioVideoClaimsObj, song_only=True):
        _url = (
            f"{self._base_url}/{self._service}/{self._version}/video_editor/edit_video"
        )

        payload = {
            "externalVideoId": claim.video_id,
            "claimEditChange": {
                "addRemoveSongEdit": {
                    "claimId": claim.claim_id,
                    "method": "REMOVE_SONG_METHOD_WAVEFORM_ERASE"
                    if song_only
                    else "REMOVE_SONG_METHOD_MUTE",
                    "muteSegments": self._get_claimed_duration(claim),
                    "allKnownMatchesCovered": True,
                }
            },
            "context": {
                "client": {
                    "clientName": 62,
                    "clientVersion": "1.20231128.04.00",
                    "hl": "en",
                    "gl": "IN",
                    "experimentsToken": "",
                    "utcOffsetMinutes": 330,
                    "userInterfaceTheme": "USER_INTERFACE_THEME_DARK",
                    "screenWidthPoints": 811,
                    "screenHeightPoints": 629,
                    "screenPixelDensity": 1,
                    "screenDensityFloat": 1,
                },
                "request": {
                    "returnLogEntry": True,
                    "internalExperimentFlags": [],
                    "sessionInfo": {"token": self.get_session_token},
                    "consistencyTokenJars": [],
                },
                "user": {
                    "delegationContext": {
                        "externalChannelId": self._channel_id,
                        "roleType": {
                            "channelRoleType": "CREATOR_CHANNEL_ROLE_TYPE_OWNER"
                        },
                    }
                },
            },
        }

        response = self._session.post(_url, json=payload)
        if response.status_code == 409:
            return {
                "status": "wait until existing edit process to complete...!",
                "code": "WAITING_FOR_COMPLETE",
            }
        if response.status_code == 402:
            self.context.logger.info(
                "Google Session Expired, Trying Again Please Wait...!"
            )
            self._session = self.google_websession()
            return self.mute_segment_songs(claim, song_only)

        response.raise_for_status()
        res_data = response.json()
        return {"status": res_data.get("executionStatus"), "code": "INITIATED_FOR_EDIT"}
