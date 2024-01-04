"""
Google Photos Api
"""
import os
from typing import Optional

from requests import Session

from artifi.config.ext.exception import PhotoError
from artifi.google import Google
from artifi.utils import fetch_mime_type


class GooglePhotos(Google):
    def __init__(self, context, scope):
        super().__init__(context)
        self._creds = self.oauth_creds(scope, 'photos')
        self._service: Session = Optional[Session]
        self._base_url = "https://photoslibrary.googleapis.com"
        self._version = "v1"
        self.authorize()

    @property
    def base_url(self):
        """

        @return:
        """
        return f"{self._base_url}/{self._version}"

    @property
    def service(self):
        """

        @return:
        """
        return self._service

    def authorize(self):
        """
        authorize the session
        """
        session = Session()
        session.headers = {
            "Authorization": f"Bearer {self._creds.token}",
            "Content-type": "application/json"
        }
        self._service = session

    def create_album(self, album_name):
        """

        @param album_name:
        @return:
        """
        payload = {
            "album": {
                'title': album_name.strip()[:500]
            }
        }
        url = f"{self.base_url}/albums"
        self.context.logger.info("Getting Media Details...!")
        response = self._service.post(url, json=payload, timeout=30)
        if response.status_code in [401]:
            self.authorize()
            return self.create_album(album_name)
        elif response.status_code in [200]:
            data = response.json()
            return data
        else:
            response.raise_for_status()

    def list_album(self):
        """
        Get All Photos
        """
        payload = {
            'pageSize': 50
        }
        media_url = f"{self.base_url}/albums"
        output = []
        self.context.logger.info("Fetching All Media Items In Google Photos, "
                                 "It May Take While Depending On No. photos")
        while True:
            response = self._service.get(media_url, params=payload, timeout=30)
            if response.status_code in [401]:
                # Unauthorized, reauthorize and retry
                self.authorize()
                continue
            elif response.status_code in [200]:
                data = response.json()

                if not (page_token := data.get('nextPageToken')):
                    break
                payload['pageToken'] = page_token
                output.extend(data.get('albums'))
            else:
                response.raise_for_status()

        return output

    def get_album(self, album_id):
        """
        Get All Photos
        """
        media_url = f"{self.base_url}/albums/{album_id}"
        self.context.logger.info("Getting Media Details...!")
        response = self._service.get(media_url, timeout=30)
        if response.status_code in [401]:
            self.authorize()
            return self.get_media(album_id)
        elif response.status_code in [200]:
            data = response.json()
            return data
        else:
            response.raise_for_status()

    def list_media(self, album_id=None):
        """

        @param album_id:
        """
        payload = {
            "pageSize": 100,
            "orderBy": "MediaMetadata.creation_time desc",
        }
        if album_id:
            payload['albumId'] = album_id
        media_url = f"{self.base_url}/mediaItems:search"
        output = []
        self.context.logger.info("Fetching All Media Items In Google Photos, "
                                 "It May Take While Depending On No. photos")
        while True:
            response = self._service.post(media_url, json=payload, timeout=30)
            if response.status_code in [401]:
                # Unauthorized, reauthorize and retry
                self.authorize()
                continue
            elif response.status_code in [200]:
                data = response.json()
                output.extend(data.get('mediaItems'))
                if not (page_token := data.get('nextPageToken')):
                    break
                payload['pageToken'] = page_token
            else:
                response.raise_for_status()

        return output

    def get_media(self, media_id):
        """
        Get All Photos
        """
        media_url = f"{self.base_url}/mediaItems/{media_id}"
        self.context.logger.info("Getting Media Details...!")
        response = self._service.get(media_url, timeout=30)
        if response.status_code in [401]:
            self.authorize()
            return self.get_media(media_id)
        elif response.status_code in [200]:
            data = response.json()
            return data
        else:
            response.raise_for_status()

    def _upload_folder(self, path, album_id):
        files = os.listdir(path)
        for file in files:
            file_path = os.path.join(path, file)
            if os.path.isdir(file_path):
                self._upload_folder(file_path, album_id)
            elif os.path.isfile(file_path):
                response = MediaIoPhotosUpload(self, file_path, album_id).upload_file()
                self.context.logger.info(response)
        return True

    def upload(self, path, album_id=None):
        """

        @param album_id:
        @param path:
        """
        if os.path.isdir(path):
            return self._upload_folder(path, album_id)
        elif os.path.isfile(path):
            response = MediaIoPhotosUpload(self, path, album_id).upload_file()
            self.context.logger.info(response)
            return response
        else:
            raise PhotoError("Path Must Be Either Folder Or File...!")


class MediaIoPhotosUpload:
    """
    Media Upload chunk
    """

    def __init__(self, gphotos, file_path, album_id=None, chunk_size=50 * 1024 * 1024):
        self._gphotos: GooglePhotos = gphotos
        self._file_path = file_path
        self._album_id = album_id
        self._chunk_size = chunk_size
        self._upload_url = self._upload_session()
        self._upload_token = None

    def _upload_session(self):
        """

        @return:
        """
        mime_type = fetch_mime_type(self._file_path)

        headers = {
            "Content-Length": '0',
            "X-Goog-Upload-Command": 'start',
            "X-Goog-Upload-Content-Type": mime_type,
            "X-Goog-Upload-Protocol": 'resumable',
            "X-Goog-Upload-Raw-Size": str(self._get_file_size()),
        }
        url = f"{self._gphotos.base_url}/uploads"
        response = self._gphotos.service.post(url=url, headers=headers)
        if response.status_code in [401]:
            self._gphotos.authorize()
            return self._upload_session()
        elif response.status_code in [200]:
            return response.headers['X-Goog-Upload-URL']
        else:
            response.raise_for_status()

    def _get_file_size(self):
        """

        @return: 
        """
        with open(self._file_path, 'rb') as file:
            file.seek(0, 2)  # Seek to the end of the file
            return file.tell()

    def _upload_chunk(self, chunk, offset):
        """

        @param chunk: 
        @param offset: 
        """
        is_final_chunk = (offset + len(chunk) == self._get_file_size())
        command = 'upload, finalize' if is_final_chunk else 'upload'

        headers = {
            'Content-Length': str(len(chunk)),
            'X-Goog-Upload-Command': command,
            'X-Goog-Upload-Offset': str(offset)
        }
        response = self._gphotos.service.post(
            self._upload_url,
            headers=headers,
            data=chunk
        )

        if response.status_code in [401]:
            self._gphotos.authorize()
            return self._upload_chunk(chunk, offset)
        elif is_final_chunk and response.status_code in [200]:
            self._upload_token = response.content.decode()

        else:
            response.raise_for_status()
        return True

    def _create_file(self):
        url = f"{self._gphotos.base_url}/mediaItems:batchCreate"
        payload = {
            "newMediaItems": [
                {
                    "simpleMediaItem": {
                        "fileName": os.path.basename(self._file_path),
                        "uploadToken": self._upload_token
                    },

                }
            ],
            "albumPosition": {
                "position": "FIRST_IN_ALBUM"
            }
        }
        if self._album_id:
            payload['albumId'] = self._album_id
        response = self._gphotos.service.post(url, json=payload, timeout=30)
        print(response.text)
        if response.status_code in [401]:
            self._gphotos.authorize()
            return self._create_file()
        elif response.status_code in [200]:
            data = response.json()
            return data
        else:
            response.raise_for_status()

    def upload_file(self):
        """

        @return:
        """

        offset = 0

        with open(self._file_path, 'rb') as file:
            while offset < self._get_file_size():
                chunk = file.read(self._chunk_size)
                self._upload_chunk(chunk, offset)
                offset += len(chunk)
        return False if self._get_file_size() == 0 else self._create_file()
