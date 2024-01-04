"""Google Drive"""
import io
import os
import re
import time
import urllib.parse as urlparse
import uuid
from random import randrange
from urllib.parse import parse_qs

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from tenacity import *

from artifi.config.ext.exception import DriveUploadError, DriveError, \
    DriveDownloadError, DriveCloneError, DrivePropertiesError
from artifi.google import Google
from artifi.utils import readable_size, fetch_mime_type, \
    sanitize_name, readable_time, speed_convert

export_mime = {
    "application/vnd.google-apps.document":
        ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",
         '.docx'),

    "application/vnd.google-apps.spreadsheet":
        ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
         '.xlsx'),

    "application/vnd.google-apps.presentation":
        ("application/vnd.openxmlformats-officedocument.presentationml.presentation",
         '.pptx'),

    "application/vnd.google-apps.scenes":
        ("video/mp4",
         '.mp4'),

    "application/vnd.google-apps.jam":
        ("application/pdf",
         '.pdf'),

    "application/vnd.google-apps.script":
        ("application/vnd.google-apps.script+json",
         '.json'),

    "application/vnd.google-apps.form":
        ("application/zip",
         '.zip'),

    "application/vnd.google-apps.drawing":
        ("image/jpeg",
         '.jpg'),

    "application/vnd.google-apps.site":
        ("text/plain",
         '.txt'),

    "application/vnd.google-apps.mail-layout":
        ("text/plain",
         '.txt')
}


class GoogleDrive(Google):

    def __init__(self,
                 context,
                 scope,
                 drive_id,
                 use_sa=False,
                 is_td=False,
                 stop_duplicate=True
                 ):
        super().__init__(context)
        self.scope = scope
        self.parent_id = drive_id
        self.use_sa = use_sa
        self.is_td = is_td
        self.stop_duplicate = stop_duplicate

        self._sa_count = 0
        self._sa_path = os.path.join(self.context.directory, 'sa')
        self._sa_idx = randrange(len(os.listdir(self._sa_path))) if (
            self.use_sa) else None
        self.drive_folder_mime = "application/vnd.google-apps.folder"
        self.dl_file_prefix = "https://drive.google.com/uc?id={}&export=download"
        self.dl_folder_prefix = "https://drive.google.com/drive/folders/{}"
        self._service = self.authorize()

    def authorize(self):
        """

        @return:
        """
        # Get credentials
        credentials = self.oauth_creds(self.scope,
                                       service_user=self.use_sa,
                                       cname="drive")
        return build('drive', 'v3', credentials=credentials, cache_discovery=False)

    def switch_service_account(self):
        """switch to service"""
        service_account_count = len(os.listdir("accounts"))
        if self._sa_idx == service_account_count - 1:
            self._sa_idx = 0
        self._sa_count += 1
        self._sa_idx += 1
        self.context.logger.info(
            f"Switching to {self._sa_idx}.json service account")
        self._service = self.authorize()

    def get_id_by_url(self, link: str):
        """

        @param link:
        @return:
        """
        if "folders" in link or "file" in link:
            regex = r"https://drive\.google\.com/(drive)?/?u?/?\d?/?(mobile)?/?(file)?(folders)?/?d?/([-\w]+)[?+]?/?(w+)?"
            if not (res := re.search(regex, link)):
                self.context.logger.info(
                    f'No Folder or File Was Found On Given Link!')
                return None
            return res.group(5)
        parsed = urlparse.urlparse(link)
        return parse_qs(parsed.query)['id'][0]

    def get_file_id(self, file_name, mime_type, parent_id):
        """
        Check if a file with the same name, mime type, and parent directory ID already exists.
        If it exists, return its ID; otherwise, return None.
        """
        query = f"name='{file_name}' and mimeType='{mime_type}'"
        if parent_id:
            query += f" and '{parent_id}' in parents"

        results = self._service.files().list(
            q=query,
            spaces="drive",
            fields="files(id)",
            supportsTeamDrives=True
        ).execute()

        files = results.get("files", [])
        return files[0]["id"] if files else None

    def get_folder_id(self, directory_name, parent_id):
        """
        @param directory_name: Name of the directory to be checked.
        @param parent_id: ID of the parent directory.
        @return: ID of the existing directory if found, otherwise None.
        """
        query = f"name='{directory_name}' and mimeType='{self.drive_folder_mime}'"
        if parent_id:
            query += f" and trashed=false and '{parent_id}' in parents"
        results = self._service.files().list(
            q=query,
            spaces="drive",
            fields="files(id)",
            supportsTeamDrives=True
        ).execute()

        files = results.get("files", [])
        if files:
            return files[0]["id"]  # Return the ID of the first matching directory
        else:
            return None

    def set_permission(self, drive_id):
        """

        @param drive_id: 
        @return: 
        """
        if not self.is_td:
            permissions = {
                'role': 'reader',
                'type': 'anyone',
                'value': None,
                'withLink': True
            }
            return self._service.permissions().create(supportsTeamDrives=True,
                                                      fileId=drive_id,
                                                      body=permissions).execute()
        return None

    def get_metadata(self, file_id):
        """

        @param file_id:
        @return:
        """
        return self._service.files().get(supportsAllDrives=True, fileId=file_id,
                                         fields="name,id,mimeType,size").execute()

    def drive_detail(self, fields=None):
        """
        @param fields:
        @return:
        """
        data = self._service.about().get(
            fields=fields if fields else "storageQuota").execute()
        return data

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def create_folder(self, directory_name, parent_id):
        """
        @param directory_name: Name of the directory to be created or checked.
        @param parent_id: ID of the parent directory.
        @return: ID of the created or existing directory.
        """
        # Check if the directory already exists in the parent directory
        existing_directory_id = self.get_folder_id(directory_name, parent_id)
        if existing_directory_id:
            # Directory already exists, return its ID
            self.context.logger.info(
                f"Directory '{directory_name}' already exists. Returning existing ID: {existing_directory_id}"
            )
            return existing_directory_id

        # If the directory does not exist, create it
        file_metadata = {
            "name": directory_name,
            "mimeType": self.drive_folder_mime
        }
        if parent_id is not None:
            file_metadata["parents"] = [parent_id]

        file = self._service.files().create(
            supportsTeamDrives=True,
            body=file_metadata
        ).execute()

        file_id = file.get("id")
        if not self.is_td:
            self.set_permission(file_id)

        self.context.logger.info(
            f"Created G-Drive FolderName: {file.get('name')}"
        )
        return file_id

    def Upload(self, directory_path):
        """

        @return:
        """
        return DriveUpload(self, directory_path)

    def Download(self, drive_link):
        """

        @return:
        """
        return DriveDownload(self, drive_link)

    def Properties(self, drive_link):
        """

        @return:
        """
        return DriveProperties(self, drive_link)

    def Clone(self, drive_link):
        """

        @return:
        """
        return DriveCloner(self, drive_link)

    @property
    def service(self):
        """

        @return:
        """
        return self._service


class DriveUpload:
    """ Drive Upload Functionality"""

    def __init__(self, gdrive, directory_path):
        self.__UPLOAD_STARTED_TIME = time.time()

        self.gdrive: GoogleDrive = gdrive
        self._upload_path = directory_path

        self.__CONTENT_PROPERTIES__ = self._directory_properties()

        self.__FAILED_UPLOAD = []

        self.__TOTAL_FILES = 0
        self.__TOTAL_FOLDERS = 0
        self.__CURRENT_FILE_NAME = None
        self.__UPLOADED_BYTES__ = 0

        self.is_cancelled = False

    def on_upload_progress(self):
        """

        @return:
        """
        progress = {
            'filename': self.__CURRENT_FILE_NAME,
            'status': 'Uploading',
            'progress': f'{readable_size(self.__UPLOADED_BYTES__)}/{readable_size(self.__CONTENT_PROPERTIES__["size"])}',
            "elapsed": readable_time(time.time() - self.__UPLOAD_STARTED_TIME),
            'speed': f'{speed_convert(self.__UPLOADED_BYTES__ / (time.time() + 1 - self.__UPLOAD_STARTED_TIME))}'
        }
        return progress

    def _upload_folder(self, input_directory, parent_id):
        """

        @param input_directory:
        @param parent_id:
        @return:
        """
        list_dirs = os.listdir(input_directory)
        if len(list_dirs) == 0:
            return parent_id
        new_id = None
        for item in list_dirs:
            current_file = os.path.join(input_directory, item)
            if os.path.isdir(current_file):
                self.__TOTAL_FOLDERS += 1
                current_dir_id = self.gdrive.create_folder(item, parent_id)
                new_id = self._upload_folder(current_file, current_dir_id)

            else:
                mime_type = fetch_mime_type(current_file)
                file_name = os.path.basename(current_file)
                self._upload_file(current_file, file_name, mime_type, parent_id)
                new_id = parent_id

            if self.is_cancelled:
                raise DriveUploadError('Upload Cancelled!')
        return new_id

    def _duplicate_file(self, file_md, media_body):
        if self.gdrive.stop_duplicate and (
                ext_file_id := self.gdrive.get_file_id(file_md['name'],
                                                       file_md['mimeType'],
                                                       file_md['parents'][0])):
            drive_file = self.gdrive.service.files().update(fileId=ext_file_id,
                                                            media_body=media_body
                                                            )
        else:
            drive_file = self.gdrive.service.files().create(supportsTeamDrives=True,
                                                            body=file_md,
                                                            media_body=media_body)
        return drive_file

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def _upload_file(self, file_path, file_name, mime_type, parent_id):
        """

        @param file_path:
        @param file_name:
        @param mime_type:
        @param parent_id:
        @return:
        """
        file_size = os.path.getsize(file_path)
        self.__CURRENT_FILE_NAME = file_name
        self.gdrive.context.logger.info(f"Uploading FileName: {file_name}")
        # File body description
        file_metadata = {
            'name': file_name,
            'description': 'Uploaded by ArtiFi',
            'mimeType': mime_type,
            "parents": [parent_id]
        }
        fh = io.FileIO(file_path, 'rb')
        media_body = MediaIoBaseUpload(
            fh,
            mimetype=mime_type,
            resumable=True,
            chunksize=10 * 1024 * 1024
        )
        ul_file = self._duplicate_file(file_metadata, media_body)

        while True:
            if self.is_cancelled:
                fh.close()
                raise DriveUploadError("Drive Upload Cancelled")
            try:
                cr_state, chunk_state = ul_file.next_chunk()
                self.__UPLOADED_BYTES__ += (
                    file_size if chunk_state
                    else cr_state.total_size * cr_state.progress())
                if chunk_state:
                    file_id = chunk_state['id']
                    break
            except HttpError as err:
                reason = err.error_details[0]["reason"]

                if self.gdrive.use_sa and reason in [
                    'userRateLimitExceeded',
                    'dailyLimitExceeded',
                ]:
                    self.gdrive.switch_service_account()
                    self.gdrive.context.logger.info(
                        f"{reason}, Using Service Account And Trying Again...!")
                    return self._upload_file(file_path, file_name, mime_type,
                                             parent_id)
                else:
                    self.__FAILED_UPLOAD.append(file_name)
                    self.is_cancelled = True
                    self.gdrive.context.logger.info(f"Got: {reason}")
                    raise DriveError(f"Something Went Wrong {err}")

        self.gdrive.set_permission(file_id)
        # Define file instance and get url for download
        file = self.gdrive.service.files().get(supportsTeamDrives=True,
                                               fileId=file_id).execute()
        file_url = self.gdrive.dl_file_prefix.format(file.get('id'))
        self.__TOTAL_FILES += 1
        return file_url

    def _directory_properties(self):
        self.gdrive.context.logger.info("Counting Local Path:")
        output = {'size': 0, 'sub_folder': 0, 'files': 0}

        for root, sub_folders, files in os.walk(self._upload_path):
            output['sub_folder'] += len(sub_folders)
            output['files'] += len(files)

            for filename in files:
                filepath = os.path.join(root, filename)
                output['size'] += os.path.getsize(filepath)

        return output

    def upload(self):
        """
        @return:
        """
        self.gdrive.context.logger.info(f"Uploading Media: {self._upload_path}")

        output = {}
        if os.path.isfile(self._upload_path):
            filename = os.path.basename(self._upload_path)
            mime_type = fetch_mime_type(self._upload_path)
            link = self._upload_file(self._upload_path, filename, mime_type,
                                     self.gdrive.parent_id)
            if not link:
                raise DriveError('Unable to Get File Link!')
            self.gdrive.context.logger.info(f"Uploaded To G-Drive: {self._upload_path}")
            output['name'] = filename
            output['type'] = "File"
            output['link'] = link
        else:
            root_dir_name = os.path.basename(
                os.path.abspath(self._upload_path))
            root_dir_id = self.gdrive.create_folder(root_dir_name,
                                                    self.gdrive.parent_id)

            result = self._upload_folder(self._upload_path, root_dir_id)
            if not result:
                raise DriveUploadError('Upload has been manually cancelled!')
            link = f"https://drive.google.com/folderview?id={root_dir_id}"
            output['name'] = root_dir_name
            output['type'] = "Folder"
            output['link'] = link
        output['files'] = self.__TOTAL_FILES
        output['folders'] = self.__TOTAL_FOLDERS
        output['size'] = readable_size(self.__CONTENT_PROPERTIES__['size'])
        output['elapsed'] = readable_time(time.time() - self.__UPLOAD_STARTED_TIME)
        output['failed'] = self.__FAILED_UPLOAD
        return output


class DriveDownload:
    """
    Drive Download Functionality
    """

    def __init__(self, gdrive, drive_link):
        self.gdrive: GoogleDrive = gdrive
        self._drive_link = drive_link
        self.__DOWNLOADING = True
        self.__DOWNLOAD_START_TIME = time.time()
        self._properties = self.gdrive.Properties(
            self._drive_link)
        self.__CONTENT_PROPERTIES__ = self._properties.properties()

        self.__DOWNLOADED_BYTES__ = 0
        self.__CURRENT_FILE_NAME = None
        self.__TOTAL_FILES = 0
        self.__TOTAL_FOLDERS = 0
        self.__FAILED_DOWNLOAD = []
        self.is_cancelled = False

    def on_download_progress(self):
        """

        @return:
        """
        progress = {
            'filename': self.__CURRENT_FILE_NAME,
            'status': 'Downloading',
            'progress': f'{readable_size(self.__DOWNLOADED_BYTES__)}/{readable_size(self.__CONTENT_PROPERTIES__["size"])}',
            "elapsed": readable_time(time.time() - self.__DOWNLOAD_START_TIME),
            'speed': f'{speed_convert(self.__DOWNLOADED_BYTES__ / (time.time() + 1 - self.__DOWNLOAD_START_TIME))}'
        }
        return progress

    def _download_folder(self, path, file):
        """

        @param path:
        @param file:
        """
        new_folder_name = sanitize_name(file['name'])
        path = os.path.join(path, new_folder_name)
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        result = []
        page_token = None
        while True:
            if self.is_cancelled:
                raise DriveDownloadError("Download Cancelled By User...!")
            files = self.gdrive.service.files().list(
                supportsTeamDrives=True,
                includeTeamDriveItems=True,
                q=f"'{file['id']}' in parents",
                fields='nextPageToken, files(id, name, mimeType, size, shortcutDetails)',
                pageToken=page_token,
                pageSize=1000).execute()
            result.extend(files['files'])
            page_token = files.get("nextPageToken")
            if not page_token:
                break

        result = sorted(result, key=lambda k: k['name'])
        for item in result:
            if self.is_cancelled:
                raise DriveDownloadError("Download Cancelled By User...!")
            mime_type = item['mimeType']
            shortcut_details = item.get('shortcutDetails', None)
            if shortcut_details:
                mime_type = shortcut_details['targetMimeType']
            if mime_type == 'application/vnd.google-apps.folder':
                self.gdrive.context.logger.info(
                    f'Downloading FolderName:{new_folder_name}'
                )
                self._download_folder(path, item)
                self.__TOTAL_FOLDERS += 1

            elif not os.path.isfile(path + item['name']):
                self._download_file(path, item)
        return True

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def _download_file(self, path, file):
        """
        @return:
        """
        new_file_name = sanitize_name(file['name'])

        if crm := export_mime.get(file['mimeType'], None):
            request = self.gdrive.service.files().export(fileId=file['id'],
                                                         mimeType=crm[0])
            new_file_name += crm[1]
        else:
            request = self.gdrive.service.files().get_media(fileId=file['id'])

        self.__CURRENT_FILE_NAME = new_file_name
        file_path = os.path.join(path, new_file_name)

        if os.path.exists(file_path):
            self.gdrive.context.logger.info(
                f"FileName Already Exists: {new_file_name}"
            )
            return False

        self.gdrive.context.logger.info(
            f"Downloading FileName: {new_file_name}"
        )
        fh = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request,
                                         chunksize=10 * 1024 * 1024)
        while True:
            if self.is_cancelled:
                fh.close()
                raise DriveDownloadError("Upload Cancelled By User...!")
            try:
                cr_state, chunk_status = downloader.next_chunk()
                self.__DOWNLOADED_BYTES__ += (
                    cr_state.total_size if chunk_status
                    else cr_state.total_size * cr_state.progress())
                if chunk_status:
                    break
            except HttpError as err:
                reason = err.error_details[0]["reason"]
                if reason == "notFound":
                    self.gdrive.context.logger.error(
                        f"Failed To Download FileName: {new_file_name} Reason: {reason}"
                    )
                    return self.__FAILED_DOWNLOAD.append(file['id'])

                elif self.gdrive.use_sa and reason in [
                    'userRateLimitExceeded',
                    'dailyLimitExceeded',
                ]:
                    self.gdrive.switch_service_account()
                    self.gdrive.context.logger.info(
                        f"{reason}, Using Service Account And Trying Again...!")
                    self._download_file(path, file)
                else:
                    self.gdrive.context.logger.error(
                        f"Failed To Download FileName: {new_file_name} Reason: {reason}"
                    )
                    raise DriveError(f'Something Went Wrong,{err}')
        self.__TOTAL_FILES += 1
        return True

    def download(self, unique=True):
        """
        @return:
        @param unique
                :example Set 'True' to make new local folder to keep it isolated,
                         Set 'False' to name local folder name as drive folder name,
                         Not Recommended, Use it only to perform Sync
        """
        file_id = self.__CONTENT_PROPERTIES__['file_id']

        path = os.path.join(self.gdrive.context.directory,
                            str(uuid.uuid4()).lower()[:5]) if unique else (
            self.gdrive.context.directory)

        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        output = {}
        file = self.gdrive.get_metadata(file_id)
        output['name'] = file.get('name')
        output['path'] = os.path.join(path, self.__CONTENT_PROPERTIES__['filename'])
        if file.get("mimeType") == self.gdrive.drive_folder_mime:
            output['type'] = "Folder"
            self._download_folder(path, file)
        else:
            output['type'] = "File"
            self._download_file(path, file)
        output['files'] = self.__TOTAL_FILES
        output['folders'] = self.__TOTAL_FOLDERS
        output['size'] = readable_size(self.__CONTENT_PROPERTIES__['size'])
        output['elapsed'] = readable_time(time.time() - self.__DOWNLOAD_START_TIME)
        output['failed'] = self.__FAILED_DOWNLOAD
        return output


class DriveProperties:
    """
       Drive Download Functionality
    """

    def __init__(self, gdrive, drive_link):
        self.gdrive: GoogleDrive = gdrive
        self._drive_link = drive_link
        self.__TOTAL_BYTES = 0
        self.__TOTAL_FILES = 0
        self.__TOTAL_FOLDERS = 0
        self.is_cancelled = False

    def stop(self, channel_id, resource_id):
        """

        @param resource_id:
        @param channel_id:
        """
        body = {
            'id': channel_id,
            "resourceId": resource_id
        }
        try:
            self.gdrive.service.channels().stop(
                body=body
            ).execute()
        except HttpError as e:
            raise e
        return True

    def watch(self, **kwargs):
        """
        Watch changes in folder
        """
        file_id = self.gdrive.get_id_by_url(self._drive_link)
        body = {
            "payload": True,
            'id': kwargs['channel_id'],
            'type': 'web_hook',
            'address': kwargs['webhook_uri'],
        }
        response = self.gdrive.service.files().watch(fileId=file_id,
                                                     body=body,
                                                     supportsTeamDrives=True).execute()
        return response

    def _get_file_size(self, **kwargs):
        """

        @param kwargs:
        """
        try:
            size = kwargs['size']
        except KeyError:
            size = 0
        self.__TOTAL_BYTES += int(size)
        return True

    def _get_folder_size(self, **kwargs):

        """

        @param kwargs:
        @return:
        """
        files = self.list(kwargs['id'])
        for file_ in files:
            if self.is_cancelled:
                raise DrivePropertiesError("Properties was cancelled by User!")

            if file_['mimeType'] == self.gdrive.drive_folder_mime:
                self.__TOTAL_FOLDERS += 1
                self._get_folder_size(**file_)
            else:
                self.__TOTAL_FILES += 1
                self._get_file_size(**file_)
        return True

    def properties(self):
        """
        @return:
        """
        file_id = self.gdrive.get_id_by_url(self._drive_link)
        msg = {}
        self.gdrive.context.logger.info(f"File ID: {file_id}")
        drive_file = self.gdrive.service.files().get(fileId=file_id,
                                                     fields="id, name, mimeType, size",
                                                     supportsTeamDrives=True).execute()
        name = drive_file['name']
        self.gdrive.context.logger.info(f"Counting: {name}")
        if drive_file['mimeType'] == self.gdrive.drive_folder_mime:
            self._get_folder_size(**drive_file)
            msg['filename'] = name
            msg['file_id'] = file_id
            msg['size'] = self.__TOTAL_BYTES
            msg['type'] = "Folder"
            msg['sub_folders'] = self.__TOTAL_FOLDERS
            msg['files'] = self.__TOTAL_FILES
        else:
            self._get_file_size(**drive_file)
            msg['filename'] = name
            msg['file_id'] = file_id
            msg['type'] = 'File'
            msg['size'] = self.__TOTAL_BYTES
            msg['files'] = self.__TOTAL_FILES

        return msg

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def list(self, folder_id):
        """

        @param folder_id:
        @return:
        """
        page_token = None
        q = f"'{folder_id}' in parents"
        files = []
        while True:
            response = self.gdrive.service.files().list(supportsTeamDrives=True,
                                                        includeTeamDriveItems=True,
                                                        q=q,
                                                        spaces='drive',
                                                        pageSize=200,
                                                        fields='nextPageToken, files(id, name, mimeType,size)',
                                                        corpora='allDrives',
                                                        orderBy='folder, name',
                                                        pageToken=page_token).execute()
            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        return files

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def delete(self, link: str):
        """

        @param link:
        @return:
        """
        file_id = self.gdrive.get_id_by_url(link)
        msg = {}
        try:
            res = self.gdrive.service.files().delete(fileId=file_id,
                                                     supportsTeamDrives=self.gdrive.is_td).execute()
            msg = {'message': f"File Deleted Successfully! {res}"}
        except HttpError as err:
            reason = err.error_details[0]["reason"]
            self.gdrive.context.logger.error(f"Failed To Delete: {reason}")
            DriveError(f"Something Went Wrong: {err}")
        return msg


class DriveCloner:
    """
    Copy Functionality
    """

    def __init__(self, gdrive, drive_link):
        self.gdrive: GoogleDrive = gdrive
        self._drive_link = drive_link
        self._properties = self.gdrive.Properties(
            self._drive_link)
        self.__CONTENT_PROPERTIES__ = self._properties.properties()
        self.__CLONE_STARTED_TIME = time.time()

        self.__FAILED_CLONE = []

        self.__TRANSFERRED_BYTES = 0

        self.__TOTAL_FILES = 0
        self.__TOTAL_FOLDERS = 0
        self.__CURRENT_FILE_NAME = None

        self.is_cancelled = False

    def on_clone_progress(self):
        """

        @return:
        """
        progress = {
            'filename': self.__CURRENT_FILE_NAME,
            'status': 'Downloading',
            'progress': f'{readable_size(self.__TRANSFERRED_BYTES)}/{readable_size(self.__CONTENT_PROPERTIES__["size"])}',
            "elapsed": readable_time(time.time() - self.__CLONE_STARTED_TIME),
            'speed': f'{speed_convert(self.__TRANSFERRED_BYTES / (time.time() + 1 - self.__CLONE_STARTED_TIME))}'
        }
        return progress

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def _copy_file(self, file, dest_id):
        print("===>", file)
        """

        @param file:
        @param dest_id:
        @return:
        """

        file_metadata = {
            'name': file['name'],
            'description': 'Cloned by ArtiFi',
            'mimeType': file['mimeType'],
            "parents": [dest_id]
        }
        self.gdrive.context.logger.info(f"Cloning FileName:{file['name']}")
        try:
            drive_file = self.gdrive.service.files().copy(supportsAllDrives=True,
                                                          fileId=file.get('id'),
                                                          body=file_metadata).execute()
            self.__TRANSFERRED_BYTES += int(file.get('size', 0))
        except HttpError as err:
            reason = err.error_details[0]["reason"]

            if self.gdrive.use_sa and reason in [
                'userRateLimitExceeded',
                'dailyLimitExceeded',
            ]:
                self.gdrive.switch_service_account()
                self.gdrive.context.logger.info(
                    f"{reason}, Using Service Account And Trying Again...!")
                return self._copy_file(file, dest_id)
            else:
                self.__FAILED_CLONE.append(file['id'])
                self.is_cancelled = True
                self.gdrive.context.logger.info(f"Got: {reason}")
                raise DriveError(f"Something Went Wrong {err}")

        self.gdrive.set_permission(drive_file['id'])
        # Define file instance and get url for download
        file = self.gdrive.service.files().get(supportsTeamDrives=True,
                                               fileId=drive_file['id']).execute()
        file_url = self.gdrive.dl_file_prefix.format(file.get('id'))
        self.__TOTAL_FILES += 1
        return file_url

    def _clone_folder(self, local_path, file_id, parent_id):
        """

        @param local_path:
        @param file_id:
        @param parent_id:
        @return:
        """
        files = self._properties.list(file_id)
        for item in files:
            if self.is_cancelled:
                raise DriveCloneError(
                    "Cloning Was Cancelled By User!")
            if item.get('mimeType') == self.gdrive.drive_folder_mime:
                self.__TOTAL_FOLDERS += 1
                file_path = os.path.join(local_path, item.get('name'))
                current_dir_id = self.gdrive.create_folder(item.get('name'), parent_id)

                self._clone_folder(file_path, item['id'], current_dir_id)
            else:
                self._copy_file(item, parent_id)

        return True

    def clone(self):
        """
        @return:
        """
        file_id = self.__CONTENT_PROPERTIES__['file_id']
        msg = {}
        file = self.gdrive.get_metadata(file_id)
        if file.get("mimeType") == self.gdrive.drive_folder_mime:
            self.gdrive.context.logger.info(f"Cloning: {file.get('name')}")
            dir_id = self.gdrive.create_folder(file.get('name'), self.gdrive.parent_id)
            self._clone_folder(file.get('name'), file.get('id'), dir_id)
            msg['link'] = self.gdrive.dl_folder_prefix.format(dir_id)
            msg['filename'] = file.get("name")
            msg['size'] = readable_size(self.__TRANSFERRED_BYTES)
            msg['type'] = "Folder"
            msg['sub_folders'] = self.__TOTAL_FOLDERS
            msg['files'] = self.__TOTAL_FILES

        else:
            durl = self._copy_file(file, self.gdrive.parent_id)
            msg['filename'] = file.get("name")
            msg['link'] = durl
            msg['type'] = 'File'
            msg['size'] = readable_size(self.__TRANSFERRED_BYTES)
        msg['failed'] = self.__FAILED_CLONE
        return msg
