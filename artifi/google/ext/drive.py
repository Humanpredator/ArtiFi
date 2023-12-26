"""Google Drive"""
import io
import json
import os
import re
import time
import urllib.parse as urlparse
import uuid
from random import randrange
from urllib.parse import parse_qs

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from tenacity import *

from artifi import Artifi
from artifi.google import Google
from artifi.utils import readable_size, DriveSetInterval, fetch_mime_type


class GoogleDrive(Google):

    def __init__(self, context,
                 scope,
                 parent_id,
                 use_service_acc=False,
                 is_team_drive=False,
                 name=None
                 ):
        super().__init__(context)
        self.typee = None
        self.use_service_account = use_service_acc
        self.service_account_idx = randrange(len(os.listdir("accounts"))) if (
            self.use_service_account) else None
        self._is_td = is_team_drive
        self.parent_id = parent_id
        self.recursive_search = False
        self.context: Artifi = context
        self.scope = scope
        self.__G_DRIVE_DIR_MIME_TYPE = "application/vnd.google-apps.folder"
        self.__G_DRIVE_BASE_DOWNLOAD_URL = "https://drive.google.com/uc?id={}&export=download"
        self.__G_DRIVE_DIR_BASE_DOWNLOAD_URL = "https://drive.google.com/drive/folders/{}"
        self.__service = self.authorize()
        self._file_uploaded_bytes = 0
        self._file_downloaded_bytes = 0
        self.uploaded_bytes = 0
        self.downloaded_bytes = 0
        self.stopDup = True
        self.start_time = 0
        self.total_time = 0
        self.dtotal_time = 0
        self.is_uploading = False
        self.is_downloading = False
        self.is_cloning = False
        self.is_cancelled = False
        self.status = None
        self.dstatus = None
        self.updater = None
        self.name = name
        self.update_interval = 3
        self.telegraph_content = []
        self.path = []
        self.total_bytes = 0
        self.total_files = 0
        self.total_folders = 0
        self.transferred_size = 0
        self.sa_count = 0
        self.alt_auth = False

    def speed(self):
        """
        It calculates the average upload speed and returns it in bytes/seconds unit
        :return: Upload speed in bytes/second
        """
        try:
            return self.uploaded_bytes / self.total_time
        except ZeroDivisionError:
            return 0

    def dspeed(self):
        """

        @return:
        """
        try:
            return self.downloaded_bytes / self.dtotal_time
        except ZeroDivisionError:
            return 0

    def cspeed(self):
        """

        @return:
        """
        try:
            return self.transferred_size / int(time.time() - self.start_time)
        except ZeroDivisionError:
            return 0

    @staticmethod
    def getIdFromUrl(link: str):
        """

        @param link:
        @return:
        """
        if "folders" in link or "file" in link:
            regex = r"https://drive\.google\.com/(drive)?/?u?/?\d?/?(mobile)?/?(file)?(folders)?/?d?/([-\w]+)[?+]?/?(w+)?"
            res = re.search(regex, link)
            if res is None:
                raise IndexError("G-Drive ID not found.")
            return res.group(5)
        parsed = urlparse.urlparse(link)
        return parse_qs(parsed.query)['id'][0]

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def _on_upload_progress(self):
        if self.status is not None:
            chunk_size = self.status.total_size * self.status.progress() - self._file_uploaded_bytes
            self._file_uploaded_bytes = self.status.total_size * self.status.progress()
            self.context.logger.debug(
                f'Uploading {self.name}, chunk size: {readable_size(chunk_size)}')
            self.uploaded_bytes += chunk_size
            self.total_time += self.update_interval
            print("HERE")

    def __upload_empty_file(self, path, file_name, mime_type, parent_id=None):
        media_body = MediaFileUpload(path,
                                     mimetype=mime_type,
                                     resumable=False)
        file_metadata = {
            'name': file_name,
            'description': 'Uploaded using Slam Mirrorbot',
            'mimeType': mime_type,
        }
        if parent_id is not None:
            file_metadata['parents'] = [parent_id]
        return self.__service.files().create(supportsTeamDrives=True,
                                             body=file_metadata,
                                             media_body=media_body).execute()

    def switchServiceAccount(self):
        """switch to service"""
        service_account_count = len(os.listdir("accounts"))
        if self.service_account_idx == service_account_count - 1:
            self.service_account_idx = 0
        self.sa_count += 1
        self.service_account_idx += 1
        self.context.logger.info(
            f"Switching to {self.service_account_idx}.json service account")
        self.__service = self.authorize()

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def __set_permission(self, drive_id):
        permissions = {
            'role': 'reader',
            'type': 'anyone',
            'value': None,
            'withLink': True
        }
        return self.__service.permissions().create(supportsTeamDrives=True,
                                                   fileId=drive_id,
                                                   body=permissions).execute()

    def update_existing_file(self, file_id, file_path):
        """
        Update the content of an existing file with the specified file ID.
        """
        media_body = MediaFileUpload(file_path,
                                     resumable=True,
                                     chunksize=50 * 1024 * 1024)

        drive_file = self.__service.files().update(
            fileId=file_id,
            media_body=media_body
        ).execute()

        # Define file instance and get url for download
        drive_file = self.__service.files().get(supportsTeamDrives=True,
                                                fileId=drive_file['id']).execute()
        download_url = self.__G_DRIVE_BASE_DOWNLOAD_URL.format(drive_file.get('id'))
        return download_url

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def upload_file(self, file_path, file_name, mime_type, parent_id):
        """

        @param file_path:
        @param file_name:
        @param mime_type:
        @param parent_id:
        @return:
        """
        # File body description
        file_metadata = {
            'name': file_name,
            'description': 'Uploaded by Slam Mirrorbot',
            'mimeType': mime_type,
        }
        try:
            self.typee = file_metadata['mimeType']
        except:
            self.typee = 'File'
        if parent_id is not None:
            file_metadata['parents'] = [parent_id]
        existing_file_id = self.get_existing_file_id(file_name, mime_type, parent_id)
        if existing_file_id:
            # Update the existing file
            return self.update_existing_file(existing_file_id, file_path)

        if os.path.getsize(file_path) == 0:
            media_body = MediaFileUpload(file_path,
                                         mimetype=mime_type,
                                         resumable=False)
            response = self.__service.files().create(supportsTeamDrives=True,
                                                     body=file_metadata,
                                                     media_body=media_body).execute()
            if not self._is_td:
                self.__set_permission(response['id'])

            drive_file = self.__service.files().get(supportsTeamDrives=True,
                                                    fileId=response['id']).execute()
            download_url = self.__G_DRIVE_BASE_DOWNLOAD_URL.format(drive_file.get('id'))
            return download_url
        media_body = MediaFileUpload(file_path,
                                     mimetype=mime_type,
                                     resumable=True,
                                     chunksize=50 * 1024 * 1024)

        # Insert a file
        drive_file = self.__service.files().create(supportsTeamDrives=True,
                                                   body=file_metadata,
                                                   media_body=media_body)
        response = None
        while response is None:
            if self.is_cancelled:
                break
            try:
                self.status, response = drive_file.next_chunk()
            except HttpError as err:
                if err.resp.get('content-type', '').startswith('application/json'):
                    reason = json.loads(err.content).get('error').get('errors')[0].get(
                        'reason')
                    if reason not in [
                        'userRateLimitExceeded',
                        'dailyLimitExceeded',
                    ]:
                        raise err
                    if self.use_service_account:
                        self.switchServiceAccount()
                        self.context.logger.info(f"Got: {reason}, Trying Again.")
                        return self.upload_file(file_path, file_name, mime_type,
                                                parent_id)
                    else:
                        self.is_cancelled = True
                        self.context.logger.info(f"Got: {reason}")
                        raise err
        if self.is_cancelled:
            return
        self._file_uploaded_bytes = 0
        # Insert new permissions
        if not self._is_td:
            self.__set_permission(response['id'])
        # Define file instance and get url for download
        drive_file = self.__service.files().get(supportsTeamDrives=True,
                                                fileId=response['id']).execute()
        download_url = self.__G_DRIVE_BASE_DOWNLOAD_URL.format(drive_file.get('id'))
        return download_url

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def copyFile(self, file_id, dest_id):
        """

        @param file_id:
        @param dest_id:
        @return:
        """
        body = {
            'parents': [dest_id]
        }

        try:
            return (
                self.__service.files()
                .copy(supportsAllDrives=True, fileId=file_id, body=body)
                .execute()
            )

        except HttpError as err:
            if err.resp.get('content-type', '').startswith('application/json'):
                reason = json.loads(err.content).get('error').get('errors')[0].get(
                    'reason')
                if reason in ['userRateLimitExceeded', 'dailyLimitExceeded']:
                    if self.use_service_account:
                        if self.sa_count == len(os.listdir("accounts")):
                            self.is_cancelled = True
                            raise err
                        else:
                            self.switchServiceAccount()
                            return self.copyFile(file_id, dest_id)
                    else:
                        self.is_cancelled = True
                        self.context.logger.info(f"Got: {reason}")
                        raise err
                else:
                    raise err

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def getFileMetadata(self, file_id):
        """

        @param file_id:
        @return:
        """
        return self.__service.files().get(supportsAllDrives=True, fileId=file_id,
                                          fields="name,id,mimeType,size").execute()

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def getFilesByFolderId(self, folder_id):
        """

        @param folder_id:
        @return:
        """
        page_token = None
        q = f"'{folder_id}' in parents"
        files = []
        while True:
            response = self.__service.files().list(supportsTeamDrives=True,
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

    def cloneFolder(self, local_path, folder_id, parent_id):
        """
        @param local_path:
        @param folder_id:
        @param parent_id:
        @return:
        """
        self.context.logger.info(f"Syncing: {local_path}")
        files = self.getFilesByFolderId(folder_id)
        if len(files) == 0:
            return parent_id
        for file in files:
            if file.get('mimeType') == self.__G_DRIVE_DIR_MIME_TYPE:
                self.total_folders += 1
                file_path = os.path.join(local_path, file.get('name'))
                current_dir_id = self.create_directory(file.get('name'), parent_id)
                self.cloneFolder(file_path, file.get('id'),
                                 current_dir_id)
            else:
                try:
                    self.total_files += 1
                    self.transferred_size += int(file.get('size'))
                except TypeError:
                    pass
                self.copyFile(file.get('id'), parent_id)
            if self.is_cancelled:
                break

    def get_existing_file_id(self, file_name, mime_type, parent_id):
        """
        Check if a file with the same name, mime type, and parent directory ID already exists.
        If it exists, return its ID; otherwise, return None.
        """
        query = f"name='{file_name}' and mimeType='{mime_type}'"
        if parent_id:
            query += f" and '{parent_id}' in parents"

        results = self.__service.files().list(
            q=query,
            spaces="drive",
            fields="files(id)",
            supportsTeamDrives=True
        ).execute()

        files = results.get("files", [])
        return files[0]["id"] if files else None

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def get_directory_id(self, directory_name, parent_id):
        """
        @param directory_name: Name of the directory to be checked.
        @param parent_id: ID of the parent directory.
        @return: ID of the existing directory if found, otherwise None.
        """
        query = f"name='{directory_name}' and mimeType='{self.__G_DRIVE_DIR_MIME_TYPE}'"
        if parent_id:
            query += f" and trashed=false and '{parent_id}' in parents"
        results = self.__service.files().list(
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

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def create_directory(self, directory_name, parent_id):
        """
        @param directory_name: Name of the directory to be created or checked.
        @param parent_id: ID of the parent directory.
        @return: ID of the created or existing directory.
        """
        # Check if the directory already exists in the parent directory
        existing_directory_id = self.get_directory_id(directory_name, parent_id)
        if existing_directory_id:
            # Directory already exists, return its ID
            self.context.logger.info(
                f"Directory '{directory_name}' already exists. Returning existing ID: {existing_directory_id}"
            )
            return existing_directory_id

        # If the directory does not exist, create it
        file_metadata = {
            "name": directory_name,
            "mimeType": self.__G_DRIVE_DIR_MIME_TYPE
        }
        if parent_id is not None:
            file_metadata["parents"] = [parent_id]

        file = self.__service.files().create(
            supportsTeamDrives=True,
            body=file_metadata
        ).execute()

        file_id = file.get("id")
        if not self._is_td:
            self.__set_permission(file_id)

        self.context.logger.info(
            f"Created G-Drive Folder:\nName: {file.get('name')}\nID: {file_id}"
        )
        return file_id

    def upload_dir(self, input_directory, parent_id):
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

            current_file_name = os.path.join(input_directory, item)

            if os.path.isdir(current_file_name):
                current_dir_id = self.create_directory(item, parent_id)
                new_id = self.upload_dir(current_file_name, current_dir_id)
                self.total_folders += 1
            else:
                mime_type = fetch_mime_type(current_file_name)
                file_name = os.path.basename(current_file_name)
                # current_file_name will have the full path
                self.upload_file(current_file_name, file_name, mime_type, parent_id)
                self.total_files += 1
                new_id = parent_id
            if self.is_cancelled:
                break
        return new_id

    def authorize(self):
        """

        @return:
        """
        # Get credentials
        credentials = self.oauth_creds(self.scope,
                                       service_user=self.use_service_account,
                                       cname="drive")
        return build('drive', 'v3', credentials=credentials, cache_discovery=False)

    def alt_authorize(self):
        """

        @return:
        """
        if self.use_service_account and not self.alt_auth:
            self.alt_auth = True
            credentials = self.oauth_creds(self.scope,
                                           service_user=self.use_service_account,
                                           cname="drive")
            return build('drive', 'v3', credentials=credentials, cache_discovery=False)
        return None

    def get_recursive_list(self, file, rootid="root"):
        """

        @param file:
        @param rootid:
        @return:
        """
        rtnlist = []
        if not rootid:
            rootid = file.get('teamDriveId')
        if rootid == "root":
            rootid = self.__service.files().get(fileId='root',
                                                fields="id").execute().get('id')
        x = file.get("name")
        y = file.get("id")
        while y != rootid:
            rtnlist.append(x)
            file = self.__service.files().get(
                fileId=file.get("parents")[0],
                supportsAllDrives=True,
                fields='id, name, parents'
            ).execute()
            x = file.get("name")
            y = file.get("id")
        rtnlist.reverse()
        return rtnlist

    def gDrive_file(self, **kwargs):
        """

        @param kwargs:
        """
        try:
            size = int(kwargs['size'])
        except:
            size = 0
        self.total_bytes += size

    def gDrive_directory(self, **kwargs) -> None:
        """

        @param kwargs:
        @return:
        """
        files = self.getFilesByFolderId(kwargs['id'])
        if len(files) == 0:
            return
        for file_ in files:
            if file_['mimeType'] == self.__G_DRIVE_DIR_MIME_TYPE:
                self.total_folders += 1
                self.gDrive_directory(**file_)
            else:
                self.total_files += 1
                self.gDrive_file(**file_)

    def clonehelper(self, link):
        """

        @param link:
        @return:
        """
        try:
            file_id = self.getIdFromUrl(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg, "", "", ""
        self.context.logger.info(f"File ID: {file_id}")
        try:
            drive_file = self.__service.files().get(fileId=file_id,
                                                    fields="id, name, mimeType, size",
                                                    supportsTeamDrives=True).execute()
            name = drive_file['name']
            self.context.logger.info(f"Checking: {name}")
            if drive_file['mimeType'] == self.__G_DRIVE_DIR_MIME_TYPE:
                self.gDrive_directory(**drive_file)
            else:
                try:
                    self.total_files += 1
                    self.gDrive_file(**drive_file)
                except TypeError:
                    pass
            clonesize = self.total_bytes
            files = self.total_files
        except Exception as err:
            err = str(err).replace('>', '').replace('<', '')
            self.context.logger.error(err)
            if "File not found" in str(err):
                token_service = self.alt_authorize()
                if token_service is not None:
                    self.__service = token_service
                    return self.clonehelper(link)
                msg = "File not found."
            else:
                msg = f"Error.\n{err}"
            return msg, "", "", ""
        return "", clonesize, name, files

    def download_folder(self, folder_id, path, folder_name):
        """

        @param folder_id:
        @param path:
        @param folder_name:
        """
        if not os.path.exists(path + folder_name):
            os.makedirs(path + folder_name)
        path += folder_name + '/'
        result = []
        page_token = None
        while True:
            files = self.__service.files().list(
                supportsTeamDrives=True,
                includeTeamDriveItems=True,
                q=f"'{folder_id}' in parents",
                fields='nextPageToken, files(id, name, mimeType, size, shortcutDetails)',
                pageToken=page_token,
                pageSize=1000).execute()
            result.extend(files['files'])
            page_token = files.get("nextPageToken")
            if not page_token:
                break

        result = sorted(result, key=lambda k: k['name'])
        for item in result:
            file_id = item['id']
            filename = item['name']
            mime_type = item['mimeType']
            shortcut_details = item.get('shortcutDetails', None)
            if shortcut_details is not None:
                file_id = shortcut_details['targetId']
                mime_type = shortcut_details['targetMimeType']
            if mime_type == 'application/vnd.google-apps.folder':
                self.download_folder(file_id, path, filename)
            elif not os.path.isfile(path + filename):
                self.download_file(file_id, path, filename, mime_type)
            if self.is_cancelled:
                break

    def download_file(self, file_id, path, filename, mime_type):
        """

        @param file_id:
        @param path:
        @param filename:
        @param mime_type:
        @return:
        """
        request = self.__service.files().get_media(fileId=file_id)
        filename = filename.replace('/', '')
        fh = io.FileIO('{}{}'.format(path, filename), 'wb')
        downloader = MediaIoBaseDownload(fh, request, chunksize=65 * 1024 * 1024)
        done = False
        while not done:
            if self.is_cancelled:
                fh.close()
                break
            try:
                self.dstatus, done = downloader.next_chunk()
            except HttpError as err:
                if err.resp.get('content-type', '').startswith('application/json'):
                    reason = json.loads(err.content).get('error').get('errors')[0].get(
                        'reason')
                    if reason not in [
                        'downloadQuotaExceeded',
                        'dailyLimitExceeded',
                    ]:
                        raise err
                    if self.use_service_account:
                        if self.sa_count == len(os.listdir("accounts")):
                            self.is_cancelled = True
                            raise err
                        else:
                            self.switchServiceAccount()
                            self.context.logger.info(f"Got: {reason}, Trying Again...")
                            return self.download_file(file_id, path, filename,
                                                      mime_type)
                    else:
                        self.is_cancelled = True
                        self.context.logger.info(f"Got: {reason}")
                        raise err
        self._file_downloaded_bytes = 0

    def _on_download_progress(self):
        if self.dstatus is not None:
            chunk_size = self.dstatus.total_size * self.dstatus.progress() - self._file_downloaded_bytes
            self._file_downloaded_bytes = self.dstatus.total_size * self.dstatus.progress()
            self.downloaded_bytes += chunk_size
            self.dtotal_time += self.update_interval

    def upload(self, file_folder_path: str):
        """

        @param file_folder_path:
        @return:
        """
        self.is_downloading = False
        self.is_uploading = True
        self.context.logger.info("Uploading File: " + file_folder_path)

        self.updater = DriveSetInterval(self.update_interval, self._on_upload_progress)
        if os.path.isfile(file_folder_path):
            try:
                mime_type = fetch_mime_type(file_folder_path)
                file_name = os.path.basename(file_folder_path)
                link = self.upload_file(file_folder_path, file_name, mime_type,
                                        self.parent_id)
                if self.is_cancelled:
                    return
                if link is None:
                    raise Exception('Upload has been manually cancelled')
                self.context.logger.info("Uploaded To G-Drive: " + file_folder_path)
            except Exception as e:
                if isinstance(e, RetryError):
                    self.context.logger.info(
                        f"Total Attempts: {e.last_attempt.attempt_number}")
                    err = e.last_attempt.exception()
                else:
                    err = e
                self.context.logger.error(err)
                return
            finally:
                self.updater.cancel()
                if self.is_cancelled:
                    return
        else:
            try:
                dir_id = self.create_directory(
                    os.path.basename(os.path.abspath(file_folder_path)), self.parent_id)
                result = self.upload_dir(file_folder_path, dir_id)
                if result is None:
                    raise Exception('Upload has been manually cancelled!')
                link = f"https://drive.google.com/folderview?id={dir_id}"
                if self.is_cancelled:
                    self.context.logger.info("Deleting uploaded data from Drive...")
                    msg = self.deletefile(link)
                    self.context.logger.info(f"{msg}")
                    return
                self.context.logger.info("Uploaded To G-Drive: " + file_folder_path)
            except Exception as e:
                if isinstance(e, RetryError):
                    self.context.logger.info(
                        f"Total Attempts: {e.last_attempt.attempt_number}")
                    err = e.last_attempt.exception()
                else:
                    err = e
                self.context.logger.error(err)
                return
            finally:
                self.updater.cancel()
                if self.is_cancelled:
                    return
        files = self.total_files
        folders = self.total_folders
        typ = self.typee
        self.context.logger.info(f"{folders},{files},{typ}")
        return link

    def clone(self, link):
        """

        @param link:
        @return:
        """
        self.is_cloning = True
        self.start_time = time.time()
        self.total_files = 0
        self.total_folders = 0
        try:
            file_id = self.getIdFromUrl(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg
        msg = {}
        self.context.logger.info(f"File ID: {file_id}")
        try:
            meta = self.getFileMetadata(file_id)
            if meta.get("mimeType") == self.__G_DRIVE_DIR_MIME_TYPE:
                dir_id = self.create_directory(meta.get('name'), self.parent_id)
                self.cloneFolder(meta.get('name'), meta.get('id'),
                                 dir_id)
                durl = self.__G_DRIVE_DIR_BASE_DOWNLOAD_URL.format(dir_id)
                if self.is_cancelled:
                    self.context.logger.info("Deleting cloned data from Drive...")
                    msg = self.deletefile(durl)
                    self.context.logger.info(f"{msg}")
                    return "your clone has been stopped and cloned data has been deleted!", "cancelled"
                msg['filename'] = meta.get("name")
                msg['size'] = readable_size(self.transferred_size)
                msg['type'] = "Folder"
                msg['sub_folders'] = self.total_folders
                msg['files'] = self.total_files

            else:
                file = self.copyFile(meta.get('id'), self.parent_id)
                msg['filename'] = file.get("name")
                durl = self.__G_DRIVE_BASE_DOWNLOAD_URL.format(file.get("id"))
                self.context.logger.info(durl)
                try:
                    msg['type'] = file.get('mimeType')
                except:
                    msg['type'] = 'File'
                try:
                    msg['size'] = readable_size(int(meta.get("size")))
                except TypeError:
                    pass
        except RetryError as err:
            self.context.logger.error(
                f"Total Attempts: {err.last_attempt.attempt_number}")
            err = err.last_attempt.exception()
            if "User rate limit exceeded" in str(err):
                self.context.logger.error("User Limit Exceeded...!")
                token_service = self.alt_authorize()
                if token_service is not None:
                    self.__service = token_service
                    return self.clone(link)
        except HttpError as e:
            if e.resp.status == 404:
                self.context.logger.error(
                    f"HttpError {e.reason}")
            else:
                token_service = self.alt_authorize()
                if token_service is not None:
                    self.__service = token_service
                    return self.clone(link)
        return msg

    def count(self, link):
        """

        @param link:
        @return:
        """
        try:
            file_id = self.getIdFromUrl(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg
        msg = {}
        self.context.logger.info(f"File ID: {file_id}")
        try:
            drive_file = self.__service.files().get(fileId=file_id,
                                                    fields="id, name, mimeType, size",
                                                    supportsTeamDrives=True).execute()
            name = drive_file['name']
            self.context.logger.info(f"Counting: {name}")
            if drive_file['mimeType'] == self.__G_DRIVE_DIR_MIME_TYPE:
                self.gDrive_directory(**drive_file)
                msg['filename'] = name
                msg['size'] = readable_size(self.total_bytes)
                msg['type'] = "Folder"
                msg['sub_folders'] = self.total_folders
                msg['files'] = self.total_files
            else:
                msg['filename'] = name
                try:
                    msg['type'] = drive_file['mimeType']
                except:
                    msg['type'] = 'File'
                try:
                    self.total_files += 1
                    self.gDrive_file(**drive_file)
                    msg['size'] = readable_size(self.total_bytes)
                    msg['files'] = self.total_files
                except TypeError:
                    pass

        except HttpError as e:
            if e.resp.status == 404:
                self.context.logger.error(
                    f"HttpError {e.reason}")
            else:
                token_service = self.alt_authorize()
                if token_service is not None:
                    self.__service = token_service
                    return self.count(link)
        return msg

    def download(self, link):
        """

        @param link:
        @return:
        """
        self.is_downloading = True
        file_id = self.getIdFromUrl(link)
        self.updater = DriveSetInterval(self.update_interval,
                                        self._on_download_progress)
        path = f"{self.context.directory}/{str(uuid.uuid4()).lower()[:5]}/"
        try:
            meta = self.getFileMetadata(file_id)
            if meta.get("mimeType") == self.__G_DRIVE_DIR_MIME_TYPE:
                self.download_folder(file_id, path, meta.get('name'))
            else:
                os.makedirs(path)
                self.download_file(file_id, path, meta.get('name'),
                                   meta.get('mimeType'))
        except Exception as err:
            err = str(err).replace('>', '').replace('<', '')
            self.context.logger.error(err)
            if "downloadQuotaExceeded" in str(err):
                err = "Download Quota Exceeded."
            elif "File not found" in str(err):
                token_service = self.alt_authorize()
                if token_service is not None:
                    self.__service = token_service
                    return self.download(link)
                err = "File not found"
            self.context.logger.info(err)
            return path
        finally:
            self.updater.cancel()
            if self.is_cancelled:
                return "Cancelled"

    def cancel_download(self):
        """
        Cancel Download
        """
        self.is_cancelled = True
        if self.is_downloading:
            self.context.logger.info(f"Cancelling Download: {self.name}")
        elif self.is_cloning:
            self.context.logger.info(f"Cancelling Clone: {self.name}")
        elif self.is_uploading:
            self.context.logger.info(f"Cancelling Upload: {self.name}")

    def deletefile(self, link: str):
        """

        @param link:
        @return:
        """
        try:
            file_id = self.getIdFromUrl(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg
        msg = ''
        try:
            res = self.__service.files().delete(fileId=file_id,
                                                supportsTeamDrives=self._is_td).execute()
            msg = res
        except HttpError as err:
            self.context.logger.error(str(err))
            if "File not found" in str(err):
                msg = "No such file exist"
            else:
                msg = "Something went wrong check log"
        finally:
            return msg
