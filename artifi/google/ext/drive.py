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
from artifi.utils import readable_size, fetch_mime_type, \
    sanitize_name, readable_time

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
                 use_service_acc=False,
                 is_team_drive=False
                 ):
        super().__init__(context)
        self.context: Artifi = context
        self.scope = scope
        self.parent_id = drive_id

        self.use_service_account = use_service_acc
        self.service_account_idx = randrange(len(os.listdir("accounts"))) if (
            self.use_service_account) else None
        self._is_td = is_team_drive

        self.updater = None
        self.sa_count = 0
        self.alt_auth = False
        self.__folder_mime_type = "application/vnd.google-apps.folder"
        self.__file_download_base_uri = "https://drive.google.com/uc?id={}&export=download"
        self.__folder_download_base_uri = "https://drive.google.com/drive/folders/{}"
        self._service = self.authorize()
        self._content_type = None
        self._failed_files = []

        self.is_uploading = False
        self.is_downloading = False
        self.is_cloning = False
        self.is_cancelled = False
        self.update_interval = 5

        self.total_bytes = 0
        self.total_files = 0
        self.total_folders = 0

        self.dl_filename = None
        self.dl_start_time = None
        self.dl_state = None

        self.ul_filename = None
        self.ul_start_time = None
        self.ul_state = None

        self.transferred_size = 0
        self.cl_start_time = None

    def drive_detail(self, fields=None):
        """
        @param fields: 
        @return: 
        """
        data = self._service.about().get(
            fields=fields if fields else "storageQuota").execute()
        return data

    @staticmethod
    def _id_by_url(link: str):
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

    def _file_size(self, **kwargs):
        """

        @param kwargs:
        """
        try:
            size = int(kwargs['size'])
        except:
            size = 0
        self.total_bytes += size

    def _folder_size(self, **kwargs) -> None:
        """

        @param kwargs:
        @return:
        """
        files = self._files_by_folder_id(kwargs['id'])
        if len(files) == 0:
            return
        for file_ in files:
            if file_['mimeType'] == self.__folder_mime_type:
                self.total_folders += 1
                self._folder_size(**file_)
            else:
                self.total_files += 1
                self._file_size(**file_)

    def _switch_service_account(self):
        """switch to service"""
        service_account_count = len(os.listdir("accounts"))
        if self.service_account_idx == service_account_count - 1:
            self.service_account_idx = 0
        self.sa_count += 1
        self.service_account_idx += 1
        self.context.logger.info(
            f"Switching to {self.service_account_idx}.json service account")
        self._service = self.authorize()

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def _set_permission(self, drive_id):
        permissions = {
            'role': 'reader',
            'type': 'anyone',
            'value': None,
            'withLink': True
        }
        return self._service.permissions().create(supportsTeamDrives=True,
                                                  fileId=drive_id,
                                                  body=permissions).execute()

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def _get_file_md(self, file_id):
        """

        @param file_id:
        @return:
        """
        return self._service.files().get(supportsAllDrives=True, fileId=file_id,
                                         fields="name,id,mimeType,size").execute()

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def _files_by_folder_id(self, folder_id):
        """

        @param folder_id:
        @return:
        """
        page_token = None
        q = f"'{folder_id}' in parents"
        files = []
        while True:
            response = self._service.files().list(supportsTeamDrives=True,
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
    def _get_file_id(self, file_name, mime_type, parent_id):
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

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def _get_folder_id(self, directory_name, parent_id):
        """
        @param directory_name: Name of the directory to be checked.
        @param parent_id: ID of the parent directory.
        @return: ID of the existing directory if found, otherwise None.
        """
        query = f"name='{directory_name}' and mimeType='{self.__folder_mime_type}'"
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

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def _create_folder(self, directory_name, parent_id):
        """
        @param directory_name: Name of the directory to be created or checked.
        @param parent_id: ID of the parent directory.
        @return: ID of the created or existing directory.
        """
        # Check if the directory already exists in the parent directory
        existing_directory_id = self._get_folder_id(directory_name, parent_id)
        if existing_directory_id:
            # Directory already exists, return its ID
            self.context.logger.info(
                f"Directory '{directory_name}' already exists. Returning existing ID: {existing_directory_id}"
            )
            return existing_directory_id

        # If the directory does not exist, create it
        file_metadata = {
            "name": directory_name,
            "mimeType": self.__folder_mime_type
        }
        if parent_id is not None:
            file_metadata["parents"] = [parent_id]

        file = self._service.files().create(
            supportsTeamDrives=True,
            body=file_metadata
        ).execute()

        file_id = file.get("id")
        if not self._is_td:
            self._set_permission(file_id)

        self.context.logger.info(
            f"Created G-Drive Folder:\nName: {file.get('name')}\nID: {file_id}"
        )
        return file_id

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

    def _delete_file(self, link: str):
        """

        @param link:
        @return:
        """
        try:
            file_id = self._id_by_url(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg
        msg = ''
        try:
            res = self._service.files().delete(fileId=file_id,
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

    @retry(wait=wait_exponential(multiplier=2, min=3, max=6),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(HttpError))
    def _copy_sfile(self, file_id, dest_id):
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
                self._service.files()
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
                            self._switch_service_account()
                            return self._copy_sfile(file_id, dest_id)
                    else:
                        self.is_cancelled = True
                        self.context.logger.info(f"Got: {reason}")
                        raise err
                else:
                    raise err

    def _clone_ext(self, link):
        """

        @param link:
        @return:
        """
        try:
            file_id = self._id_by_url(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg, "", "", ""
        self.context.logger.info(f"File ID: {file_id}")
        try:
            drive_file = self._service.files().get(fileId=file_id,
                                                   fields="id, name, mimeType, size",
                                                   supportsTeamDrives=True).execute()
            name = drive_file['name']
            self.context.logger.info(f"Checking: {name}")
            if drive_file['mimeType'] == self.__folder_mime_type:
                self._folder_size(**drive_file)
            else:
                try:
                    self.total_files += 1
                    self._file_size(**drive_file)
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
                    self._service = token_service
                    return self._clone_ext(link)
                msg = "File not found."
            else:
                msg = f"Error.\n{err}"
            return msg, "", "", ""
        return "", clonesize, name, files

    def _clone_folder(self, local_path, folder_id, parent_id):
        """
        @param local_path:
        @param folder_id:
        @param parent_id:
        @return:
        """
        self.context.logger.info(f"Syncing: {local_path}")
        files = self._files_by_folder_id(folder_id)
        if len(files) == 0:
            return parent_id
        for file in files:
            if file.get('mimeType') == self.__folder_mime_type:
                self.total_folders += 1
                file_path = os.path.join(local_path, file.get('name'))
                current_dir_id = self._create_folder(file.get('name'), parent_id)
                self._clone_folder(file_path, file.get('id'),
                                   current_dir_id)
            else:
                try:
                    self.total_files += 1
                    self.transferred_size += int(file.get('size'))
                except TypeError:
                    pass
                self._copy_sfile(file.get('id'), parent_id)
            if self.is_cancelled:
                break

    def on_download_progress(self):
        """

        @return:
        """
        if self.dl_state is not None:
            elapsed_time = time.time() - self.dl_start_time
            downloaded_size = self.dl_state.total_size * self.dl_state.progress()
            speed = readable_size(downloaded_size / elapsed_time)
            data = (f"filename: {self.dl_filename} "
                    f"Downloading: {readable_size(downloaded_size)}/{readable_size(self.dl_state.total_size)} "
                    f"time_elapsed: {readable_time(elapsed_time)} "
                    f"speed: {speed} "
                    )
            print(data)

    def _download_folder(self, folder_id, folder_path, folder_name):
        """

        @param folder_id:
        @param folder_path:
        @param folder_name:
        """
        path = os.path.join(folder_path, folder_name)
        os.makedirs(path, exist_ok=True)
        result = []
        page_token = None
        while True:
            files = self._service.files().list(
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
                self._download_folder(file_id, path, filename)
            elif not os.path.isfile(path + filename):
                self._download_file(file_id, path, filename, mime_type)
            if self.is_cancelled:
                break

    def _download_file(self, file_id, path, filename, mime_type):
        """

        @param file_id:
        @param path:
        @param filename:
        @param mime_type:
        @return:
        """
        new_file_name = sanitize_name(filename)
        self.dl_filename = new_file_name
        self.context.logger.info(
            f"<---Downloading--->\nFileName: {new_file_name}\nFileId: {file_id}"
        )
        if crm := export_mime.get(mime_type, None):
            request = self._service.files().export(fileId=file_id,
                                                   mimeType=crm[0])
            new_file_name += crm[1]
        else:
            request = self._service.files().get_media(fileId=file_id)

        file_path = os.path.join(path, new_file_name)

        fh = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request, chunksize=65 * 1024 * 1024)
        done = False
        while not done:
            if self.is_cancelled:
                fh.close()
                break
            try:

                self.dl_state, done = downloader.next_chunk()
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
                            self._switch_service_account()
                            self.context.logger.info(f"Got: {reason}, Trying Again...!")
                            return self._download_file(file_id, path, filename,
                                                       mime_type)
                    else:
                        self.is_cancelled = True
                        self._failed_files.append(file_id)
                        self.context.logger.error(
                            (f"Failed To Download FileName: {new_file_name}\n"
                             f"FileId: {file_id}\nReason: {reason}")
                        )
                        raise err

        self.context.logger.info(
            f"<---Downloaded--->\nFileName: {new_file_name}\nFileId: {file_id}"
        )

    def on_upload_progress(self):
        """

        @return:
        """

        if self.ul_state is not None:
            elapsed_time = time.time() - self.ul_start_time
            upload_size = self.ul_state.total_size * self.ul_state.progress()
            speed = readable_size(upload_size / elapsed_time)
            data = (f"filename:{self.dl_filename} "
                    f"Downloading:{readable_size(upload_size)}/{readable_size(self.ul_state.total_size)} "
                    f"time_elapsed:{readable_time(elapsed_time)} "
                    f"speed:{speed} "
                    )
            print(data)

    def _upload_empty_file(self, path, file_name, mime_type, parent_id=None):
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
        return self._service.files().create(supportsTeamDrives=True,
                                            body=file_metadata,
                                            media_body=media_body).execute()

    def _update_file(self, file_id, file_path):
        """
        Update the content of an existing file with the specified file ID.
        """
        media_body = MediaFileUpload(file_path,
                                     resumable=True,
                                     chunksize=50 * 1024 * 1024)

        drive_file = self._service.files().update(
            fileId=file_id,
            media_body=media_body
        ).execute()

        # Define file instance and get url for download
        drive_file = self._service.files().get(supportsTeamDrives=True,
                                               fileId=drive_file['id']).execute()
        download_url = self.__file_download_base_uri.format(drive_file.get('id'))
        return download_url

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

            current_file_name = os.path.join(input_directory, item)

            if os.path.isdir(current_file_name):
                current_dir_id = self._create_folder(item, parent_id)
                new_id = self._upload_folder(current_file_name, current_dir_id)
                self.total_folders += 1
            else:
                mime_type = fetch_mime_type(current_file_name)
                file_name = os.path.basename(current_file_name)

                # current_file_name will have the full path
                self._upload_file(current_file_name, file_name, mime_type, parent_id)
                self.total_files += 1
                new_id = parent_id
            if self.is_cancelled:
                break
        return new_id

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
        self.ul_filename = file_name
        # File body description
        file_metadata = {
            'name': file_name,
            'description': 'Uploaded by Slam Mirrorbot',
            'mimeType': mime_type,
        }
        try:
            self._content_type = file_metadata['mimeType']
        except:
            self._content_type = 'File'
        if parent_id is not None:
            file_metadata['parents'] = [parent_id]
        existing_file_id = self._get_file_id(file_name, mime_type, parent_id)
        if existing_file_id:
            # Update the existing file
            return self._update_file(existing_file_id, file_path)

        if os.path.getsize(file_path) == 0:
            media_body = MediaFileUpload(file_path,
                                         mimetype=mime_type,
                                         resumable=False)
            response = self._service.files().create(supportsTeamDrives=True,
                                                    body=file_metadata,
                                                    media_body=media_body).execute()
            if not self._is_td:
                self._set_permission(response['id'])

            drive_file = self._service.files().get(supportsTeamDrives=True,
                                                   fileId=response['id']).execute()
            download_url = self.__file_download_base_uri.format(drive_file.get('id'))
            return download_url
        media_body = MediaFileUpload(file_path,
                                     mimetype=mime_type,
                                     resumable=True,
                                     chunksize=50 * 1024 * 1024)

        # Insert a file
        drive_file = self._service.files().create(supportsTeamDrives=True,
                                                  body=file_metadata,
                                                  media_body=media_body)
        response = None
        while response is None:
            if self.is_cancelled:
                break
            try:
                self.ustatus, response = drive_file.next_chunk()
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
                        self._switch_service_account()
                        self.context.logger.info(f"Got: {reason}, Trying Again.")
                        return self._upload_file(file_path, file_name, mime_type,
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
            self._set_permission(response['id'])
        # Define file instance and get url for download
        drive_file = self._service.files().get(supportsTeamDrives=True,
                                               fileId=response['id']).execute()
        download_url = self.__file_download_base_uri.format(drive_file.get('id'))
        return download_url

    def properties(self, link):
        """

        @param link:
        @return:
        """
        try:
            file_id = self._id_by_url(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg
        msg = {}
        self.context.logger.info(f"File ID: {file_id}")
        try:
            drive_file = self._service.files().get(fileId=file_id,
                                                   fields="id, name, mimeType, size",
                                                   supportsTeamDrives=True).execute()
            name = drive_file['name']
            self.context.logger.info(f"Counting: {name}")
            if drive_file['mimeType'] == self.__folder_mime_type:
                self._folder_size(**drive_file)
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
                    self._file_size(**drive_file)
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
                    self._service = token_service
                    return self.properties(link)
        return msg

    def clone(self, link):
        """

        @param link:
        @return:
        """
        self.is_cloning = True
        cl_start_time = time.time()
        try:
            file_id = self._id_by_url(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg
        msg = {}
        self.context.logger.info(f"File ID: {file_id}")
        try:
            meta = self._get_file_md(file_id)
            if meta.get("mimeType") == self.__folder_mime_type:
                dir_id = self._create_folder(meta.get('name'), self.parent_id)
                self._clone_folder(meta.get('name'), meta.get('id'),
                                   dir_id)
                durl = self.__folder_download_base_uri.format(dir_id)
                if self.is_cancelled:
                    self.context.logger.info("Deleting cloned data from Drive...")
                    msg = self._delete_file(durl)
                    self.context.logger.info(f"{msg}")
                    return "your clone has been stopped and cloned data has been deleted!", "cancelled"
                msg['filename'] = meta.get("name")
                msg['size'] = readable_size(self.transferred_size)
                msg['type'] = "Folder"
                msg['sub_folders'] = self.total_folders
                msg['files'] = self.total_files

            else:
                file = self._copy_sfile(meta.get('id'), self.parent_id)
                msg['filename'] = file.get("name")
                durl = self.__file_download_base_uri.format(file.get("id"))
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
                    self._service = token_service
                    return self.clone(link)
        except HttpError as e:
            if e.resp.status == 404:
                self.context.logger.error(
                    f"HttpError {e.reason}")
            else:
                token_service = self.alt_authorize()
                if token_service is not None:
                    self._service = token_service
                    return self.clone(link)
        return msg

    def download(self, link):
        """

        @param link:
        @return:
        """
        self.dl_start_time = time.time()
        self.is_downloading = True
        file_id = self._id_by_url(link)

        path = os.path.join(self.context.directory, str(uuid.uuid4()).lower()[:5])
        os.makedirs(path)
        try:
            meta = self._get_file_md(file_id)
            if meta.get("mimeType") == self.__folder_mime_type:
                self.context.logger.info(
                    f"Downloading FolderName: {meta.get('name')}\nFolderId: {file_id}"
                )
                self._download_folder(file_id, path, meta.get('name'))
            else:
                self.context.logger.info(
                    f"Downloading FileName: {meta.get('name')}\nFileId: {file_id}"
                )
                self._download_file(file_id, path, meta.get('name'),
                                    meta.get('mimeType'))
        except Exception as err:
            err = str(err).replace('>', '').replace('<', '')
            self.context.logger.error(err)
            if "downloadQuotaExceeded" in str(err):
                err = "Download Quota Exceeded."
            elif "File not found" in str(err):
                token_service = self.alt_authorize()
                if token_service is not None:
                    self._service = token_service
                    return self.download(link)
                err = "File not found"
            self.context.logger.info(err)
            return path
        finally:
            self.updater.cancel()
            if self.is_cancelled:
                return "Cancelled"
        return self._failed_files

    def upload(self, file_folder_path: str):
        """

        @param file_folder_path:
        @return:
        """
        self.is_downloading = False
        self.is_uploading = True
        self.context.logger.info("Uploading File: " + file_folder_path)

        if os.path.isfile(file_folder_path):
            try:
                mime_type = fetch_mime_type(file_folder_path)
                file_name = os.path.basename(file_folder_path)
                link = self._upload_file(file_folder_path, file_name, mime_type,
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
                dir_id = self._create_folder(
                    os.path.basename(os.path.abspath(file_folder_path)), self.parent_id)
                result = self._upload_folder(file_folder_path, dir_id)
                if result is None:
                    raise Exception('Upload has been manually cancelled!')
                link = f"https://drive.google.com/folderview?id={dir_id}"
                if self.is_cancelled:
                    self.context.logger.info("Deleting uploaded data from Drive...")
                    msg = self._delete_file(link)
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
        typ = self._content_type
        self.context.logger.info(f"{folders},{files},{typ}")
        return link
