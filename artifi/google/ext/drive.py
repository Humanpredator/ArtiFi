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
from artifi.config.ext.exception import DriveDownloadError, DriveError, DriveUploadError
from artifi.google import Google
from artifi.utils import fetch_mime_type, readable_size, readable_time, sanitize_name

export_mime = {
    "application/vnd.google-apps.document": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".docx",
    ),
    "application/vnd.google-apps.spreadsheet": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xlsx",
    ),
    "application/vnd.google-apps.presentation": (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ".pptx",
    ),
    "application/vnd.google-apps.scenes": ("video/mp4", ".mp4"),
    "application/vnd.google-apps.jam": ("application/pdf", ".pdf"),
    "application/vnd.google-apps.script": (
        "application/vnd.google-apps.script+json",
        ".json",
    ),
    "application/vnd.google-apps.form": ("application/zip", ".zip"),
    "application/vnd.google-apps.drawing": ("image/jpeg", ".jpg"),
    "application/vnd.google-apps.site": ("text/plain", ".txt"),
    "application/vnd.google-apps.mail-layout": ("text/plain", ".txt"),
}


class GoogleDrive(Google):
    def __init__(
        self,
        context,
        scope,
        drive_id,
        use_service_acc=False,
        is_team_drive=False,
        stop_duplicate=True,
    ):
        super().__init__(context)
        self.context: Artifi = context
        self.scope = scope
        self.parent_id = drive_id

        self.use_service_account = use_service_acc
        self.service_account_idx = (
            randrange(len(os.listdir("accounts")))
            if (self.use_service_account)
            else None
        )
        self.is_team_drive = is_team_drive
        self.stop_duplicate = stop_duplicate
        self.sa_count = 0
        self.alt_auth = False
        self.drive_folder_mime = "application/vnd.google-apps.folder"
        self.dl_file_prefix = "https://drive.google.com/uc?id={}&export=download"
        self.dl_folder_prefix = "https://drive.google.com/drive/folders/{}"
        self._service = self.authorize()

        self.total_bytes = 0
        self.total_files = 0
        self.total_folders = 0

        self.transferred_size = 0

    def drive_detail(self, fields=None):
        """
        @param fields:
        @return:
        """
        data = (
            self._service.about()
            .get(fields=fields if fields else "storageQuota")
            .execute()
        )
        return data

    @staticmethod
    def get_id_by_url(link: str):
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
        return parse_qs(parsed.query)["id"][0]

    def get_file_size(self, **kwargs):
        """

        @param kwargs:
        """
        try:
            size = int(kwargs["size"])
        except:
            size = 0
        self.total_bytes += size

    def get_folder_size(self, **kwargs) -> None:
        """

        @param kwargs:
        @return:
        """
        files = self.list_files(kwargs["id"])
        if len(files) == 0:
            return
        for file_ in files:
            if file_["mimeType"] == self.drive_folder_mime:
                self.total_folders += 1
                self.get_folder_size(**file_)
            else:
                self.total_files += 1
                self.get_file_size(**file_)

    def switch_service_account(self):
        """switch to service"""
        service_account_count = len(os.listdir("accounts"))
        if self.service_account_idx == service_account_count - 1:
            self.service_account_idx = 0
        self.sa_count += 1
        self.service_account_idx += 1
        self.context.logger.info(
            f"Switching to {self.service_account_idx}.json service account"
        )
        self._service = self.authorize()

    @retry(
        wait=wait_exponential(multiplier=2, min=3, max=6),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(HttpError),
    )
    def set_permission(self, drive_id):
        """

        @param drive_id:
        @return:
        """
        if not self.is_team_drive:
            permissions = {
                "role": "reader",
                "type": "anyone",
                "value": None,
                "withLink": True,
            }
            return (
                self._service.permissions()
                .create(supportsTeamDrives=True, fileId=drive_id, body=permissions)
                .execute()
            )
        return None

    @retry(
        wait=wait_exponential(multiplier=2, min=3, max=6),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(HttpError),
    )
    def get_metadata(self, file_id):
        """

        @param file_id:
        @return:
        """
        return (
            self._service.files()
            .get(supportsAllDrives=True, fileId=file_id, fields="name,id,mimeType,size")
            .execute()
        )

    @retry(
        wait=wait_exponential(multiplier=2, min=3, max=6),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(HttpError),
    )
    def list_files(self, folder_id):
        """

        @param folder_id:
        @return:
        """
        page_token = None
        q = f"'{folder_id}' in parents"
        files = []
        while True:
            response = (
                self._service.files()
                .list(
                    supportsTeamDrives=True,
                    includeTeamDriveItems=True,
                    q=q,
                    spaces="drive",
                    pageSize=200,
                    fields="nextPageToken, files(id, name, mimeType,size)",
                    corpora="allDrives",
                    orderBy="folder, name",
                    pageToken=page_token,
                )
                .execute()
            )
            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break
        return files

    @retry(
        wait=wait_exponential(multiplier=2, min=3, max=6),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(HttpError),
    )
    def get_file_id(self, file_name, mime_type, parent_id):
        """
        Check if a file with the same name, mime type, and parent directory ID already exists.
        If it exists, return its ID; otherwise, return None.
        """
        query = f"name='{file_name}' and mimeType='{mime_type}'"
        if parent_id:
            query += f" and '{parent_id}' in parents"

        results = (
            self._service.files()
            .list(q=query, spaces="drive", fields="files(id)", supportsTeamDrives=True)
            .execute()
        )

        files = results.get("files", [])
        return files[0]["id"] if files else None

    @retry(
        wait=wait_exponential(multiplier=2, min=3, max=6),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(HttpError),
    )
    def get_folder_id(self, directory_name, parent_id):
        """
        @param directory_name: Name of the directory to be checked.
        @param parent_id: ID of the parent directory.
        @return: ID of the existing directory if found, otherwise None.
        """
        query = f"name='{directory_name}' and mimeType='{self.drive_folder_mime}'"
        if parent_id:
            query += f" and trashed=false and '{parent_id}' in parents"
        results = (
            self._service.files()
            .list(q=query, spaces="drive", fields="files(id)", supportsTeamDrives=True)
            .execute()
        )

        files = results.get("files", [])
        if files:
            # Return the ID of the first matching directory
            return files[0]["id"]
        else:
            return None

    @retry(
        wait=wait_exponential(multiplier=2, min=3, max=6),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(HttpError),
    )
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
        file_metadata = {"name": directory_name, "mimeType": self.drive_folder_mime}
        if parent_id is not None:
            file_metadata["parents"] = [parent_id]

        file = (
            self._service.files()
            .create(supportsTeamDrives=True, body=file_metadata)
            .execute()
        )

        file_id = file.get("id")
        if not self.is_team_drive:
            self.set_permission(file_id)

        self.context.logger.info(
            f"Created G-Drive Folder:\nName: {file.get('name')}\nID: {file_id}"
        )
        return file_id

    def authorize(self):
        """

        @return:
        """
        # Get credentials
        credentials = self.oauth_creds(
            self.scope, service_user=self.use_service_account, cname="drive"
        )
        return build("drive", "v3", credentials=credentials, cache_discovery=False)

    def alt_authorize(self):
        """

        @return:
        """
        if self.use_service_account and not self.alt_auth:
            self.alt_auth = True
            credentials = self.oauth_creds(
                self.scope, service_user=self.use_service_account, cname="drive"
            )
            return build("drive", "v3", credentials=credentials, cache_discovery=False)
        return None

    def delete_files(self, link: str):
        """

        @param link:
        @return:
        """
        try:
            file_id = self.get_id_by_url(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg
        msg = ""
        try:
            res = (
                self._service.files()
                .delete(fileId=file_id, supportsTeamDrives=self.is_team_drive)
                .execute()
            )
            msg = res
        except HttpError as err:
            self.context.logger.error(str(err))
            if "File not found" in str(err):
                msg = "No such file exist"
            else:
                msg = "Something went wrong check log"
        finally:
            return msg

    @retry(
        wait=wait_exponential(multiplier=2, min=3, max=6),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(HttpError),
    )
    def _copy_sfile(self, file_id, dest_id):
        """

        @param file_id:
        @param dest_id:
        @return:
        """
        body = {"parents": [dest_id]}

        try:
            return (
                self._service.files()
                .copy(supportsAllDrives=True, fileId=file_id, body=body)
                .execute()
            )

        except HttpError as err:
            if err.resp.get("content-type", "").startswith("application/json"):
                reason = (
                    json.loads(err.content).get("error").get("errors")[0].get("reason")
                )
                if reason in ["userRateLimitExceeded", "dailyLimitExceeded"]:
                    if self.use_service_account:
                        if self.sa_count == len(os.listdir("accounts")):
                            self.is_cancelled = True
                            raise err
                        else:
                            self.switch_service_account()
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
            file_id = self.get_id_by_url(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg, "", "", ""
        self.context.logger.info(f"File ID: {file_id}")
        try:
            drive_file = (
                self._service.files()
                .get(
                    fileId=file_id,
                    fields="id, name, mimeType, size",
                    supportsTeamDrives=True,
                )
                .execute()
            )
            name = drive_file["name"]
            self.context.logger.info(f"Checking: {name}")
            if drive_file["mimeType"] == self.drive_folder_mime:
                self.get_folder_size(**drive_file)
            else:
                try:
                    self.total_files += 1
                    self.get_file_size(**drive_file)
                except TypeError:
                    pass
            clonesize = self.total_bytes
            files = self.total_files
        except Exception as err:
            err = str(err).replace(">", "").replace("<", "")
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
        files = self.list_files(folder_id)
        if len(files) == 0:
            return parent_id
        for file in files:
            if file.get("mimeType") == self.drive_folder_mime:
                self.total_folders += 1
                file_path = os.path.join(local_path, file.get("name"))
                current_dir_id = self.create_folder(file.get("name"), parent_id)
                self._clone_folder(file_path, file.get("id"), current_dir_id)
            else:
                try:
                    self.total_files += 1
                    self.transferred_size += int(file.get("size"))
                except TypeError:
                    pass
                self._copy_sfile(file.get("id"), parent_id)
            if self.is_cancelled:
                break

    def clone(self, link):
        """

        @param link:
        @return:
        """
        self.is_cloning = True
        cl_start_time = time.time()
        try:
            file_id = self.get_id_by_url(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg
        msg = {}
        self.context.logger.info(f"File ID: {file_id}")
        try:
            meta = self.get_metadata(file_id)
            if meta.get("mimeType") == self.drive_folder_mime:
                dir_id = self.create_folder(meta.get("name"), self.parent_id)
                self._clone_folder(meta.get("name"), meta.get("id"), dir_id)
                durl = self.dl_folder_prefix.format(dir_id)
                if self.is_cancelled:
                    self.context.logger.info("Deleting cloned data from Drive...")
                    msg = self.delete_files(durl)
                    self.context.logger.info(f"{msg}")
                    return (
                        "your clone has been stopped and cloned data has been deleted!",
                        "cancelled",
                    )
                msg["filename"] = meta.get("name")
                msg["size"] = readable_size(self.transferred_size)
                msg["type"] = "Folder"
                msg["sub_folders"] = self.total_folders
                msg["files"] = self.total_files

            else:
                file = self._copy_sfile(meta.get("id"), self.parent_id)
                msg["filename"] = file.get("name")
                durl = self.dl_file_prefix.format(file.get("id"))
                self.context.logger.info(durl)
                try:
                    msg["type"] = file.get("mimeType")
                except:
                    msg["type"] = "File"
                try:
                    msg["size"] = readable_size(int(meta.get("size")))
                except TypeError:
                    pass
        except RetryError as err:
            self.context.logger.error(
                f"Total Attempts: {err.last_attempt.attempt_number}"
            )
            err = err.last_attempt.exception()
            if "User rate limit exceeded" in str(err):
                self.context.logger.error("User Limit Exceeded...!")
                token_service = self.alt_authorize()
                if token_service is not None:
                    self._service = token_service
                    return self.clone(link)
        except HttpError as e:
            if e.resp.status == 404:
                self.context.logger.error(f"HttpError {e.reason}")
            else:
                token_service = self.alt_authorize()
                if token_service is not None:
                    self._service = token_service
                    return self.clone(link)
        return msg

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

    @property
    def service(self):
        """

        @return:
        """
        return self._service


class DriveUpload:
    """Drive Upload Functionality"""

    def __init__(self, gdrive, directory_path):
        self.gdrive: GoogleDrive = gdrive
        self._upload_path = directory_path
        self.__UPLOADING = True

        self.__TOTAL_FILES = 0
        self.__TOTAL_FOLDERS = 0
        self.__CURRENT_FILE_NAME = None
        self.__CURRENT_FILE_SIZE = None

        self.__UPLOAD_STARTED_TIME = time.time()
        self.__UPLOAD_PROGRESS = None
        self.is_cancelled = False

    def on_upload_progress(self):
        """

        @return:
        """
        progress = {
            "filename": self.__CURRENT_FILE_NAME,
            "status": "Uploading",
            "size": readable_size(self.__CURRENT_FILE_SIZE),
            "progress": f"{readable_size(self.__CURRENT_FILE_SIZE)}/{readable_size(self.__CURRENT_FILE_SIZE)}",
            "elapsed": readable_time(time.time() - self.__UPLOAD_STARTED_TIME),
            "speed": "0/s",
        }
        if self.__UPLOAD_PROGRESS:
            self.__CURRENT_FILE_SIZE = self.__UPLOAD_PROGRESS.total_size
            upload_size = self.__CURRENT_FILE_SIZE * self.__UPLOAD_PROGRESS.progress()
            progress[
                "speed"
            ] = f"{readable_size(upload_size / (time.time() - self.__UPLOAD_STARTED_TIME))}/s"
            progress[
                "progress"
            ] = f"{readable_size(upload_size)}/{readable_size(self.__CURRENT_FILE_SIZE)}"

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
                raise DriveUploadError("Upload Cancelled!")
        return new_id

    def _duplicate_file(self, file_md, media_body):
        if self.gdrive.stop_duplicate and (
            ext_file_id := self.gdrive.get_file_id(
                file_md["name"], file_md["mimeType"], file_md["parents"][0]
            )
        ):
            drive_file = self.gdrive.service.files().update(
                fileId=ext_file_id, media_body=media_body
            )
        else:
            drive_file = self.gdrive.service.files().create(
                supportsTeamDrives=True, body=file_md, media_body=media_body
            )
        return drive_file

    @retry(
        wait=wait_exponential(multiplier=2, min=3, max=6),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(HttpError),
    )
    def _upload_file(self, file_path, file_name, mime_type, parent_id):
        """

        @param file_path:
        @param file_name:
        @param mime_type:
        @param parent_id:
        @return:
        """
        drive_file = None
        file_size = os.path.getsize(file_path)

        self.__CURRENT_FILE_NAME = file_name
        self.__CURRENT_FILE_SIZE = file_size

        # File body description
        file_metadata = {
            "name": file_name,
            "description": "Uploaded by ArtiFi",
            "mimeType": mime_type,
            "parents": [parent_id],
        }

        media_body = (
            MediaFileUpload(file_path, mimetype=mime_type, resumable=False)
            if file_size == 0
            else MediaFileUpload(
                file_path,
                mimetype=mime_type,
                resumable=True,
                chunksize=50 * 1024 * 1024,
            )
        )

        if (
            ul_file := self._duplicate_file(file_metadata, media_body)
        ) and file_size > 0:
            while not drive_file:
                if self.is_cancelled:
                    raise DriveUploadError("Drive Upload Cancelled")
                try:
                    self.__UPLOAD_PROGRESS, drive_file = ul_file.next_chunk()
                except HttpError as err:
                    reason = err.error_details[0]["reason"]

                    if self.gdrive.use_service_account and reason in [
                        "userRateLimitExceeded",
                        "dailyLimitExceeded",
                    ]:
                        self.gdrive.switch_service_account()
                        self.gdrive.context.logger.info(
                            f"{reason}, Using Service Account And Trying Again...!"
                        )
                        return self._upload_file(
                            file_path, file_name, mime_type, parent_id
                        )
                    else:
                        self.is_cancelled = True
                        self.gdrive.context.logger.info(f"Got: {reason}")
                        raise DriveError(f"Something Went Wrong {err}")
        else:
            drive_file = ul_file.execute()

        self.gdrive.set_permission(drive_file["id"])
        # Define file instance and get url for download
        file = (
            self.gdrive.service.files()
            .get(supportsTeamDrives=True, fileId=drive_file["id"])
            .execute()
        )
        file_url = self.gdrive.dl_file_prefix.format(file.get("id"))
        self.__TOTAL_FILES += 1
        return file_url

    @staticmethod
    def _local_directory_info(directory_path):
        output = {"size": 0, "sub_folder": 0, "files": 0}

        for root, sub_folders, files in os.walk(directory_path):
            output["sub_folder"] += len(sub_folders)
            output["files"] += len(files)

            for filename in files:
                filepath = os.path.join(root, filename)
                output["size"] += os.path.getsize(filepath)

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
            link = self._upload_file(
                self._upload_path, filename, mime_type, self.gdrive.parent_id
            )
            if self.is_cancelled:
                raise DriveUploadError("Upload Has Been Manually Cancelled!")
            if not link:
                raise DriveError("Unable to Get File Link!")
            self.gdrive.context.logger.info(f"Uploaded To G-Drive: {self._upload_path}")
            output["name"] = filename
            output["type"] = "File"
            output["link"] = link
        else:
            root_dir_name = os.path.basename(os.path.abspath(self._upload_path))
            root_dir_id = self.gdrive.create_folder(
                root_dir_name, self.gdrive.parent_id
            )

            result = self._upload_folder(self._upload_path, root_dir_id)
            if not result:
                raise DriveUploadError("Upload has been manually cancelled!")
            link = f"https://drive.google.com/folderview?id={root_dir_id}"
            if self.is_cancelled:
                self.gdrive.context.logger.info("Deleting uploaded data from Drive...")
                msg = self.gdrive.delete_files(link)
                self.gdrive.context.logger.info(f"{msg}")
            self.gdrive.context.logger.info(f"Uploaded To G-Drive: {self._upload_path}")
            output["name"] = root_dir_name
            output["type"] = "Folder"
            output["link"] = link
        output["files"] = self.__TOTAL_FILES
        output["folders"] = self.__TOTAL_FOLDERS
        output["elapsed"] = readable_time(time.time() - self.__UPLOAD_STARTED_TIME)
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

        self.__TOTAL_FILES = 0
        self.__TOTAL_FOLDERS = 0
        self.__FAILED_DOWNLOAD = []

        self.__CURRENT_FILE_NAME = None
        self.__CURRENT_FILE_SIZE = None

        self.__DOWNLOAD_PROGRESS = None
        self.is_cancelled = False

    def on_download_progress(self):
        """

        @return:
        """
        progress = {
            "filename": self.__CURRENT_FILE_NAME,
            "status": "Downloading",
            "size": readable_size(self.__CURRENT_FILE_SIZE),
            "progress": f"{readable_size(self.__CURRENT_FILE_SIZE)}/{readable_size(self.__CURRENT_FILE_SIZE)}",
            "elapsed": readable_time(time.time() - self.__DOWNLOAD_START_TIME),
            "speed": "0/s",
        }
        if self.__DOWNLOAD_PROGRESS:
            self.__CURRENT_FILE_SIZE = self.__DOWNLOAD_PROGRESS.total_size
            upload_size = self.__CURRENT_FILE_SIZE * self.__DOWNLOAD_PROGRESS.progress()
            progress[
                "speed"
            ] = f"{readable_size(upload_size / (time.time() - self.__DOWNLOAD_START_TIME))}/s"
            progress[
                "progress"
            ] = f"{readable_size(upload_size)}/{readable_size(self.__CURRENT_FILE_SIZE)}"

        return progress

    def _download_folder(self, folder_id, folder_path, folder_name):
        """

        @param folder_id:
        @param folder_path:
        @param folder_name:
        """
        new_folder_name = sanitize_name(folder_name)
        path = os.path.join(folder_path, new_folder_name)
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        result = []
        page_token = None
        while True:
            if self.is_cancelled:
                raise DriveDownloadError("Download Cancelled By User...!")
            files = (
                self.gdrive.service.files()
                .list(
                    supportsTeamDrives=True,
                    includeTeamDriveItems=True,
                    q=f"'{folder_id}' in parents",
                    fields="nextPageToken, files(id, name, mimeType, size, shortcutDetails)",
                    pageToken=page_token,
                    pageSize=1000,
                )
                .execute()
            )
            result.extend(files["files"])
            page_token = files.get("nextPageToken")
            if not page_token:
                break

        result = sorted(result, key=lambda k: k["name"])
        for item in result:
            if self.is_cancelled:
                raise DriveDownloadError("Download Cancelled By User...!")
            file_id = item["id"]
            filename = item["name"]

            mime_type = item["mimeType"]
            shortcut_details = item.get("shortcutDetails", None)
            if shortcut_details:
                file_id = shortcut_details["targetId"]
                mime_type = shortcut_details["targetMimeType"]
            if mime_type == "application/vnd.google-apps.folder":
                self.gdrive.context.logger.info(
                    f"Downloading FolderName:{new_folder_name}"
                )
                self._download_folder(file_id, path, filename)
                self.__TOTAL_FOLDERS += 1

            elif not os.path.isfile(path + filename):
                self._download_file(file_id, path, filename, mime_type)
        return True

    @retry(
        wait=wait_exponential(multiplier=2, min=3, max=6),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(HttpError),
    )
    def _download_file(self, file_id, path, filename, mime_type):
        """

        @param file_id:
        @param path:
        @param filename:
        @param mime_type:
        @return:
        """
        new_file_name = sanitize_name(filename)

        self.gdrive.context.logger.info(f"Downloading FileName: {new_file_name}")
        if crm := export_mime.get(mime_type, None):
            request = self.gdrive.service.files().export(
                fileId=file_id, mimeType=crm[0]
            )
            new_file_name += crm[1]
        else:
            request = self.gdrive.service.files().get_media(fileId=file_id)

        self.__CURRENT_FILE_NAME = new_file_name
        file_path = os.path.join(path, new_file_name)

        fh = io.FileIO(file_path, "wb")
        downloader = MediaIoBaseDownload(fh, request, chunksize=50 * 1024 * 1024)
        done = False
        while not done:
            if self.is_cancelled:
                fh.close()
                raise DriveDownloadError("Upload Cancelled By User...!")
            try:
                self.__DOWNLOAD_PROGRESS, done = downloader.next_chunk()
            except HttpError as err:
                reason = err.error_details[0]["reason"]
                print("==============>", err)
                if reason == "notFound":
                    print("here")
                    self.gdrive.context.logger.error(
                        f"Failed To Download FileName: {new_file_name} Reason: {reason}"
                    )
                    return self.__FAILED_DOWNLOAD.append(file_id)

                elif self.gdrive.use_service_account and reason in [
                    "userRateLimitExceeded",
                    "dailyLimitExceeded",
                ]:
                    self.gdrive.switch_service_account()
                    self.gdrive.context.logger.info(
                        f"{reason}, Using Service Account And Trying Again...!"
                    )
                else:
                    self.gdrive.context.logger.error(
                        f"Failed To Download FileName: {new_file_name} Reason: {reason}"
                    )
                    raise DriveError(f"Something Went Wrong,{err}")
        self.__TOTAL_FILES += 1
        return True

    def download(self):
        """
        @return:
        """
        file_id = self.gdrive.get_id_by_url(self._drive_link)
        path = os.path.join(
            self.gdrive.context.directory, str(uuid.uuid4()).lower()[:5]
        )
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        output = {}
        meta = self.gdrive.get_metadata(file_id)
        output["name"] = meta.get("name")
        output["path"] = path
        if meta.get("mimeType") == self.gdrive.drive_folder_mime:
            output["type"] = "Folder"
            self._download_folder(file_id, path, meta.get("name"))
        else:
            output["type"] = "File"
            self._download_file(file_id, path, meta.get("name"), meta.get("mimeType"))
        output["files"] = self.__TOTAL_FILES
        output["folders"] = self.__TOTAL_FOLDERS
        output["elapsed"] = readable_time(time.time() - self.__DOWNLOAD_START_TIME)
        return output


class DriveProperties:
    """
    Drive Download Functionality
    """

    def __init__(self, gdrive, drive_link):
        self.gdrive: GoogleDrive = gdrive
        self._drive_link = drive_link

    def properties(self, link):
        """

        @param link:
        @return:
        """
        try:
            file_id = self.gdrive.get_id_by_url(link)
        except (KeyError, IndexError):
            msg = "Google Drive ID could not be found in the provided link"
            return msg
        msg = {}
        self.gdrive.context.logger.info(f"File ID: {file_id}")
        drive_file = (
            self.gdrive.service.files()
            .get(
                fileId=file_id,
                fields="id, name, mimeType, size",
                supportsTeamDrives=True,
            )
            .execute()
        )
        name = drive_file["name"]
        self.gdrive.context.logger.info(f"Counting: {name}")
        if drive_file["mimeType"] == self.gdrive.drive_folder_mime:
            self.gdrive.get_folder_size(**drive_file)
            msg["filename"] = name
            msg["size"] = readable_size(self.gdrive.total_bytes)
            msg["type"] = "Folder"
            msg["sub_folders"] = self.gdrive.total_folders
            msg["files"] = self.gdrive.total_files
        else:
            msg["filename"] = name
            try:
                msg["type"] = drive_file["mimeType"]
            except:
                msg["type"] = "File"
            try:
                self.gdrive.total_files += 1
                self.gdrive.get_file_size(**drive_file)
                msg["size"] = readable_size(self.gdrive.total_bytes)
                msg["files"] = self.gdrive.total_files
            except TypeError:
                pass

        return msg
