class ArtifiException(Exception):
    """Artifi Base Exception"""


class ConfigFileError(ArtifiException):
    """Artifi Configuration File Error"""


class DriveError(ArtifiException):
    """Drive Base Error"""


class DriveUploadError(ArtifiException):
    """Drive Upload Cancelled"""


class DriveDownloadError(ArtifiException):
    """Drive Upload Cancelled"""
