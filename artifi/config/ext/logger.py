"""Artifi Logging Configuration"""
import faulthandler
import logging
import os
import sys


class LogConfig:
    """Logger configuration"""

    def __init__(self, context):
        """@param context: pass :class Artifi"""
        self.context = context
        self._file_name = os.path.join(self.context.cwd, "artifi.log")
        faulthandler.enable(file=sys.stderr)
        logging.basicConfig(
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            handlers=[
                logging.FileHandler(filename=self._file_name, encoding="utf-8"),
                logging.StreamHandler(),
            ],
            level=logging.INFO,
        )
        self._logger = logging.getLogger(self.context.import_name)

    @property
    def logger(self) -> logging.Logger:
        """
        logger function
        @return:
        """
        return self._logger
