"""Ext For Instagram"""
from instaloader import InstaloaderContext

from artifi import Artifi


class CustomContext(InstaloaderContext):
    """To Alter the Instaloader default logging method"""

    def __init__(self, acontext: Artifi):
        """@param acontext: Pass :class Artifi"""
        self.acontext = acontext
        super().__init__()

    def log(self, *msg, sep="", end="\n", flush=False):
        """

        @param msg:
        @param sep:
        @param end:
        @param flush:
        @return:
        """
        if flush:
            self.acontext.logger.info("\n")
        self.acontext.logger.info(f"{sep.join(map(str, msg))}{end}")

    def error(self, msg, repeat_at_end=True):
        """

        @param msg:
        @param repeat_at_end:
        @return:
        """
        self.acontext.logger.info(msg)
        if repeat_at_end:
            self.error_log.append(msg)

    def close(self):
        """@return:"""
        if self.error_log and not self.quiet:
            for err in self.error_log:
                self.acontext.logger.error(err)
        self._session.close()
