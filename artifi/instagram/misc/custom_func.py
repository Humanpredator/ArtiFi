from instaloader import InstaloaderContext

from artifi import Artifi


class CustomContext(InstaloaderContext):
    def __init__(self, acontext: Artifi):
        self.acontext = acontext
        super().__init__()

    def log(self, *msg, sep="", end="\n", flush=False):
        log_message = sep.join(map(str, msg))
        self.acontext.logger.info(log_message)

    def error(self, msg, repeat_at_end=True):
        self.acontext.logger.info(msg)
        if repeat_at_end:
            self.error_log.append(msg)

    def close(self):
        if self.error_log and not self.quiet:
            for err in self.error_log:
                self.acontext.logger.error(err)
        self._session.close()
