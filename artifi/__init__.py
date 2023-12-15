import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

import pytz
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from cachetools import func
from flask import Flask
from sqlalchemy import Engine, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from .config import BaseConfig
from .config.exception import ArtifiException
from .config.logger import LogConfig


class Artifi(BaseConfig):
    dbmodel = declarative_base()

    def __init__(self, import_name, config_path: Optional[None | str] = None):
        super().__init__(import_name, config_path)

        self.import_name: str = import_name
        self._module_path: str = os.path.dirname(os.path.abspath(__file__))
        self._scheduler: BackgroundScheduler = BackgroundScheduler(
            jobstores={self.__class__.__name__: MemoryJobStore()}
        )
        self.cwd: str = self.get_root_path()
        self.directory: str = self._create_directory()
        self.logger: logging = LogConfig(self).logger
        self.db_engine: Engine = self._db_engine()
        self.fsapi: Flask = Flask(import_name)

        sys.excepthook = lambda exctype, value, traceback: self.logger.error(
            f"{traceback} || {exctype.__name__} || {value}"
        )

    def _create_directory(self):
        working_directory = os.path.join(self.cwd, "Downloads")
        os.makedirs(working_directory, exist_ok=True)
        return working_directory

    def _db_engine(self) -> Engine:
        try:
            engine = create_engine(self.SQLALCHEMY_DATABASE_URI, echo=False)
        except SQLAlchemyError as e:
            self.logger.info(f"Failed To Connect to DB, Existing...!\nReason: {e}")
            raise SQLAlchemyError("Failed To Connect To DB")
        return engine

    def db_session(self) -> Session:
        session_maker = sessionmaker(bind=self.db_engine)
        return session_maker()

    def add_scheduler(
        self,
        function: func,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        interval: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        allow_duplicate: bool = True,
    ):
        defaults = {
            "start_date": datetime.now().strftime("%Y-%m-%d"),
            "end_date": (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
            "start_time": "00:00",
            "end_time": "23:59",
            "interval": 60 * 24 if end_date is None else None,
        }
        start_date, end_date, start_time, end_time, interval = (
            value if value is not None else defaults[key]
            for key, value in zip(
                defaults.keys(), (start_date, end_date, start_time, end_time, interval)
            )
        )
        tz = pytz.timezone("asia/kolkata")
        start_datetime = tz.localize(
            datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
        )
        end_datetime = tz.localize(
            datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M")
        )
        job_id = f"{function.__name__}_job"
        self._scheduler.add_job(
            function,
            "interval",
            minutes=interval,
            start_date=start_datetime,
            end_date=end_datetime,
            id=job_id,
            replace_existing=allow_duplicate,
            jobstore=self.__class__.__name__,
        )
        self.logger.info(
            f"Function {function.__name__} was added to Scheduler with job ID: {job_id} ...!"
        )

    def start_schedular(self):
        return self._scheduler.start()

    @property
    def module_path(self):
        return self._module_path
