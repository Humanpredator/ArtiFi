"""Artifi Main"""
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta
from typing import List, Optional, Any

import pytz
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from cachetools import func
from flask import Flask
from sqlalchemy import Engine, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from artifi.config.ext.logger import LogConfig
from .config import BaseConfig


class Artifi(BaseConfig):
    """
    Needs to be initiated first and required by all other class
    example_usage: arti=Artifi(__name__)
    """

    dbmodel = declarative_base()

    def __init__(self, import_name, config_path: Optional[None | str] = None):
        """
        @param import_name: Current file name
        @param config_path: config.env path
        """
        super().__init__(import_name, config_path)

        self.import_name: str = import_name
        self._module_path: str = os.path.dirname(os.path.abspath(__file__))
        self._scheduler: BackgroundScheduler = BackgroundScheduler(
            jobstores={self.__class__.__name__: MemoryJobStore()}
        )
        self.cwd: str = self.get_root_path()
        self.directory: str = self._create_directory()
        self.logger: logging.Logger = LogConfig(self).logger
        self.db_engine: Engine = self._db_engine()
        self.fsapi: Flask = Flask(import_name)
        self.tz: datetime.tzinfo = pytz.timezone('Asia/Kolkata')
        sys.excepthook = lambda exctype, value, tb: self.logger.critical(
            "".join(traceback.format_exception(exctype, value, tb))
        )

    def create_db_table(self, tables: List[dbmodel]):
        """

        @param tables: List of model class
                example: DBModel
        @return: It will return whether the table is created or not
        """
        if tables:
            for table in tables:
                table(self).__table__.create(self.db_engine, checkfirst=True)
        return True

    def _create_directory(self):
        """

        @return create and return the default directory for the file and other junks
                required for artifi:
        """
        working_directory = os.path.join(self.cwd, "Downloads")
        os.makedirs(working_directory, exist_ok=True)
        return working_directory

    def _db_engine(self) -> Engine:
        """@return connect to db and return the connection engine:"""
        try:
            engine = create_engine(self.SQLALCHEMY_DATABASE_URI, echo=False)
        except SQLAlchemyError as e:
            self.logger.error(f"Failed To Connect to DB, Existing...!\nReason: {e}")
            raise SQLAlchemyError("Failed To Connect To DB")
        return engine

    def db_session(self) -> Session:
        """@return: New DB session"""
        session_maker = sessionmaker(bind=self.db_engine)
        return session_maker()

    def add_scheduler(
            self,
            function: func,
            args: Optional[Any] = None,
            job_id: Optional[str] = None,
            start_date: Optional[str] = None,
            start_time: Optional[str] = None,
            end_date: Optional[str] = None,
            end_time: Optional[str] = None,
            interval: Optional[int] = None,
            no_duplicate: bool = True,
    ):
        """
        @param job_id:
        @param args:
        @param function: A Callable function
        @param start_time: Determine when to start the scheduler
        @param end_time: Determine when to stop the scheduler
        @param interval: Time delay between execution
        @param start_date: Starting date of the scheduler execution
        @param end_date: Starting date of the scheduler execution
        @param no_duplicate: 'True' to replace the existing scheduler with same
                                job_id.'False' to add same scheduler with same job_id.
        example_usage: self.add_scheduler(function,'HH:MM','HH:MM','YYYY-MM-DD',
                       'YYYY-MM-DD',60)
        Note: If the start_time or end_time is not given it will take 00:00 and 23:59,
              and if start_date or end_date is not given it will run every day, and
              the default interval will be 24hrs
        """
        defaults = {
            "start_date": datetime.now().strftime("%Y-%m-%d"),
            "end_date": (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
            "start_time": "00:00",
            "end_time": "23:59",
            "interval": 60 * 24,
        }
        start_date, end_date, start_time, end_time, interval = (
            value if value is not None else defaults[key]
            for key, value in zip(defaults.keys(),
                                  (start_date, end_date, start_time, end_time,
                                   interval))
        )

        start_datetime = self.tz.localize(
            datetime.strptime(f"{start_date} {start_time}",
                              "%Y-%m-%d %H:%M")
        )
        end_datetime = self.tz.localize(
            datetime.strptime(f"{end_date} {end_time}",
                              "%Y-%m-%d %H:%M")
        )
        job_func_id = job_id if job_id else f"{function.__name__}_job"
        self._scheduler.add_job(
            function,
            args=args,
            trigger="interval",
            minutes=interval,
            start_date=start_datetime,
            end_date=end_datetime,
            id=job_func_id,
            replace_existing=no_duplicate,
            jobstore=self.__class__.__name__,
        )
        self.logger.info(
            (f"Function {function.__name__} was added to Scheduler "
             f"with job ID: {job_func_id} ...!")
        )

    def start_scheduler(self):
        """

        @return: It will start all the scheduler job in then self.__scheduler and
                 return the status
        """
        return self._scheduler.start()

    @property
    def module_path(self):
        """@return package path:"""
        return self._module_path
