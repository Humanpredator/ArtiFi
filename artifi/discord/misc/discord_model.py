from datetime import datetime

from sqlalchemy import Column, INTEGER, VARCHAR, TIMESTAMP

from artifi import Artifi


class DiscordSudoModel(Artifi.dbmodel):
    def __init__(self, context):
        self.context: Artifi = context

    __tablename__ = "discord_sudo"
    id = Column(INTEGER(), autoincrement=True, primary_key=True)
    user_id = Column(VARCHAR())
    created_at = Column(TIMESTAMP())
    updated_at = Column(TIMESTAMP(), default=datetime.now())
