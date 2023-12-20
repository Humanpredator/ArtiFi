"""Discord Sudo User Model"""
from datetime import datetime

from sqlalchemy import INTEGER, TIMESTAMP, VARCHAR, Column

from artifi import Artifi


class DiscordSudoModel(Artifi.dbmodel):
    """Manager Sudo Users"""

    def __init__(self, context):
        """@param context:"""
        self.context: Artifi = context

    __tablename__ = "discord_authorize"
    pid = Column(INTEGER(), autoincrement=True, primary_key=True)
    user_id = Column(VARCHAR())
    created_at = Column(TIMESTAMP())
    updated_at = Column(TIMESTAMP(), default=datetime.now())
