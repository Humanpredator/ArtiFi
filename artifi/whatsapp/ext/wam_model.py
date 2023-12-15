from sqlalchemy import INTEGER, TIMESTAMP, VARCHAR, Column, ForeignKey
from sqlalchemy.orm import relationship

from artifi import Artifi


class WaProfileModel(Artifi.dbmodel):
    def __init__(self, context):
        self.context: Artifi = context

    __tablename__ = "wa_profile"
    wa_profile_pid = Column(INTEGER(), autoincrement=True, primary_key=True)
    wa_profile_waid = Column(VARCHAR())
    wa_profile_name = Column(VARCHAR())

    # logs
    wa_profile_created_at = Column(TIMESTAMP())
    wa_profile_updated_at = Column(TIMESTAMP())


class WaMessageModel(Artifi.dbmodel):
    def __init__(self, context):
        self.context: Artifi = context

    __tablename__ = "wa_message"

    wa_msg_pid = Column(INTEGER(), autoincrement=True, primary_key=True)
    wa_msg_id = Column(VARCHAR())

    wa_received_msg = Column(VARCHAR())
    wa_replied_msg = Column(VARCHAR())
    wa_msg_status = Column(VARCHAR())
    wa_msg_sent = Column(TIMESTAMP())
    wa_msg_delivered = Column(TIMESTAMP())

    wa_msg_created = Column(TIMESTAMP())
    wa_msg_updated = Column(TIMESTAMP())

    # relationship
    wa_profile_pid = Column(
        INTEGER(), ForeignKey("wa_profile.wa_profile_pid", ondelete="SET NULL")
    )
    wa_profile_pid_fk = relationship("WaProfileModel", foreign_keys=[wa_profile_pid])
