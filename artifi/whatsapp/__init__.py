from datetime import datetime

from cachetools import func
from flask import jsonify, request
from requests import Session
from sqlalchemy import and_

from artifi import Artifi
from artifi.cloudflare.ext.cfai import CloudFlareAi
from artifi.whatsapp.ext.wam_model import WaMessageModel, WaProfileModel


class WhatsApp:
    def __init__(self, context: Artifi):
        self.context = context
        self.base_url = "https://graph.facebook.com"
        self.version = "v17.0"
        self.wa_access_token = self.context.WHATSAPP_TOKEN
        self.wa_number_id = self.context.WHATSAPP_NUMBER_ID
        self.res_func: func = None
        self._cfai = CloudFlareAi(context)
        self.context.fsapi.add_url_rule(
            "/callback/wa-webhook",
            "webhook_server",
            self._chat_endpoint,
            methods=["POST", "GET"],
        )

        WaProfileModel(self.context).__table__.create(
            self.context.db_engine, checkfirst=True
        )
        WaMessageModel(self.context).__table__.create(
            self.context.db_engine, checkfirst=True
        )

    def send_text_message(self, wa_id: str, message: str):
        url = f"{self.base_url}/{self.version}/{self.wa_number_id}/messages"
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": wa_id,
            "type": "text",
            "text": {"preview_url": False, "body": message.strip()},
        }

        response = self._wa_request.post(url, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        self._ssr(data, msg=message)
        msg_id = data["messages"][0]["id"]

        return msg_id

    def _ssr(self, obj, **kwargs):
        wa_id: str = obj.get("contacts")[0]["wa_id"]
        msg_id: str = obj.get("messages")[0]["id"]
        with self.context.db_session() as session:
            profile = (
                session.query(WaProfileModel)
                .filter(WaProfileModel.wa_profile_waid == wa_id)
                .first()
            )
            if not profile:
                profile = WaProfileModel(self.context)
                profile.wa_profile_created_at = datetime.now()
            profile.wa_profile_waid = wa_id
            profile.wa_profile_updated_at = datetime.now()
            wam = WaMessageModel(self.context)
            wam.wa_replied_msg = kwargs["msg"]
            wam.wa_msg_created = datetime.now()
            wam.wa_profile_pid = profile.wa_profile_pid
            wam.wa_msg_id = msg_id
            session.add(profile)
            session.add(wam)
            session.commit()

    def _chat_endpoint(self):
        if request.method == "POST":
            data = WaPhraseMessage(request.json)
            if data.ibm_type == "MSG":
                if not self.res_func:
                    res_msg = self._cfai.text_generation(data.wa_id, data.msg_text)
                    msg_id = self.send_text_message(data.wa_id, res_msg)
                else:
                    msg_id, res_msg = self.res_func(self, data)
                with self.context.db_session() as session:
                    profile = (
                        session.query(WaProfileModel)
                        .filter(WaProfileModel.wa_profile_waid == data.wa_id)
                        .first()
                    )
                    if not profile:
                        profile = WaProfileModel(self.context)
                        profile.wa_profile_created_at = datetime.now()
                    profile.wa_profile_waid = data.wa_id
                    profile.wa_profile_name = data.profile_name
                    profile.wa_profile_updated_at = datetime.now()
                    wam = WaMessageModel(self.context)
                    wam.wa_msg_created = datetime.now()
                    wam.wa_profile_pid = profile.wa_profile_pid
                    wam.wa_msg_id = msg_id
                    wam.wa_received_msg = data.msg_text
                    wam.wa_replied_msg = res_msg
                    session.add(profile)
                    session.add(wam)
                    session.commit()

            elif data.ibm_type == "STS":
                with self.context.db_session() as session:
                    profile = (
                        session.query(WaProfileModel)
                        .filter(WaProfileModel.wa_profile_waid == data.wa_id)
                        .first()
                    )
                    if not profile:
                        self.context.logger.info("No Profile Found, Skipping...!")
                    else:
                        wam = (
                            session.query(WaMessageModel)
                            .filter(
                                and_(
                                    WaMessageModel.wa_profile_pid
                                    == profile.wa_profile_pid,
                                    WaMessageModel.wa_msg_id == data.msg_id,
                                )
                            )
                            .first()
                        )
                        if not wam:
                            self.context.logger.info("No Message Found, Skipping...!")
                        else:
                            wam.wa_profile_pid = profile.wa_profile_pid
                            wam.wa_msg_id = data.msg_id
                            wam.wa_msg_status = data.msg_status
                            if data.msg_status == "sent":
                                wam.wa_msg_sent = datetime.now()
                            elif data.msg_status == "delivered":
                                wam.wa_msg_delivered = datetime.now()
                            wam.wa_msg_updated = datetime.now()
                            session.add(wam)
                            session.commit()
            return jsonify("Request Processed Successfully...!"), 200

        elif request.method == "GET":
            mode = request.args.get("hub.mode")
            token = request.args.get("hub.verify_token")
            challenge = request.args.get("hub.challenge")
            if mode and token:
                if (
                    mode == "subscribe"
                    and token == self.context.WHATSAPP_WEBHOOK_SECRET
                ):
                    return challenge, 200
                else:
                    return jsonify("Unauthorized"), 403
        return jsonify("Unable To Process Request"), 400

    @property
    def _wa_request(self) -> Session:
        _session = Session()
        _session.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.wa_access_token}",
        }
        return _session

    def run_whserver(self, host="0.0.0.0", port=5000):
        self.context.fsapi.run(host, port)


class WaPhraseMessage:
    def __init__(self, inbound_obj):
        self._inbound_obj = inbound_obj
        self._profile_name = None
        self._wa_id = None
        self._mobile_number = None
        self._msg_id = None
        self._received_msg_text = None
        self._status = None
        self._ibm_type = None
        self._phrase_webhook()

    def _phrase_webhook(self):
        if (entry := self._inbound_obj.get("entry", [])) and isinstance(entry, list):
            if (changes := entry[0].get("changes", [])) and isinstance(changes, list):
                if (value := changes[0].get("value", {})) and isinstance(value, dict):
                    if (contacts := value.get("contacts", [])) and isinstance(
                        contacts, list
                    ):
                        self._profile_name = contacts[0].get("profile").get("name")
                        self._wa_id = contacts[0].get("wa_id")

                    if (status := value.get("statuses", [])) and isinstance(
                        contacts, list
                    ):
                        self._wa_id = status[0].get("recipient_id")
                        self._status = status[0].get("status")
                        self._msg_id = status[0].get("id")
                        self._ibm_type = "STS"

                    if (messages := value.get("messages", [])) and isinstance(
                        messages, list
                    ):
                        if text := messages[0].get("text", {}).get("body", ""):
                            self._mobile_number = messages[0].get("from", "")
                            self._received_msg_text = text
                            self._ibm_type = "MSG"
                            return self
        return "NOCNT"

    @property
    def mobile_number(self) -> str:
        return self._mobile_number

    @property
    def msg_id(self) -> str:
        return self._msg_id

    @property
    def msg_text(self) -> str:
        return self._received_msg_text

    @property
    def wa_id(self) -> str:
        return self._wa_id

    @property
    def msg_status(self) -> str:
        return self._status

    @property
    def ibm_type(self) -> str:
        return self._ibm_type

    @property
    def profile_name(self) -> str:
        return self._profile_name
