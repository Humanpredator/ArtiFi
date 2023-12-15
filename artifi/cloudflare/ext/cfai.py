from artifi import Artifi
from artifi.cloudflare import CloudFlare


class CloudFlareAi(CloudFlare):
    def __init__(self, context):
        super().__init__(context)
        self.context: Artifi = context

    def text_generation(self, chat_uid: str, msg: str, model="meta_fp") -> str:
        if not self._chat_data.get(chat_uid):
            self._chat_data[chat_uid] = {
                "messages": [{"role": "user", "content": msg.strip()}]
            }
        else:
            self._chat_data[chat_uid]["messages"].append(
                {"role": "user", "content": msg.strip()}
            )
        url = f"{self._base_url}/{self.service}/{self.version}/accounts/{self.account_id}/ai/run/{self._textmodels(model)}"

        response = self._request.post(url, json=self._chat_data[chat_uid], timeout=30)
        response.raise_for_status()
        data = response.json()
        msg = data["result"]["response"]
        return msg

    @staticmethod
    def _textmodels(model_name) -> str:
        text_models = {
            "meta_fp": "@cf/meta/llama-2-7b-chat-fp16",
            "meta_q": "@cf/meta/llama-2-7b-chat-int8",
            "mistral_ift": "@cf/mistral/mistral-7b-instruct-v0.1",
            "awq_clm": "@hf/thebloke/codellama-7b-instruct-awq",
        }
        return text_models.get(model_name, "@cf/meta/llama-2-7b-chat-fp16")

    @staticmethod
    def _t2imodels(model_name) -> str:
        t2i_models = {
            "sd_xl": "@cf/stabilityai/stable-diffusion-xl-base-1.0",
        }
        return t2i_models.get(model_name, "@cf/meta/llama-2-7b-chat-fp16")
