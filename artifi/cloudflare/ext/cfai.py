"""Cloudflare Beta AI"""
import os

from artifi import Artifi
from artifi.cloudflare import CloudFlare


class CloudFlareAi(CloudFlare):
    """Cloudflare AI REST API"""

    def __init__(self, context):
        """@param context: Artifi object"""
        super().__init__(context)
        self.context: Artifi = context

    def text_generation(self, chat_uid: str, msg: str, model="meta_fp") -> str:
        """
        @param chat_uid: unique string to store user prompt on memory
        @param msg: prompt
        @param model: pass the text-generation models
        @return: AI response for your msg
        """
        if not self._chat_data.get(chat_uid):
            self._chat_data[chat_uid] = {
                "messages": [{"role": "user", "content": msg.strip()}]
            }
        else:
            self._chat_data[chat_uid]["messages"].append(
                {"role": "user", "content": msg.strip()}
            )
        url = (f"{self._base_url}/{self.service}/{self.version}/accounts/"
               f"{self.account_id}/ai/run/{self._textmodels(model)}")

        response = self._cfrequest.post(url, json=self._chat_data[chat_uid],
                                        timeout=30)
        response.raise_for_status()
        data = response.json()
        msg = data["result"]["response"]
        return msg

    def image_generation(self, prompt: str, path: str = None, model="sd_xl") -> str:
        """
        @param prompt: prompt to be generated
        @param path: file path to store generated images
        @param model: text to image generation model
        @return: saved file path
        """
        if not path:
            path = self.context.directory
        payload = {"prompt": prompt.strip()}
        url = (f"{self._base_url}/{self.service}/{self.version}/accounts/"
               f"{self.account_id}/ai/run/{self._t2imodels(model)}")
        self.context.logger.info("Image Generation Is In Progress...")
        response = self._cfrequest.post(url, json=payload, timeout=30)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type").split("/")
        file_path = os.path.join(
            path, f"{prompt[:5] if len(prompt) > 5 else prompt}.{content_type[1]}"
        )
        match content_type[0]:
            case "image":
                with open(file_path, "wb") as byte:
                    byte.write(response.content)
            case _:
                raise Exception("Something Went Wrong...!")
        return file_path

    @staticmethod
    def _textmodels(model_name) -> str:
        """

        @param model_name:
        @return:
        """
        text_models = {
            "meta_fp": "@cf/meta/llama-2-7b-chat-fp16",
            "meta_q": "@cf/meta/llama-2-7b-chat-int8",
            "mistral_ift": "@cf/mistral/mistral-7b-instruct-v0.1",
            "awq_clm": "@hf/thebloke/codellama-7b-instruct-awq",
        }
        return text_models.get(model_name, "@cf/meta/llama-2-7b-chat-fp16")

    @staticmethod
    def _t2imodels(model_name) -> str:
        """

        @param model_name:
        @return:
        """
        t2i_models = {
            "sd_xl": "@cf/stabilityai/stable-diffusion-xl-base-1.0",
        }
        return t2i_models.get(model_name, "@cf/meta/llama-2-7b-chat-fp16")
