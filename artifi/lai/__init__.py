import g4f

from artifi import Artifi


class LAi:
    def __init__(self, context):
        self.context: Artifi = context
        g4f.debug.logging = True
        g4f.check_version = False
        self._chat_data: dict = {}

    def text_generation(self, chat_id, msg: str):
        try:
            if not self._chat_data.get(chat_id):
                self._chat_data[chat_id] = {"messages": [
                    {
                        "role": "user",
                        "content": msg.strip()
                    }
                ]}
            else:
                self._chat_data[chat_id]['messages'].append({
                    "role": "user",
                    "content": msg.strip()
                })
            response = g4f.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=self._chat_data.get(chat_id)['messages'],
                stream=True,
            )
            res_msg = ''.join(response)
            return res_msg
        except:
            return "Unable To Respond Your Message...!"
