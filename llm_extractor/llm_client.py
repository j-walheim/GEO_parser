import os
import re
import time
from dotenv import load_dotenv
from openai import AzureOpenAI, RateLimitError
import requests

load_dotenv()

class LLMClient:
    @staticmethod
    def create(model='groq'):
        if model == "groq":
            return LLMClient._create_groq_client()
        else:
            return LLMClient._create_azure_client(model)


    @staticmethod
    def _create_azure_client(model):
        client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2024-05-01-preview"
        )
        return LLMClient(client, model)

    @staticmethod
    def _create_groq_client():
        api_key = os.getenv("GROQ_API_KEY")
        base_url = "https://api.groq.com/openai/v1/chat/completions"
        return LLMClient(None, "llama-3.1-70b-versatile", api_key, base_url)

    def __init__(self, client, model, api_key=None, base_url=None):
        self.client = client
        self.model = model
        self.api_key = api_key
        self.base_url = base_url

    def chat(self, messages):
        max_retries = 10
        retry_delay = 30

        for attempt in range(max_retries):
            try:
                if self.client:
                    return self._azure_chat(messages)
                else:
                    return self._groq_chat(messages)
            except (RateLimitError, requests.exceptions.RequestException) as e:
                if attempt < max_retries - 1:
                    print(f"API error. Attempt {attempt + 1}/{max_retries}. Waiting for {retry_delay} seconds before retrying...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print(f"Max retries reached. Error: {str(e)}")
                    return None
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                return None

        return None

    def _azure_chat(self, messages):
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages
        )
        return response.choices[0].message.content

    def _groq_chat(self, messages):
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        data = {
            "model": self.model,
            "messages": messages,
            "temperature": 0
        }
        response = requests.post(self.base_url, headers=headers, json=data)
        return response.json()['choices'][0]['message']['content']
# def create_extraction_chain(prompt, llm):
#     messages = [{"role": "user", "content": prompt}]
#     response = llm.chat(messages)

#     result = {
#         "reasoning": re.findall(r'<reasoning>(.*?)</reasoning>', response, re.DOTALL),
#         "extracted": [item.strip() for item in ','.join(re.findall(r'<answer>(.*?)</answer>', response, re.DOTALL)).split(',') if item.strip()]
#     }

#     return result

def get_llm(model='groq'):
    return LLMClient.create(model)
