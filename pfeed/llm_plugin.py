'''
A simple wrapper around litellm with preferred free models, 
Current free apis:
- Gemini
- Groq
- Mistral
Use cases:
symbol=AAPL, which databento's dataset should I use?
'''
from typing import Literal
import warnings

with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=DeprecationWarning)
    import litellm


tFREE_LLM_PROVIDERS = Literal['gemini', 'groq', 'mistral']
def get_llm_providers():
    return [provider.value for provider in litellm.LlmProviders]


def get_free_llm_api_key(provider: tFREE_LLM_PROVIDERS) -> str | None:
    if provider == 'groq':
        return 'https://console.groq.com/keys'
    elif provider == 'mistral':
        return 'https://console.mistral.ai/api-keys'
    elif provider == 'gemini':
        return 'https://ai.google.dev/gemini-api/docs/api-key'


class LLM:
    DEFAULT_MODELS = {
        'gemini': 'gemini-1.5-flash',
        'groq': 'llama-3.2-90b-text-preview',
        'mistral': 'mistral-large-latest',
    }
    def __init__(self, provider: tFREE_LLM_PROVIDERS | str, model: str=''):
        self.provider = provider.lower()
        if model:
            model = model.lower()
        elif self.provider in self.DEFAULT_MODELS:
            model = self.DEFAULT_MODELS[self.provider]
        else:
            raise ValueError(f'No model found for {self.provider}, please specify one')
        self.model = model

    def ask(self, message: str, context="") -> str:
        response = litellm.completion(
            model='/'.join([self.provider, self.model]), 
            messages=[
                {"role": "system", "content": context},
                {"role": "user", "content": message}
            ] if context else [{"role": "user", "content": message}]
        )
        return response.choices[0].message.content
    
    