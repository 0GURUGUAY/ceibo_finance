from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_name: str = 'ceibo_finance'
    app_env: str = 'dev'
    app_host: str = '127.0.0.1'
    app_port: int = 8000
    log_level: str = 'INFO'

    alpaca_api_key: str = ''
    alpaca_api_secret: str = ''
    alpaca_paper: bool = True
    alpaca_data_feed: str = 'iex'

    openai_api_key: str = ''
    openai_model: str = 'gpt-4.1-mini'

    anthropic_api_key: str = ''
    anthropic_model: str = 'claude-3-5-sonnet-latest'

    openrouter_api_key: str = ''
    openrouter_model: str = 'meta-llama/llama-3.3-70b-instruct'

    gemini_api_key: str = ''
    gemini_model: str = 'gemini-1.5-flash'


settings = Settings()
