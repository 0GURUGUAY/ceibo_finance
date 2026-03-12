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


settings = Settings()
