from dataclasses import dataclass
from yaml import safe_load

with open('/home/aleks/Документы/kursovoy_poekt/dags/config.yaml') as config:
    result_config = safe_load(config)
    MOEX_URL = result_config['URL_MOEX_SECURITIES']
    BOT_TOKEN = result_config['BOT_TOKEN']
    CHAT_ID = result_config['CHAT_ID']
    JDBC_CONNECTION_URL = result_config['JDBC_CONNECTION_URL']
    JDBC_USER = result_config['JDBC_USER']
    JDBC_PASSWORD = result_config['JDBC_PASSWORD']
    JDBC_TABLE_SECURITIES = result_config['JDBC_TABLE_SECURITIES']
    JDBC_TABLE_AGGREGATES = result_config['JDBC_TABLE_AGGREGATES']


@dataclass
class Settings:
    BOT_TOKEN: str = BOT_TOKEN
    MOEX_URL: str = MOEX_URL
    CHAT_ID: int = CHAT_ID
    JDBC_CONNECTION_URL: str = JDBC_CONNECTION_URL
    JDBC_USER: str = JDBC_USER
    JDBC_PASSWORD: str = JDBC_PASSWORD
    JDBC_TABLE_SECURITIES: str = JDBC_TABLE_SECURITIES
    JDBC_TABLE_AGGREGATES: str = JDBC_TABLE_AGGREGATES
