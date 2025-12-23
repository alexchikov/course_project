from dataclasses import dataclass
from yaml import safe_load
import os


with open(f'{os.environ["HOME"]}/dags/config.yaml') as config:
    result_config = safe_load(config)
    MOEX_SECURITIES_URL = result_config['URL_MOEX_SECURITIES']
    MOEX_AGGREGATES_URL = result_config['URL_MOEX_AGGREGATES']  #
    MOEX_HISTORY_URL = result_config['URL_MOEX_HISTORY']
    BOT_TOKEN = result_config['BOT_TOKEN']
    CHAT_ID = result_config['CHAT_ID']
    JDBC_CONNECTION_URL = result_config['JDBC_CONNECTION_URL']
    JDBC_USER = result_config['JDBC_USER']
    JDBC_PASSWORD = result_config['JDBC_PASSWORD']
    JDBC_TABLE_SECURITIES = result_config['JDBC_TABLE_SECURITIES']
    JDBC_TABLE_AGGREGATES = result_config['JDBC_TABLE_AGGREGATES']
    JDBC_TABLE_HISTORY = result_config['JDBC_TABLE_HISTORY']


@dataclass
class Settings:
    BOT_TOKEN: str = BOT_TOKEN
    MOEX_SECURITIES_URL: str = MOEX_SECURITIES_URL
    MOEX_AGGREGATES_URL: str = MOEX_AGGREGATES_URL
    MOEX_HISTORY_URL: str = MOEX_HISTORY_URL
    CHAT_ID: str = CHAT_ID
    JDBC_CONNECTION_URL: str = JDBC_CONNECTION_URL
    JDBC_USER: str = JDBC_USER
    JDBC_PASSWORD: str = JDBC_PASSWORD
    JDBC_TABLE_SECURITIES: str = JDBC_TABLE_SECURITIES
    JDBC_TABLE_AGGREGATES: str = JDBC_TABLE_AGGREGATES
    JDBC_TABLE_HISTORY: str = JDBC_TABLE_HISTORY
