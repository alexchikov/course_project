from airflow.utils.context import Context
from airflow.notifications.basenotifier import BaseNotifier
from telegram import Bot
from utils.settings import Settings
import asyncio


class TelegramNotifier(BaseNotifier):
    template_fields = ("_message", )

    def __init__(self, message: str):
        super().__init__()
        self._message = message
        self._chat_id = Settings.CHAT_ID
        self._bot = Bot(token=Settings.BOT_TOKEN)

    async def send_message(self, message: str) -> None:
        await self._bot.send_message(self._chat_id, text="".join(message), parse_mode='html')

    def notify(self, context: Context) -> None:
        dag_id = context['ti'].dag_id
        task_id = context['ti'].task_id
        dag_state = context['ti'].state
        run_datetime = context['ti'].execution_date
        message = (f"<b>DAG ID</b>: {dag_id}\n" +
                   f"<b>Task ID</b>: {task_id}\n" +
                   f"<b>DAG state</b>: {dag_state}\n" +
                   f"<b>Run datetime</b>: {run_datetime}\n" +
                   f"<b>{self._message}<b>")
        asyncio.run(self.send_message("".join(message)))