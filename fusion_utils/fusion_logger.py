# fusion_logger.py
import logging
from io import StringIO
from email.mime.text import MIMEText
from .slackbot import SlackBot

class FusionLogger:
    def __init__(self, slack_bot_token=None, slack_channel=None):
        self.log_stream = StringIO()

        # Set up the console handler
        self.console_handler = logging.StreamHandler()
        self.console_handler.setLevel(logging.INFO)

        # Set up the memory handler
        self.memory_handler = logging.StreamHandler(self.log_stream)
        self.memory_handler.setLevel(logging.INFO)

        # Set up the file handler
        self.file_handler = logging.FileHandler('pipeline_log.txt')
        self.file_handler.setLevel(logging.INFO)

        # Create the logger
        self.logger = logging.getLogger('FusionLogger')
        self.logger.setLevel(logging.INFO)

        # Create a custom formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Set the formatter for all handlers
        self.console_handler.setFormatter(formatter)
        self.memory_handler.setFormatter(formatter)
        self.file_handler.setFormatter(formatter)

        # Add handlers to the logger
        self.logger.addHandler(self.console_handler)
        self.logger.addHandler(self.memory_handler)
        self.logger.addHandler(self.file_handler)

        # Set up the Slack handler if token and channel are provided
        self.slack_bot = None
        self.slack_channel = None
        if slack_bot_token and slack_channel:
            self.slack_bot = SlackBot(slack_bot_token)
            self.slack_channel = slack_channel

        # Prevent propagation to the root logger
        self.logger.propagate = False

    def log(self, message, level='info'):
        if level == 'info':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)
        elif level == 'critical':
            self.logger.critical(message)

        # Send the log message to Slack if the Slack bot is initialized
        if self.slack_bot:
            self.slack_bot.send_message(self.slack_channel, message)

    def get_log_contents(self):
        return self.log_stream.getvalue()

    def attach_to_email(self, email_message):
        log_contents = self.get_log_contents()
        attachment = MIMEText(log_contents, 'plain')
        attachment.add_header('Content-Disposition', 'attachment', filename='pipeline_log.txt')
        email_message.attach(attachment)