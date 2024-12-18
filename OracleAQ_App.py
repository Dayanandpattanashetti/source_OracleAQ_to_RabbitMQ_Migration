import os
import sys
import time
import json
import signal
import configparser
from typing import Callable, Dict, Any
from datetime import datetime
from dataclasses import dataclass

import cx_Oracle


@dataclass
class OracleConfig:
    username: str
    password: str
    host: str
    port: int
    service_name: str
    queue_name: str
    queue_table: str
    batch_size: int
    wait_timeout: int
    max_retries: int
    retry_delay: int

    @property
    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service_name}"

    @classmethod
    def load_config(cls, config_path='config.ini'):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        config = configparser.ConfigParser()
        config.read(config_path)

        return cls(
            username=config.get('oracle', 'username'),
            password=config.get('oracle', 'password'),
            host=config.get('oracle', 'host'),
            port=config.getint('oracle', 'port'),
            service_name=config.get('oracle', 'service_name'),
            queue_name=config.get('oracle', 'queue_name'),
            queue_table=config.get('oracle', 'queue_table'),
            batch_size=config.getint('application', 'batch_size'),
            wait_timeout=config.getint('application', 'wait_timeout'),
            max_retries=config.getint('application', 'max_retries'),
            retry_delay=config.getint('application', 'retry_delay')
        )


class MessageProducer:
    def __init__(self, config: OracleConfig):
        self.config = config
        self.connection = None
        self.queue = None

    def connect(self):
        for attempt in range(self.config.max_retries):
            try:
                self.connection = cx_Oracle.connect(
                    user=self.config.username,
                    password=self.config.password,
                    dsn=self.config.dsn
                )
                self.queue = self.connection.queue(
                    self.config.queue_name,
                    payloadType=cx_Oracle.OBJECT
                )
                return
            except cx_Oracle.Error:
                if attempt == self.config.max_retries - 1:
                    raise
                time.sleep(self.config.retry_delay)

    def send_message(self, message: Dict[str, Any]):
        try:
            if not self.connection:
                self.connect()

            message_text = json.dumps(message)
            self.queue.enqOne(self.connection.msgproperties(payload=message_text))
            self.connection.commit()
            print(f"Message sent successfully: {message}")
        except Exception as e:
            print(f"Error sending message: {str(e)}")
            raise

    def close(self):
        if self.connection:
            self.connection.close()


class MessageSubscriber:
    def __init__(self, config: OracleConfig):
        self.config = config
        self.connection = None
        self.queue = None

    def connect(self):
        for attempt in range(self.config.max_retries):
            try:
                self.connection = cx_Oracle.connect(
                    user=self.config.username,
                    password=self.config.password,
                    dsn=self.config.dsn
                )
                self.queue = self.connection.queue(
                    self.config.queue_name,
                    payloadType=cx_Oracle.OBJECT
                )
                return
            except cx_Oracle.Error:
                if attempt == self.config.max_retries - 1:
                    raise
                time.sleep(self.config.retry_delay)

    def start_listening(self, callback: Callable[[dict], None]):
        try:
            if not self.connection:
                self.connect()

            self.queue.deqOptions.wait = self.config.wait_timeout
            self.queue.deqOptions.navigation = cx_Oracle.DEQ_FIRST_MSG

            print(f"Started listening to queue: {self.config.queue_name}")

            while True:
                messages = []
                for _ in range(self.config.batch_size):
                    try:
                        message = self.queue.deqOne()
                        if message and message.payload:
                            messages.append(json.loads(message.payload.decode()))
                    except cx_Oracle.empty:
                        break

                if messages:
                    for msg in messages:
                        try:
                            callback(msg)
                        except Exception as e:
                            print(f"Error processing message: {str(e)}")

                    self.connection.commit()
        except Exception as e:
            print(f"Error in message listening: {str(e)}")
            raise
        finally:
            self.close()

    def close(self):
        if self.connection:
            self.connection.close()


def publish_message():
    try:
        config = OracleConfig.load_config()
        producer = MessageProducer(config)

        message_content = sys.argv[1] if len(sys.argv) > 1 else "Default message"
        message = {
            "id": int(time.time()),
            "content": message_content,
            "timestamp": datetime.now().isoformat()
        }

        producer.send_message(message)
        print("Message published successfully!")
    except Exception as e:
        print(f"Error publishing message: {str(e)}")
        sys.exit(1)
    finally:
        if 'producer' in locals():
            producer.close()


class MessageReceiver:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        print("\nReceived signal to stop...")
        self.running = False
        sys.exit(0)

    def process_message(self, message):
        print("\nReceived message:")
        print(f"ID: {message.get('id')}")
        print(f"Content: {message.get('content')}")
        print(f"Timestamp: {message.get('timestamp')}")
        print("-" * 50)


def receive_messages():
    try:
        config = OracleConfig.load_config()
        receiver = MessageReceiver()
        subscriber = MessageSubscriber(config)

        print("Starting message receiver...")
        print("Press Ctrl+C to stop")
        subscriber.start_listening(receiver.process_message)
    except KeyboardInterrupt:
        print("\nStopping message receiver...")
    except Exception as e:
        print(f"Error in message receiver: {str(e)}")
        sys.exit(1)
    finally:
        if 'subscriber' in locals():
            subscriber.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "receive":
        receive_messages()
    else:
        publish_message()
