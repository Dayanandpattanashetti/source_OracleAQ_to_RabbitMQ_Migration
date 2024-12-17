# subscriber.py
import json
import time
from typing import Callable
import cx_Oracle
from config import OracleConfig

class MessageSubscriber:
    def __init__(self, config: OracleConfig):
        self.config = config
        self.connection = None
        self.queue = None
        
    def connect(self):
        """Establish connection to Oracle and initialize the queue"""
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
                
            except cx_Oracle.Error as e:
                if attempt == self.config.max_retries - 1:
                    raise
                print(f"Connection attempt {attempt + 1} failed. Retrying in {self.config.retry_delay} seconds...")
                time.sleep(self.config.retry_delay)
        
    def start_listening(self, callback: Callable[[dict], None]):
        """Start listening for messages with the specified callback"""
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
        """Close the connection"""
        if self.connection:
            self.connection.close()

