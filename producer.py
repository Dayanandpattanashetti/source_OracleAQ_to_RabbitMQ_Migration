# producer.py
import json
import time
from typing import Dict, Any
import cx_Oracle
from config import OracleConfig

class MessageProducer:
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
    
    def send_message(self, message: Dict[str, Any]):
        """Send a message to the queue"""
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
        """Close the connection"""
        if self.connection:
            self.connection.close()
