# config.py
import os
import configparser
from dataclasses import dataclass

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
        """Load configuration from INI file"""
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