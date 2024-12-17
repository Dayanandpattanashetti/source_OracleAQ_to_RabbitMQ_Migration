# publish_message.py
import sys
import time
from datetime import datetime
from config import OracleConfig
from producer import MessageProducer

def main():
    try:
        # Load configuration
        config = OracleConfig.load_config()
        
        # Create producer instance
        producer = MessageProducer(config)
        
        # Get message content from command line or use default
        message_content = sys.argv[1] if len(sys.argv) > 1 else "Default message"
        
        # Create message
        message = {
            "id": int(time.time()),
            "content": message_content,
            "timestamp": datetime.now().isoformat()
        }
        
        # Send message
        producer.send_message(message)
        print("Message published successfully!")
        
    except Exception as e:
        print(f"Error publishing message: {str(e)}")
        sys.exit(1)
    finally:
        if 'producer' in locals():
            producer.close()

if __name__ == "__main__":
    main()
