# receive_messages.py
import signal
import sys
from config import OracleConfig
from subscriber import MessageSubscriber

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
        """Message processing callback"""
        print("\nReceived message:")
        print(f"ID: {message.get('id')}")
        print(f"Content: {message.get('content')}")
        print(f"Timestamp: {message.get('timestamp')}")
        print("-" * 50)

def main():
    try:
        # Load configuration
        config = OracleConfig.load_config()
        
        # Create receiver instance
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
    main()