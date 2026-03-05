import requests
import os
import logging
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Your Bot configuration
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

def upload_to_telegram(video_path: str, title: str, description: str) -> Optional[str]:
    """
    Uploads a video to Telegram using the requests library.
    """
    # Load configuration from environment variables for security
    bot_token = BOT_TOKEN
    chat_id = CHAT_ID

    

    url = f"https://api.telegram.org/bot{bot_token}/sendVideo"
    
    if not os.path.exists(video_path):
        logger.error(f"File not found: {video_path}")
        return

    # Combine title and description for Telegram's caption
    # We use HTML parsing so you can use <b> or <i> tags
    full_caption = f"<b>{title}</b>\n\n{description}"

    logger.info(f"Uploading {video_path} to Telegram...")

    with open(video_path, 'rb') as video_file:
        payload = {
            'chat_id': chat_id,
            'caption': full_caption,
            'parse_mode': 'HTML',
            'supports_streaming': True 
        }
        files = {
            'video': video_file
        }

        try:
            response = requests.post(url, data=payload, files=files, timeout=60)
            response.raise_for_status()  # Check for HTTP errors
            
            result = response.json()
            if result.get("ok"):
                logger.info("Upload Complete!")
                logger.info(f"Message ID: {result['result']['message_id']}")
                return result['result']['message_id']
            else:
                logger.error(f"Telegram Error: {result.get('description')}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error occurred: {e}")
            return None

# Example usage
if __name__ == "__main__":
    upload_to_telegram(
        video_path="final_video.mp4",
        title="Project Update",
        description="This video was sent via the Telegram Bot API."
    )