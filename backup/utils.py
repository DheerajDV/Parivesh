import os
import time
import random
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parivesh_scraper.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("parivesh_scraper")

def setup_headers():
    """Setup request headers to mimic a browser."""
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0"
    }

def random_delay(min_seconds=1, max_seconds=3):
    """Add a random delay between requests to avoid rate limiting."""
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)
    return delay

def parse_date(date_str):
    """Parse date string into datetime object."""
    try:
        # Try different date formats
        formats = [
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d %b %Y",
            "%d %B %Y"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        # If all formats fail, log and return None
        logger.warning(f"Could not parse date: {date_str}")
        return None
    except Exception as e:
        logger.error(f"Error parsing date {date_str}: {str(e)}")
        return None

def clean_text(text):
    """Clean and normalize text."""
    if not text:
        return ""
    
    # Remove extra whitespace
    text = " ".join(text.split())
    return text.strip()

def extract_sw_no(text):
    """Extract S/W No. from text."""
    if not text:
        return ""
    
    # Look for patterns like "SW/123/2023" or similar
    import re
    match = re.search(r'[A-Z]+/\d+/\d+', text)
    if match:
        return match.group(0)
    return text.strip()

def save_to_file(content, filename, directory="downloads"):
    """Save content to a file."""
    try:
        # Create directory if it doesn't exist
        os.makedirs(directory, exist_ok=True)
        
        # Create full path
        filepath = os.path.join(directory, filename)
        
        # Write content to file
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return filepath
    except Exception as e:
        logger.error(f"Error saving file {filename}: {str(e)}")
        return None
