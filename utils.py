import logging
import re
import hashlib
from typing import Any, Dict, List, Optional
from datetime import datetime
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def setup_logging(log_level: str = "INFO", log_file: str = None):
    """Setup logging configuration"""
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Reduce noise from external libraries
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('apscheduler').setLevel(logging.INFO)

def extract_price_from_text(price_text: str) -> float:
    """Extract numeric price from text"""
    if not price_text:
        return 0.0
    
    try:
        # Remove common currency symbols and text
        cleaned = re.sub(r'[^\d.,]', '', str(price_text))
        
        # Handle different decimal separators
        if ',' in cleaned and '.' in cleaned:
            # Assume comma is thousands separator if it comes before period
            if cleaned.rindex(',') < cleaned.rindex('.'):
                cleaned = cleaned.replace(',', '')
            else:
                # Assume period is thousands separator
                cleaned = cleaned.replace('.', '').replace(',', '.')
        elif ',' in cleaned:
            # Could be decimal separator (European style) or thousands
            if len(cleaned.split(',')[-1]) <= 2:
                cleaned = cleaned.replace(',', '.')
            else:
                cleaned = cleaned.replace(',', '')
        
        return float(cleaned)
        
    except (ValueError, AttributeError):
        logger.warning(f"Could not extract price from: {price_text}")
        return 0.0

def extract_number_from_text(text: str) -> int:
    """Extract first number from text"""
    if not text:
        return 0
    
    try:
        match = re.search(r'\d+', str(text))
        return int(match.group()) if match else 0
    except (ValueError, AttributeError):
        return 0

def clean_text(text: str, max_length: int = None) -> str:
    """Clean and normalize text"""
    if not text:
        return ""
    
    # Remove extra whitespace and normalize
    cleaned = ' '.join(str(text).split())
    
    # Remove HTML tags if present
    cleaned = re.sub(r'<[^>]+>', '', cleaned)
    
    # Truncate if needed
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length-3] + "..."
    
    return cleaned

def normalize_location(location: str) -> str:
    """Normalize location text for consistent matching"""
    if not location:
        return ""
    
    # Convert to lowercase and clean
    normalized = location.lower().strip()
    
    # Remove common prefixes/suffixes
    prefixes_to_remove = ['dubai', 'uae', 'united arab emirates']
    suffixes_to_remove = ['area', 'district', 'community']
    
    for prefix in prefixes_to_remove:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):].strip()
    
    for suffix in suffixes_to_remove:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()
    
    # Normalize common variations
    replacements = {
        'ras al khor': 'ras al khor',
        'ras alkhor': 'ras al khor',
        'rasalkhor': 'ras al khor',
        'industrial area 3': 'industrial third',
        'industrial 3': 'industrial third',
        'ind 3': 'industrial third',
        'ind third': 'industrial third'
    }
    
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)
    
    return normalized

def is_valid_url(url: str) -> bool:
    """Check if URL is valid"""
    if not url:
        return False
    
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

def generate_property_signature(property_data: Dict[str, Any]) -> str:
    """Generate a unique signature for property deduplication"""
    # Use key identifying fields
    signature_data = {
        'title': clean_text(str(property_data.get('title', ''))).lower(),
        'location': normalize_location(str(property_data.get('location', ''))),
        'price': property_data.get('price', 0),
        'bedrooms': property_data.get('bedrooms', 0),
        'size_sqft': property_data.get('size_sqft', 0)
    }
    
    # Create hash
    signature_string = '|'.join(str(v) for v in signature_data.values())
    return hashlib.md5(signature_string.encode()).hexdigest()

def format_currency(amount: float, currency: str = "AED") -> str:
    """Format currency amount"""
    if amount == 0:
        return "Price on request"
    
    if amount >= 1_000_000:
        return f"{currency} {amount/1_000_000:.1f}M"
    elif amount >= 1_000:
        return f"{currency} {amount/1_000:.0f}K"
    else:
        return f"{currency} {amount:,.0f}"

def calculate_price_change_percentage(old_price: float, new_price: float) -> float:
    """Calculate percentage change in price"""
    if old_price == 0:
        return 0.0
    
    return ((new_price - old_price) / old_price) * 100

def truncate_description(description: str, max_length: int = 200) -> str:
    """Truncate description intelligently"""
    if not description or len(description) <= max_length:
        return description
    
    # Try to cut at sentence boundary
    truncated = description[:max_length]
    last_sentence_end = max(
        truncated.rfind('.'),
        truncated.rfind('!'),
        truncated.rfind('?')
    )
    
    if last_sentence_end > max_length * 0.7:  # If we found a good cut point
        return truncated[:last_sentence_end + 1]
    else:
        return truncated + "..."

def validate_property_data(property_data: Dict[str, Any]) -> List[str]:
    """Validate property data and return list of issues"""
    issues = []
    
    # Required fields
    required_fields = ['external_id', 'title', 'location', 'price', 'source']
    for field in required_fields:
        if not property_data.get(field):
            issues.append(f"Missing required field: {field}")
    
    # Data type validation
    numeric_fields = ['price', 'bedrooms', 'bathrooms', 'size_sqft']
    for field in numeric_fields:
        value = property_data.get(field)
        if value is not None:
            try:
                float(value)
            except (ValueError, TypeError):
                issues.append(f"Invalid numeric value for {field}: {value}")
    
    # URL validation
    url = property_data.get('url')
    if url and not is_valid_url(url):
        issues.append(f"Invalid URL: {url}")
    
    # Price validation
    price = property_data.get('price', 0)
    if price and (price < 0 or price > 100_000_000):  # Reasonable bounds
        issues.append(f"Price outside reasonable range: {price}")
    
    return issues

def get_time_ago(timestamp: str) -> str:
    """Get human-readable time difference"""
    try:
        if isinstance(timestamp, str):
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        else:
            dt = timestamp
        
        now = datetime.now()
        if dt.tzinfo:
            now = now.replace(tzinfo=dt.tzinfo)
        
        diff = now - dt
        
        if diff.days > 0:
            return f"{diff.days} day{'s' if diff.days > 1 else ''} ago"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
        else:
            return "Just now"
    
    except Exception:
        return "Unknown"

def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe file operations"""
    # Remove or replace problematic characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Remove control characters
    sanitized = ''.join(char for char in sanitized if ord(char) >= 32)
    
    # Limit length
    if len(sanitized) > 255:
        sanitized = sanitized[:255]
    
    return sanitized.strip()

class RateLimiter:
    """Simple rate limiter"""
    
    def __init__(self, max_calls: int, time_window: int):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
    
    def can_make_call(self) -> bool:
        """Check if a call can be made within rate limits"""
        now = datetime.now()
        
        # Remove old calls outside time window
        self.calls = [call_time for call_time in self.calls 
                     if (now - call_time).total_seconds() < self.time_window]
        
        # Check if we can make another call
        return len(self.calls) < self.max_calls
    
    def make_call(self):
        """Register a call"""
        if self.can_make_call():
            self.calls.append(datetime.now())
            return True
        return False
    
    def time_until_next_call(self) -> float:
        """Get seconds until next call is allowed"""
        if self.can_make_call():
            return 0.0
        
        if not self.calls:
            return 0.0
        
        oldest_call = min(self.calls)
        time_until_expire = self.time_window - (datetime.now() - oldest_call).total_seconds()
        
        return max(0.0, time_until_expire)

def format_current_listings_summary(listing_counts: Dict[str, Any]) -> str:
    """Format current listings summary for Telegram notifications"""
    if not listing_counts or not listing_counts.get('by_bedrooms'):
        return "No active listings currently available"
    
    lines = []
    total = listing_counts.get('total_active', 0)
    
    if total == 0:
        return "No active listings currently available"
    
    lines.append(f"📋 *Total Active: {total}*")
    
    # Sort bedroom types
    bedroom_data = listing_counts['by_bedrooms']
    sorted_bedrooms = sorted(bedroom_data.keys(), key=lambda x: int(x.replace('BHK', '').replace('Studio', '0')))
    
    for bedroom_type in sorted_bedrooms:
        data = bedroom_data[bedroom_type]
        count = data['total']
        
        # Format like "2BHK: 1 listing" or "3BHK: 3 listings"  
        plural = "listing" if count == 1 else "listings"
        lines.append(f"🏠 *{bedroom_type}:* {count} {plural}")
        
        # Add rent/sale breakdown if both exist
        if data.get('rent', 0) > 0 and data.get('sale', 0) > 0:
            lines.append(f"   └ Rent: {data['rent']}, Sale: {data['sale']}")
        elif data.get('rent', 0) > 0:
            lines.append(f"   └ For Rent")
        elif data.get('sale', 0) > 0:
            lines.append(f"   └ For Sale")
    
    return "\n".join(lines)
