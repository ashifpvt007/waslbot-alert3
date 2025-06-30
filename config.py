import os
from typing import Dict, Any

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = "7835804086:AAG6ScVVIJoHkH8RFpBEYUtTHInQW893xNc"
TELEGRAM_CHAT_ID = "5868500316"
TELEGRAM_BOT_USERNAME = "@Wasl_alert1_bot"

# API Configuration
UAE_REAL_ESTATE_API_KEY = os.getenv("UAE_REAL_ESTATE_API_KEY", "")
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN", "")

# API Endpoints
UAE_REAL_ESTATE_BASE_URL = "https://uae-real-estate.p.rapidapi.com"
APIFY_BASE_URL = "https://api.apify.com/v2"

# Location Configuration
TARGET_LOCATIONS = [
    "Ras Al Khor Third",
    "Ras Al Khor Industrial Area 3",
    "Ras Al Khor Industrial 3",
    "Ras al Khor Industrial Area 3",
    "Ras Al Khor Ind Third"
]

# Property Types to Monitor
PROPERTY_TYPES = ["apartment", "villa", "townhouse", "commercial", "warehouse", "office"]
LISTING_TYPES = ["rent", "sale"]

# Database Configuration
DATABASE_PATH = "property_listings.db"

# Monitoring Configuration
MONITORING_INTERVAL_MINUTES = 10  # Reduced from 30 to 10 minutes
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

# Rate Limiting
API_RATE_LIMIT_DELAY = 1  # seconds between API calls
MAX_PROPERTIES_PER_REQUEST = 50

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Error Handling
MAX_CONSECUTIVE_ERRORS = 5
ERROR_NOTIFICATION_THRESHOLD = 3

# Message Templates
NEW_LISTING_TEMPLATE = """
🏠 *NEW PROPERTY LISTING*

📍 *Location:* {location}
🏢 *Type:* {property_type}
💰 *Price:* AED {price}
🛏️ *Bedrooms:* {bedrooms}
🚿 *Bathrooms:* {bathrooms}
📐 *Size:* {size} sq ft
🔗 *Link:* {url}

📊 *Details:*
{description}

📈 *Current Market Summary:*
{current_listings}

⏰ *Listed:* {date_added}
"""

PRICE_CHANGE_TEMPLATE = """
💰 *PRICE CHANGE ALERT*

📍 *Property:* {title}
📍 *Location:* {location}

💵 *Old Price:* AED {old_price:,}
💰 *New Price:* AED {new_price:,}
📈 *Change:* {price_change:+,.0f} AED ({percentage_change:+.1f}%)

🔗 *Link:* {url}
"""

DELETED_LISTING_TEMPLATE = """
❌ *PROPERTY DELISTED*

📍 *Property:* {title}
📍 *Location:* {location}
💰 *Last Price:* AED {price:,}
🗓️ *Was Listed:* {date_added}
🗓️ *Removed:* {date_removed}

🔗 *Link:* {url}
"""

# Health Check Configuration
HEALTH_CHECK_INTERVAL_HOURS = 6
STATUS_REPORT_TEMPLATE = """
📊 *WASL PROPERTY MONITOR STATUS*

✅ *Active Monitoring:* Ras Al Khor Third Area
⏰ *Last Check:* {last_check}
📈 *Properties Tracked:* {total_properties}
🆕 *New Listings (24h):* {new_today}
💰 *Price Changes (24h):* {price_changes_today}
❌ *Deletions (24h):* {deletions_today}

📈 *Current Market Summary:*
{current_listings}

🔄 *Next Check:* {next_check}
"""
