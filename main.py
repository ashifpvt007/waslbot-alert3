
import time
from wasl_scraper import scrape_properties
from property_monitor import detect_changes_and_notify

while True:
    try:
        print("⏳ Checking Wasl website...")
        listings = scrape_properties()
        detect_changes_and_notify(listings)
        print("✅ Check complete.")
    except Exception as e:
        print(f"❌ Error occurred: {e}")
    time.sleep(120)  # 2-minute interval
