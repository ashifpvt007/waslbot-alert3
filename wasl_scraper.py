
#!/usr/bin/env python3
"""
Real-time Al Wasl Property Scraper using Playwright
"""

import asyncio
from playwright.sync_api import sync_playwright
from datetime import datetime

def scrape_properties():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.goto("https://www.wasl.ae/en/search/residential?location=ras-al-khor-ind-third", timeout=60000)
        page.wait_for_timeout(5000)  # wait for JS to load content

        listings = []
        cards = page.query_selector_all(".project-box")

        for card in cards:
            try:
                title = card.query_selector(".title").inner_text().strip()
                price_text = card.query_selector(".price").inner_text().strip()
                url = card.query_selector("a").get_attribute("href")

                listings.append({
                    "title": title,
                    "price": float(''.join(filter(str.isdigit, price_text))),
                    "url": "https://www.wasl.ae" + url,
                    "source": "wasl.ae",
                    "date_scraped": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
            except Exception:
                continue

        browser.close()
        return listings

if __name__ == "__main__":
    props = scrape_properties()
    for p in props:
        print(p)
