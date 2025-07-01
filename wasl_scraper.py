
#!/usr/bin/env python3
"""
Real-time Al Wasl Property Scraper using Playwright
"""

import asyncio
from playwright.sync_api import sync_playwright

import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
]

def get_random_user_agent():
    return random.choice(USER_AGENTS)

from datetime import datetime

def scrape_properties():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_extra_http_headers({"User-Agent": get_random_user_agent()})
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
