import requests
import time
import logging
from typing import List, Dict, Any, Optional
from config import (
    UAE_REAL_ESTATE_API_KEY, UAE_REAL_ESTATE_BASE_URL, 
    APIFY_API_TOKEN, APIFY_BASE_URL,
    TARGET_LOCATIONS, PROPERTY_TYPES, LISTING_TYPES,
    API_RATE_LIMIT_DELAY, REQUEST_TIMEOUT, MAX_RETRIES
)

logger = logging.getLogger(__name__)

class APIClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.timeout = REQUEST_TIMEOUT
    
    def _make_request_with_retry(self, url: str, headers: Dict[str, str], 
                                params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Make HTTP request with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(API_RATE_LIMIT_DELAY)  # Rate limiting
                
                response = self.session.get(url, headers=headers, params=params)
                response.raise_for_status()
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"All retry attempts failed for URL: {url}")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None

class UAERealeStateAPIClient(APIClient):
    """Client for UAE Real Estate API (Zyla API Hub)"""
    
    def __init__(self):
        super().__init__()
        self.base_url = UAE_REAL_ESTATE_BASE_URL
        self.headers = {
            "x-rapidapi-key": UAE_REAL_ESTATE_API_KEY,
            "x-rapidapi-host": "uae-real-estate.p.rapidapi.com"
        }
    
    def get_location_suggestions(self, query: str) -> List[Dict[str, Any]]:
        """Get location autocomplete suggestions"""
        if not UAE_REAL_ESTATE_API_KEY:
            logger.warning("UAE Real Estate API key not provided")
            return []
        
        try:
            url = f"{self.base_url}/auto-complete"
            params = {"query": query}
            
            response = self._make_request_with_retry(url, self.headers, params)
            
            if response and "hits" in response:
                return response["hits"]
            
            logger.warning(f"No location suggestions found for query: {query}")
            return []
            
        except Exception as e:
            logger.error(f"Error getting location suggestions: {e}")
            return []
    
    def get_properties(self, location_external_id: str = None, 
                      property_type: str = None, listing_type: str = "rent",
                      min_price: float = None, max_price: float = None) -> List[Dict[str, Any]]:
        """Get property listings"""
        if not UAE_REAL_ESTATE_API_KEY:
            logger.warning("UAE Real Estate API key not provided")
            return []
        
        try:
            url = f"{self.base_url}/properties/list"
            params = {
                "locationExternalIDs": location_external_id,
                "purpose": listing_type,
                "hitsPerPage": 50
            }
            
            if property_type:
                params["categoryExternalID"] = property_type
            if min_price:
                params["priceMin"] = min_price
            if max_price:
                params["priceMax"] = max_price
            
            response = self._make_request_with_retry(url, self.headers, params)
            
            if response and "hits" in response:
                properties = []
                for hit in response["hits"]:
                    property_data = self._normalize_uae_property(hit)
                    if self._is_target_location(property_data.get("location", "")):
                        properties.append(property_data)
                
                logger.info(f"Found {len(properties)} properties in target locations from UAE API")
                return properties
            
            return []
            
        except Exception as e:
            logger.error(f"Error fetching properties from UAE API: {e}")
            return []
    
    def _normalize_uae_property(self, raw_property: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize UAE API property data to standard format"""
        try:
            # Extract location information
            location = ""
            if "geography" in raw_property:
                location_parts = []
                for level in ["level1", "level2", "level3", "level4"]:
                    if level in raw_property["geography"] and raw_property["geography"][level]:
                        location_parts.append(raw_property["geography"][level])
                location = ", ".join(location_parts)
            
            # Extract property details
            rooms = raw_property.get("rooms", 0)
            baths = raw_property.get("baths", 0)
            
            # Parse area
            area = 0
            if "area" in raw_property and raw_property["area"]:
                try:
                    area = float(raw_property["area"])
                except (ValueError, TypeError):
                    area = 0
            
            # Extract price
            price = 0
            if "price" in raw_property and raw_property["price"]:
                try:
                    price = float(raw_property["price"])
                except (ValueError, TypeError):
                    price = 0
            
            return {
                "external_id": str(raw_property.get("externalID", "")),
                "title": raw_property.get("title", ""),
                "location": location,
                "property_type": raw_property.get("category", [{}])[0].get("name", "").lower() if raw_property.get("category") else "",
                "listing_type": raw_property.get("purpose", "").lower(),
                "price": price,
                "bedrooms": rooms,
                "bathrooms": baths,
                "size_sqft": area,
                "description": raw_property.get("description", ""),
                "url": f"https://www.bayut.com/property/details-{raw_property.get('externalID', '')}",
                "source": "uae_api",
                "raw_data": raw_property
            }
            
        except Exception as e:
            logger.error(f"Error normalizing UAE API property: {e}")
            return {}
    
    def _is_target_location(self, location: str) -> bool:
        """Check if location matches target area"""
        location_lower = location.lower()
        for target in TARGET_LOCATIONS:
            if target.lower() in location_lower:
                return True
        return False

class ApifyClient(APIClient):
    """Client for Apify scrapers"""
    
    def __init__(self):
        super().__init__()
        self.base_url = APIFY_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {APIFY_API_TOKEN}",
            "Content-Type": "application/json"
        }
    
    def run_propertyfinder_scraper(self, location: str = "ras-al-khor") -> List[Dict[str, Any]]:
        """Run PropertyFinder scraper via Apify"""
        if not APIFY_API_TOKEN:
            logger.warning("Apify API token not provided")
            return []
        
        try:
            # Start scraper run
            run_input = {
                "location": location,
                "propertyType": "all",
                "purpose": "all",
                "maxItems": 100
            }
            
            url = f"{self.base_url}/acts/dhrumil~propertyfinder-scraper/runs"
            response = self._make_request_with_retry(url, self.headers, run_input)
            
            if not response or "data" not in response:
                logger.error("Failed to start PropertyFinder scraper")
                return []
            
            run_id = response["data"]["id"]
            logger.info(f"Started PropertyFinder scraper run: {run_id}")
            
            # Wait for completion and get results
            return self._wait_for_scraper_results(run_id)
            
        except Exception as e:
            logger.error(f"Error running PropertyFinder scraper: {e}")
            return []
    
    def _wait_for_scraper_results(self, run_id: str, max_wait_minutes: int = 10) -> List[Dict[str, Any]]:
        """Wait for scraper to complete and return results"""
        max_checks = max_wait_minutes * 6  # Check every 10 seconds
        
        for check in range(max_checks):
            try:
                # Check run status
                status_url = f"{self.base_url}/acts/dhrumil~propertyfinder-scraper/runs/{run_id}"
                status_response = self._make_request_with_retry(status_url, self.headers)
                
                if not status_response or "data" not in status_response:
                    continue
                
                status = status_response["data"]["status"]
                
                if status == "SUCCEEDED":
                    # Get results
                    results_url = f"{self.base_url}/acts/dhrumil~propertyfinder-scraper/runs/{run_id}/dataset/items"
                    results_response = self._make_request_with_retry(results_url, self.headers)
                    
                    if results_response:
                        properties = []
                        for item in results_response:
                            property_data = self._normalize_apify_property(item)
                            if self._is_target_location(property_data.get("location", "")):
                                properties.append(property_data)
                        
                        logger.info(f"PropertyFinder scraper completed: {len(properties)} target properties found")
                        return properties
                
                elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
                    logger.error(f"PropertyFinder scraper failed with status: {status}")
                    return []
                
                # Wait before next check
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"Error checking scraper status: {e}")
                time.sleep(10)
        
        logger.warning("PropertyFinder scraper timed out")
        return []
    
    def _normalize_apify_property(self, raw_property: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Apify scraper property data to standard format"""
        try:
            # Extract price
            price = 0
            price_str = raw_property.get("price", "")
            if price_str:
                # Remove currency symbols and commas, extract numbers
                import re
                price_match = re.search(r'[\d,]+', str(price_str).replace(',', ''))
                if price_match:
                    try:
                        price = float(price_match.group().replace(',', ''))
                    except ValueError:
                        price = 0
            
            return {
                "external_id": str(raw_property.get("id", raw_property.get("propertyId", ""))),
                "title": raw_property.get("title", raw_property.get("name", "")),
                "location": raw_property.get("location", raw_property.get("address", "")),
                "property_type": raw_property.get("propertyType", "").lower(),
                "listing_type": raw_property.get("purpose", "rent").lower(),
                "price": price,
                "bedrooms": raw_property.get("bedrooms", raw_property.get("rooms", 0)),
                "bathrooms": raw_property.get("bathrooms", raw_property.get("baths", 0)),
                "size_sqft": raw_property.get("area", raw_property.get("size", 0)),
                "description": raw_property.get("description", ""),
                "url": raw_property.get("url", raw_property.get("link", "")),
                "source": "apify_propertyfinder",
                "raw_data": raw_property
            }
            
        except Exception as e:
            logger.error(f"Error normalizing Apify property: {e}")
            return {}
    
    def _is_target_location(self, location: str) -> bool:
        """Check if location matches target area"""
        location_lower = location.lower()
        for target in TARGET_LOCATIONS:
            if target.lower() in location_lower:
                return True
        return False

class PropertyAPIManager:
    """Manager class that coordinates multiple API clients"""
    
    def __init__(self):
        self.uae_client = UAERealeStateAPIClient()
        self.apify_client = ApifyClient()
        # Import and initialize Wasl scraper
        from wasl_scraper import WaslPropertyScraper
        self.wasl_scraper = WaslPropertyScraper()
    
    def fetch_all_properties(self) -> List[Dict[str, Any]]:
        """Fetch properties from all available sources"""
        all_properties = []
        
        # Try Al Wasl scraper first (primary source)
        try:
            logger.info("Fetching properties from Al Wasl website")
            wasl_properties = self.wasl_scraper.fetch_properties()
            all_properties.extend(wasl_properties)
            logger.info(f"Al Wasl scraper returned {len(wasl_properties)} properties")
            
        except Exception as e:
            logger.error(f"Error fetching from Al Wasl: {e}")
        
        # Try UAE Real Estate API as backup (if no API key warnings, skip this)
        if UAE_REAL_ESTATE_API_KEY:
            try:
                logger.info("Fetching properties from UAE Real Estate API")
                
                # Get location suggestions for target areas
                for location in TARGET_LOCATIONS[:2]:  # Limit to avoid rate limits
                    suggestions = self.uae_client.get_location_suggestions(location)
                    
                    for suggestion in suggestions[:3]:  # Limit suggestions per location
                        if "externalID" in suggestion:
                            # Fetch properties for each listing type
                            for listing_type in LISTING_TYPES:
                                properties = self.uae_client.get_properties(
                                    location_external_id=suggestion["externalID"],
                                    listing_type=listing_type
                                )
                                all_properties.extend(properties)
                                
                                # Rate limiting
                                time.sleep(API_RATE_LIMIT_DELAY)
                
                logger.info(f"UAE API returned additional properties, total now: {len(all_properties)}")
                
            except Exception as e:
                logger.error(f"Error fetching from UAE API: {e}")
        
        # Try Apify PropertyFinder scraper as backup (only if API token available)
        if APIFY_API_TOKEN:
            try:
                logger.info("Fetching properties from Apify PropertyFinder scraper")
                apify_properties = self.apify_client.run_propertyfinder_scraper()
                all_properties.extend(apify_properties)
                
                logger.info(f"Apify scraper returned {len(apify_properties)} properties")
                
            except Exception as e:
                logger.error(f"Error fetching from Apify: {e}")
        
        # Remove duplicates based on external_id
        unique_properties = {}
        for prop in all_properties:
            external_id = prop.get("external_id")
            if external_id and external_id not in unique_properties:
                unique_properties[external_id] = prop
        
        final_properties = list(unique_properties.values())
        logger.info(f"Total unique properties found: {len(final_properties)}")
        
        return final_properties
