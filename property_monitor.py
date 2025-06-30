import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime
from database import PropertyDatabase
from api_client import PropertyAPIManager
from telegram_bot import TelegramNotifier
from config import ERROR_NOTIFICATION_THRESHOLD

logger = logging.getLogger(__name__)

class PropertyMonitor:
    def __init__(self, db_path: str):
        self.db = PropertyDatabase(db_path)
        self.api_manager = PropertyAPIManager()
        self.telegram = TelegramNotifier()
        self.consecutive_errors = 0
    
    def run_monitoring_cycle(self) -> Dict[str, int]:
        """Run a complete monitoring cycle"""
        logger.info("Starting property monitoring cycle")
        
        try:
            # Fetch latest properties from APIs
            current_properties = self.api_manager.fetch_all_properties()
            
            if not current_properties:
                logger.warning("No properties fetched from APIs")
                self.consecutive_errors += 1
                if self.consecutive_errors >= ERROR_NOTIFICATION_THRESHOLD:
                    self.telegram.send_error_notification("No properties found in multiple consecutive runs")
                return {"found": 0, "new": 0, "updated": 0, "deleted": 0}
            
            # Reset error counter on successful fetch
            self.consecutive_errors = 0
            
            # Process properties and detect changes
            stats = self._process_properties(current_properties)
            
            # Check for deleted properties
            deleted_count = self._check_for_deleted_properties(current_properties)
            stats["deleted"] = deleted_count
            
            # Log monitoring run
            self.db.log_monitoring_run(
                source="combined_apis",
                properties_found=len(current_properties),
                new_properties=stats["new"],
                updated_properties=stats["updated"],
                deleted_properties=stats["deleted"]
            )
            
            logger.info(f"Monitoring cycle completed: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")
            self.consecutive_errors += 1
            
            if self.consecutive_errors >= ERROR_NOTIFICATION_THRESHOLD:
                self.telegram.send_error_notification(f"Monitoring cycle failed: {str(e)}")
            
            # Log error in database
            self.db.log_monitoring_run(
                source="combined_apis",
                properties_found=0,
                new_properties=0,
                updated_properties=0,
                deleted_properties=0,
                errors=str(e),
                status="failed"
            )
            
            return {"found": 0, "new": 0, "updated": 0, "deleted": 0}
    
    def _process_properties(self, current_properties: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process current properties and detect new/updated ones"""
        new_count = 0
        updated_count = 0
        
        for property_data in current_properties:
            external_id = property_data.get("external_id")
            if not external_id:
                logger.warning("Property missing external_id, skipping")
                continue
            
            # Check if property exists in database
            existing_property = self.db.get_property_by_external_id(external_id)
            
            if existing_property:
                # Check for changes
                if self._has_property_changed(existing_property, property_data):
                    updated_count += 1
                    self._handle_property_update(existing_property, property_data)
            else:
                # New property
                new_count += 1
                self._handle_new_property(property_data)
        
        return {"found": len(current_properties), "new": new_count, "updated": updated_count}
    
    def _has_property_changed(self, existing: Dict[str, Any], current: Dict[str, Any]) -> bool:
        """Check if property has significant changes"""
        # Generate hashes to compare
        existing_hash = self.db.generate_property_hash(existing)
        current_hash = self.db.generate_property_hash(current)
        
        return existing_hash != current_hash
    
    def _handle_new_property(self, property_data: Dict[str, Any]):
        """Handle new property discovery"""
        try:
            property_id = self.db.insert_property(property_data)
            
            # Send Telegram notification
            success = self.telegram.send_new_listing_notification(property_data)
            if success:
                logger.info(f"Sent new listing notification for property: {property_data.get('title')}")
            else:
                logger.warning(f"Failed to send notification for new property: {property_data.get('title')}")
                
        except Exception as e:
            logger.error(f"Error handling new property: {e}")
    
    def _handle_property_update(self, existing: Dict[str, Any], updated: Dict[str, Any]):
        """Handle property update"""
        try:
            property_id = existing["id"]
            old_price = existing.get("price", 0)
            new_price = updated.get("price", 0)
            
            # Update property in database
            self.db.update_property(property_id, updated)
            
            # Check for price change
            if old_price != new_price and old_price > 0 and new_price > 0:
                # Record price change
                self.db.record_price_change(property_id, old_price, new_price)
                
                # Send notification
                success = self.telegram.send_price_change_notification(updated, old_price, new_price)
                if success:
                    logger.info(f"Sent price change notification for property: {updated.get('title')}")
                else:
                    logger.warning(f"Failed to send price change notification for property: {updated.get('title')}")
                    
        except Exception as e:
            logger.error(f"Error handling property update: {e}")
    
    def _check_for_deleted_properties(self, current_properties: List[Dict[str, Any]]) -> int:
        """Check for properties that have been delisted"""
        try:
            # Get current external IDs
            current_external_ids = {prop.get("external_id") for prop in current_properties if prop.get("external_id")}
            
            # Get all active external IDs from database
            active_external_ids = set(self.db.get_active_external_ids())
            
            # Find deleted properties
            deleted_external_ids = active_external_ids - current_external_ids
            
            deleted_count = 0
            for external_id in deleted_external_ids:
                deleted_property = self.db.get_property_by_external_id(external_id)
                if deleted_property:
                    # Mark as inactive
                    self.db.mark_property_inactive(external_id)
                    deleted_count += 1
                    
                    # Send notification
                    success = self.telegram.send_deleted_listing_notification(deleted_property)
                    if success:
                        logger.info(f"Sent deletion notification for property: {deleted_property.get('title')}")
                    else:
                        logger.warning(f"Failed to send deletion notification for property: {deleted_property.get('title')}")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error checking for deleted properties: {e}")
            return 0
    
    def send_status_report(self):
        """Send monitoring status report"""
        try:
            stats = self.db.get_monitoring_stats()
            success = self.telegram.send_status_report(stats)
            
            if success:
                logger.info("Sent status report successfully")
            else:
                logger.warning("Failed to send status report")
                
        except Exception as e:
            logger.error(f"Error sending status report: {e}")
    
    def test_system(self) -> bool:
        """Test all system components"""
        logger.info("Running system test")
        
        try:
            # Test Telegram connection
            if not self.telegram.test_connection():
                logger.error("Telegram connection test failed")
                return False
            
            # Test API connections
            test_properties = self.api_manager.fetch_all_properties()
            logger.info(f"API test returned {len(test_properties)} properties")
            
            # Test database
            stats = self.db.get_monitoring_stats()
            logger.info(f"Database test successful, {stats['total_properties']} properties tracked")
            
            # Send test notification
            self.telegram.send_startup_notification()
            
            logger.info("System test completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"System test failed: {e}")
            return False
