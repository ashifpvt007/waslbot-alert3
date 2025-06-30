#!/usr/bin/env python3
"""
WASL Property Monitor Bot for Dubai Ras Al Khor Third Area

This bot monitors property listings in the Ras Al Khor Third area using multiple APIs
and sends notifications via Telegram when new properties are listed, prices change,
or properties are delisted.

Author: Property Monitor Bot
Version: 1.0.0
"""

import sys
import argparse
import signal
import logging
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from scheduler import PropertyMonitorScheduler
from property_monitor import PropertyMonitor
from utils import setup_logging
from config import DATABASE_PATH, LOG_LEVEL

logger = logging.getLogger(__name__)

class PropertyMonitorApp:
    def __init__(self):
        self.scheduler = None
        self.is_running = False
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down gracefully...")
            self.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def start_monitoring(self):
        """Start the property monitoring service"""
        try:
            logger.info("Starting WASL Property Monitor for Ras Al Khor Third Area")
            
            # Setup signal handlers
            self.setup_signal_handlers()
            
            # Initialize scheduler
            self.scheduler = PropertyMonitorScheduler()
            self.is_running = True
            
            # Start monitoring
            success = self.scheduler.start()
            
            if not success:
                logger.error("Failed to start monitoring service")
                return False
            
            return True
            
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
            self.stop()
            return True
        except Exception as e:
            logger.error(f"Error starting monitoring service: {e}")
            return False
    
    def run_once(self):
        """Run monitoring once without scheduling"""
        try:
            logger.info("Running one-time property monitoring check")
            
            scheduler = PropertyMonitorScheduler()
            success = scheduler.run_once()
            
            if success:
                logger.info("One-time monitoring completed successfully")
            else:
                logger.error("One-time monitoring failed")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in one-time monitoring: {e}")
            return False
    
    def test_system(self):
        """Test all system components"""
        try:
            logger.info("Running system test")
            
            monitor = PropertyMonitor(DATABASE_PATH)
            success = monitor.test_system()
            
            if success:
                logger.info("System test completed successfully")
                print("✅ System test PASSED")
                print("- Telegram bot connection: OK")
                print("- API connections: OK")
                print("- Database: OK")
            else:
                logger.error("System test failed")
                print("❌ System test FAILED")
                print("Check logs for details")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in system test: {e}")
            print(f"❌ System test ERROR: {e}")
            return False
    
    def show_status(self):
        """Show current monitoring status"""
        try:
            logger.info("Checking monitoring status")
            
            monitor = PropertyMonitor(DATABASE_PATH)
            stats = monitor.db.get_monitoring_stats()
            
            print("📊 WASL Property Monitor Status")
            print("=" * 40)
            print(f"📍 Target Area: Ras Al Khor Third Area")
            print(f"📋 Total Properties Tracked: {stats['total_properties']}")
            print(f"⏰ Last Check: {stats['last_check']}")
            print(f"🆕 New Listings (24h): {stats['new_today']}")
            print(f"💰 Price Changes (24h): {stats['price_changes_today']}")
            print(f"❌ Deletions (24h): {stats['deletions_today']}")
            print(f"🤖 Bot: @Wasl_alert1_bot")
            print("=" * 40)
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking status: {e}")
            print(f"❌ Error checking status: {e}")
            return False
    
    def send_test_notification(self):
        """Send a test notification to Telegram"""
        try:
            logger.info("Sending test notification")
            
            monitor = PropertyMonitor(DATABASE_PATH)
            message = f"""
🧪 *TEST NOTIFICATION*

✅ WASL Property Monitor is working correctly!

📍 *Monitoring:* Ras Al Khor Third Area
⏰ *Test Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
🤖 *Bot:* @Wasl_alert1_bot

This is a test message to verify the notification system.
"""
            
            success = monitor.telegram.send_message(message)
            
            if success:
                logger.info("Test notification sent successfully")
                print("✅ Test notification sent successfully")
            else:
                logger.error("Failed to send test notification")
                print("❌ Failed to send test notification")
                
            return success
            
        except Exception as e:
            logger.error(f"Error sending test notification: {e}")
            print(f"❌ Error sending test notification: {e}")
            return False
    
    def stop(self):
        """Stop the monitoring service"""
        if self.scheduler and self.is_running:
            logger.info("Stopping property monitoring service")
            self.scheduler.stop()
            self.is_running = False
            logger.info("Property monitoring service stopped")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="WASL Property Monitor Bot for Dubai Ras Al Khor Third Area",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py start                    # Start continuous monitoring
  python main.py run-once                 # Run monitoring once
  python main.py test                     # Test system components
  python main.py status                   # Show current status
  python main.py test-notification        # Send test notification
  
Environment Variables:
  UAE_REAL_ESTATE_API_KEY                # UAE Real Estate API key (Zyla)
  APIFY_API_TOKEN                        # Apify API token for scraping
  
Bot Configuration:
  Telegram Bot: @Wasl_alert1_bot
  Chat ID: 5868500316
  Target Area: Ras Al Khor Third Area
        """
    )
    
    parser.add_argument(
        'command',
        choices=['start', 'run-once', 'test', 'status', 'test-notification'],
        help='Command to execute'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default=LOG_LEVEL,
        help='Set logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        help='Log to file instead of console'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    
    # Create app instance
    app = PropertyMonitorApp()
    
    # Execute command
    try:
        if args.command == 'start':
            print("🚀 Starting WASL Property Monitor...")
            print("📍 Monitoring: Ras Al Khor Third Area")
            print("🤖 Telegram Bot: @Wasl_alert1_bot")
            print("⏰ Check interval: 10 minutes")
            print("Press Ctrl+C to stop")
            print("-" * 50)
            
            success = app.start_monitoring()
            sys.exit(0 if success else 1)
            
        elif args.command == 'run-once':
            print("🔄 Running one-time monitoring check...")
            success = app.run_once()
            sys.exit(0 if success else 1)
            
        elif args.command == 'test':
            print("🧪 Testing system components...")
            success = app.test_system()
            sys.exit(0 if success else 1)
            
        elif args.command == 'status':
            success = app.show_status()
            sys.exit(0 if success else 1)
            
        elif args.command == 'test-notification':
            print("📱 Sending test notification...")
            success = app.send_test_notification()
            sys.exit(0 if success else 1)
            
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        app.stop()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"❌ Application error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Fix for potential datetime import issue
    from datetime import datetime
    main()
