import logging
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from property_monitor import PropertyMonitor
from config import (
    DATABASE_PATH, MONITORING_INTERVAL_MINUTES, 
    HEALTH_CHECK_INTERVAL_HOURS, MAX_CONSECUTIVE_ERRORS
)

logger = logging.getLogger(__name__)

class PropertyMonitorScheduler:
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.monitor = PropertyMonitor(DATABASE_PATH)
        self.consecutive_errors = 0
        
    def setup_jobs(self):
        """Setup scheduled jobs"""
        
        # Main monitoring job - every 30 minutes
        self.scheduler.add_job(
            func=self.run_monitoring_job,
            trigger=IntervalTrigger(minutes=MONITORING_INTERVAL_MINUTES),
            id='property_monitoring',
            name='Property Monitoring',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300  # 5 minutes grace time
        )
        
        # Status report job - every 6 hours
        self.scheduler.add_job(
            func=self.send_status_report,
            trigger=IntervalTrigger(hours=HEALTH_CHECK_INTERVAL_HOURS),
            id='status_report',
            name='Status Report',
            max_instances=1
        )
        
        # Daily summary job - at 9 AM Dubai time
        self.scheduler.add_job(
            func=self.send_daily_summary,
            trigger=CronTrigger(hour=9, minute=0, timezone='Asia/Dubai'),
            id='daily_summary',
            name='Daily Summary',
            max_instances=1
        )
        
        # Cleanup job - weekly at 3 AM on Sunday
        self.scheduler.add_job(
            func=self.cleanup_old_data,
            trigger=CronTrigger(day_of_week=6, hour=3, minute=0, timezone='Asia/Dubai'),
            id='cleanup',
            name='Database Cleanup',
            max_instances=1
        )
        
        logger.info("Scheduled jobs configured")
    
    def run_monitoring_job(self):
        """Run the property monitoring job"""
        try:
            logger.info("Starting scheduled monitoring job")
            start_time = datetime.now()
            
            # Run monitoring cycle
            stats = self.monitor.run_monitoring_cycle()
            
            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Monitoring job completed in {execution_time:.2f} seconds: {stats}")
            
            # Reset consecutive errors on success
            if stats["found"] > 0:
                self.consecutive_errors = 0
            else:
                self.consecutive_errors += 1
                
            # Check if we need to send error alert
            if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                self.monitor.telegram.send_error_notification(
                    f"No properties found in {self.consecutive_errors} consecutive monitoring runs"
                )
                
        except Exception as e:
            logger.error(f"Error in scheduled monitoring job: {e}")
            self.consecutive_errors += 1
            
            # Send error notification for critical failures
            try:
                self.monitor.telegram.send_error_notification(f"Monitoring job failed: {str(e)}")
            except Exception as notification_error:
                logger.error(f"Failed to send error notification: {notification_error}")
    
    def send_status_report(self):
        """Send periodic status report"""
        try:
            logger.info("Sending status report")
            self.monitor.send_status_report()
            
        except Exception as e:
            logger.error(f"Error sending status report: {e}")
    
    def send_daily_summary(self):
        """Send daily summary of property activities"""
        try:
            logger.info("Sending daily summary")
            
            # Get 24-hour statistics
            stats = self.monitor.db.get_monitoring_stats()
            recent_data = self.monitor.db.get_properties_by_date_range(24)
            
            summary_message = f"""
📊 *DAILY PROPERTY SUMMARY*
📅 Date: {datetime.now().strftime('%Y-%m-%d')}

📈 *24-Hour Activity:*
🆕 New Listings: {len(recent_data['new'])}
💰 Price Changes: {len(recent_data['price_changes'])}
❌ Deletions: {len(recent_data['deleted'])}

📋 *Total Active Properties:* {stats['total_properties']}
⏰ *Last Monitoring Check:* {stats['last_check']}

🤖 *System Status:* {"✅ Healthy" if self.consecutive_errors == 0 else f"⚠️ {self.consecutive_errors} recent errors"}
"""
            
            self.monitor.telegram.send_message(summary_message)
            
        except Exception as e:
            logger.error(f"Error sending daily summary: {e}")
    
    def cleanup_old_data(self):
        """Clean up old monitoring logs and inactive properties"""
        try:
            logger.info("Running database cleanup")
            
            with self.monitor.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Delete monitoring logs older than 30 days
                cursor.execute('''
                    DELETE FROM monitoring_log 
                    WHERE timestamp < datetime('now', '-30 days')
                ''')
                logs_deleted = cursor.rowcount
                
                # Delete price history older than 90 days
                cursor.execute('''
                    DELETE FROM price_history 
                    WHERE change_date < datetime('now', '-90 days')
                ''')
                price_history_deleted = cursor.rowcount
                
                # Keep inactive properties for 30 days then delete
                cursor.execute('''
                    DELETE FROM properties 
                    WHERE is_active = 0 
                    AND date_modified < datetime('now', '-30 days')
                ''')
                inactive_deleted = cursor.rowcount
                
                conn.commit()
                
                cleanup_message = f"""
🧹 *DATABASE CLEANUP COMPLETED*

📊 *Cleaned Up:*
📝 Monitoring logs: {logs_deleted}
💰 Price history: {price_history_deleted}
🗑️ Old inactive properties: {inactive_deleted}

⏰ *Completed:* {datetime.now().strftime('%Y-%m-%d %H:%M')}
"""
                
                self.monitor.telegram.send_message(cleanup_message)
                logger.info(f"Database cleanup completed: {logs_deleted} logs, {price_history_deleted} price history, {inactive_deleted} inactive properties deleted")
                
        except Exception as e:
            logger.error(f"Error during database cleanup: {e}")
    
    def start(self):
        """Start the scheduler"""
        try:
            # Run system test first
            logger.info("Running system test before starting scheduler")
            if not self.monitor.test_system():
                logger.error("System test failed, aborting startup")
                return False
            
            # Setup jobs
            self.setup_jobs()
            
            # Run initial monitoring cycle
            logger.info("Running initial monitoring cycle")
            self.run_monitoring_job()
            
            # Start scheduler
            logger.info("Starting property monitoring scheduler")
            self.scheduler.start()
            
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            self.stop()
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
            return False
            
        return True
    
    def stop(self):
        """Stop the scheduler gracefully"""
        try:
            logger.info("Stopping scheduler")
            if self.scheduler.running:
                self.scheduler.shutdown(wait=True)
            logger.info("Scheduler stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    def run_once(self):
        """Run monitoring once without scheduling"""
        logger.info("Running one-time monitoring check")
        
        try:
            # Run system test
            if self.monitor.test_system():
                # Run monitoring
                stats = self.monitor.run_monitoring_cycle()
                logger.info(f"One-time monitoring completed: {stats}")
                return True
            else:
                logger.error("System test failed")
                return False
                
        except Exception as e:
            logger.error(f"Error in one-time monitoring: {e}")
            return False
