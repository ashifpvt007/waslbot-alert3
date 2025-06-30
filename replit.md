# WASL Property Monitor Bot

## Overview

The WASL Property Monitor Bot is a Python-based real estate monitoring system specifically designed to track property listings in Dubai's Ras Al Khor Third area. The system continuously monitors multiple APIs for new listings, price changes, and property delistings, then sends real-time notifications via Telegram.

## System Architecture

The application follows a modular architecture with clear separation of concerns:

### Core Components
- **API Layer**: Handles external API integrations for property data fetching
- **Database Layer**: SQLite-based data persistence with property tracking and history
- **Notification Layer**: Telegram bot integration for real-time alerts
- **Scheduler**: APScheduler-based job management for automated monitoring
- **Monitor Engine**: Central coordination of monitoring cycles and change detection

### Technology Stack
- **Language**: Python 3.x
- **Database**: SQLite (local file-based storage)
- **Scheduling**: APScheduler (Advanced Python Scheduler)
- **HTTP Client**: Requests library with retry logic
- **Messaging**: Telegram Bot API
- **Logging**: Python's built-in logging module

## Key Components

### 1. PropertyMonitor (`property_monitor.py`)
- **Purpose**: Central monitoring engine that orchestrates the complete monitoring cycle
- **Responsibilities**: 
  - Fetches data from APIs
  - Detects new listings, price changes, and deletions
  - Triggers notifications
  - Tracks consecutive errors for health monitoring

### 2. PropertyDatabase (`database.py`)
- **Purpose**: Data persistence and property tracking
- **Features**:
  - Property deduplication using hash-based uniqueness
  - Price history tracking
  - Monitoring run logging
  - Property lifecycle management (active/inactive status)

### 3. APIClient (`api_client.py`)
- **Purpose**: External API integration with retry logic
- **Features**:
  - Rate limiting compliance
  - Exponential backoff retry mechanism
  - Support for UAE Real Estate API and Apify
  - Request timeout and error handling

### 4. TelegramNotifier (`telegram_bot.py`)
- **Purpose**: Real-time notification delivery
- **Features**:
  - Message formatting and templating
  - Long message splitting for Telegram limits
  - Error handling and retry logic
  - Support for Markdown formatting

### 5. PropertyMonitorScheduler (`scheduler.py`)
- **Purpose**: Automated job scheduling and execution
- **Jobs**:
  - Property monitoring (every 30 minutes)
  - Status reports (every 6 hours)
  - Daily summaries (9 AM Dubai time)

## Data Flow

1. **Scheduled Trigger**: APScheduler triggers monitoring job at configured intervals
2. **API Fetching**: APIClient retrieves current property listings from external APIs
3. **Data Processing**: PropertyMonitor processes fetched data and detects changes
4. **Database Operations**: New/updated properties are stored in SQLite database
5. **Change Detection**: System identifies new listings, price changes, and deletions
6. **Notification**: TelegramNotifier sends alerts for detected changes
7. **Logging**: All activities are logged for monitoring and debugging

## External Dependencies

### APIs
- **UAE Real Estate API** (Zyla API Hub): Primary source for property listings
- **Apify**: Secondary data source for web scraping capabilities
- **Telegram Bot API**: Notification delivery system

### Configuration Requirements
- API keys for external services
- Telegram bot token and chat ID
- Target location specifications (Ras Al Khor Third variants)

## Deployment Strategy

The application is designed for simple deployment with minimal infrastructure requirements:

### Local Deployment
- Single Python process with SQLite database
- No external database server required
- Configuration via environment variables and config.py

### Key Features for Production
- **Error Handling**: Comprehensive error tracking with notification thresholds
- **Rate Limiting**: Built-in API rate limiting to prevent service blocking
- **Health Monitoring**: Consecutive error tracking and status reporting
- **Graceful Shutdown**: Signal handling for clean application termination
- **Logging**: Structured logging with configurable levels

### Monitoring Capabilities
- Real-time error notifications
- Periodic status reports
- Daily activity summaries
- Database-backed monitoring run history

## Changelog

- June 30, 2025. Initial setup
- June 30, 2025. Added current listings summary to Telegram notifications, reduced monitoring interval from 30 to 10 minutes

## Recent Changes

### Current Listings Feature (June 30, 2025)
- Added `get_current_listing_counts()` method to database to track property counts by bedroom type
- Added `format_current_listings_summary()` utility function for formatting listing counts
- Updated Telegram notification templates to include current market summary
- Enhanced new listing and status report notifications with live count data

### Monitoring Interval Reduction (June 30, 2025)
- Reduced monitoring interval from 30 minutes to 10 minutes for more frequent checks
- Updated startup notifications to reflect new timing
- Improved responsiveness to new listings and price changes

## User Preferences

Preferred communication style: Simple, everyday language.
Features requested: Show current listing counts (e.g., "1 no of 2BHK & 3 nos of 3BHK") in Telegram notifications, reduce monitoring intervals.