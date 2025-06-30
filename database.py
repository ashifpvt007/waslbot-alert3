import sqlite3
import hashlib
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class PropertyDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database with required tables"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Properties table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS properties (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    external_id TEXT UNIQUE NOT NULL,
                    property_hash TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    location TEXT NOT NULL,
                    property_type TEXT NOT NULL,
                    listing_type TEXT NOT NULL,
                    price REAL NOT NULL,
                    bedrooms INTEGER,
                    bathrooms INTEGER,
                    size_sqft REAL,
                    description TEXT,
                    url TEXT,
                    source TEXT NOT NULL,
                    raw_data TEXT,
                    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    date_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1
                )
            ''')
            
            # Price history table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    property_id INTEGER NOT NULL,
                    old_price REAL NOT NULL,
                    new_price REAL NOT NULL,
                    change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (property_id) REFERENCES properties (id)
                )
            ''')
            
            # Monitoring log table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS monitoring_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source TEXT NOT NULL,
                    properties_found INTEGER NOT NULL,
                    new_properties INTEGER NOT NULL,
                    updated_properties INTEGER NOT NULL,
                    deleted_properties INTEGER NOT NULL,
                    errors TEXT,
                    status TEXT NOT NULL
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_external_id ON properties(external_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_location ON properties(location)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_active ON properties(is_active)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date_added ON properties(date_added)')
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    def generate_property_hash(self, property_data: Dict[str, Any]) -> str:
        """Generate a hash for property to detect changes"""
        # Include key fields that indicate property changes
        hash_data = {
            'external_id': property_data.get('external_id', ''),
            'title': property_data.get('title', ''),
            'price': property_data.get('price', 0),
            'location': property_data.get('location', ''),
            'bedrooms': property_data.get('bedrooms', 0),
            'bathrooms': property_data.get('bathrooms', 0),
            'size_sqft': property_data.get('size_sqft', 0)
        }
        
        hash_string = json.dumps(hash_data, sort_keys=True)
        return hashlib.sha256(hash_string.encode()).hexdigest()
    
    def insert_property(self, property_data: Dict[str, Any]) -> int:
        """Insert a new property into the database"""
        property_hash = self.generate_property_hash(property_data)
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO properties (
                    external_id, property_hash, title, location, property_type,
                    listing_type, price, bedrooms, bathrooms, size_sqft,
                    description, url, source, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                property_data.get('external_id'),
                property_hash,
                property_data.get('title'),
                property_data.get('location'),
                property_data.get('property_type'),
                property_data.get('listing_type'),
                property_data.get('price'),
                property_data.get('bedrooms'),
                property_data.get('bathrooms'),
                property_data.get('size_sqft'),
                property_data.get('description'),
                property_data.get('url'),
                property_data.get('source'),
                json.dumps(property_data.get('raw_data', {}))
            ))
            
            conn.commit()
            property_id = cursor.lastrowid
            logger.info(f"Inserted new property: {property_data.get('title')} (ID: {property_id})")
            return property_id
    
    def update_property(self, property_id: int, property_data: Dict[str, Any]) -> bool:
        """Update an existing property"""
        property_hash = self.generate_property_hash(property_data)
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE properties SET
                    property_hash = ?, title = ?, location = ?, property_type = ?,
                    listing_type = ?, price = ?, bedrooms = ?, bathrooms = ?,
                    size_sqft = ?, description = ?, url = ?, raw_data = ?,
                    date_modified = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                property_hash,
                property_data.get('title'),
                property_data.get('location'),
                property_data.get('property_type'),
                property_data.get('listing_type'),
                property_data.get('price'),
                property_data.get('bedrooms'),
                property_data.get('bathrooms'),
                property_data.get('size_sqft'),
                property_data.get('description'),
                property_data.get('url'),
                json.dumps(property_data.get('raw_data', {})),
                property_id
            ))
            
            conn.commit()
            logger.info(f"Updated property ID: {property_id}")
            return cursor.rowcount > 0
    
    def record_price_change(self, property_id: int, old_price: float, new_price: float):
        """Record a price change in the price history table"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO price_history (property_id, old_price, new_price)
                VALUES (?, ?, ?)
            ''', (property_id, old_price, new_price))
            
            conn.commit()
            logger.info(f"Recorded price change for property {property_id}: {old_price} -> {new_price}")
    
    def mark_property_inactive(self, external_id: str) -> bool:
        """Mark a property as inactive (delisted)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE properties SET
                    is_active = 0,
                    date_modified = CURRENT_TIMESTAMP
                WHERE external_id = ?
            ''', (external_id,))
            
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"Marked property as inactive: {external_id}")
                return True
            return False
    
    def get_property_by_external_id(self, external_id: str) -> Optional[Dict[str, Any]]:
        """Get property by external ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM properties WHERE external_id = ? AND is_active = 1
            ''', (external_id,))
            
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
    
    def get_active_external_ids(self) -> List[str]:
        """Get all active property external IDs"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('SELECT external_id FROM properties WHERE is_active = 1')
            return [row[0] for row in cursor.fetchall()]
    
    def get_properties_by_date_range(self, hours: int) -> Dict[str, List[Dict[str, Any]]]:
        """Get properties added, updated, or deleted in the last N hours"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # New properties
            cursor.execute('''
                SELECT * FROM properties 
                WHERE date_added >= datetime('now', '-{} hours')
                AND is_active = 1
            '''.format(hours))
            new_properties = [dict(row) for row in cursor.fetchall()]
            
            # Price changes
            cursor.execute('''
                SELECT p.*, ph.old_price, ph.new_price, ph.change_date
                FROM properties p
                JOIN price_history ph ON p.id = ph.property_id
                WHERE ph.change_date >= datetime('now', '-{} hours')
                AND p.is_active = 1
            '''.format(hours))
            price_changes = [dict(row) for row in cursor.fetchall()]
            
            # Deleted properties (marked inactive)
            cursor.execute('''
                SELECT * FROM properties 
                WHERE date_modified >= datetime('now', '-{} hours')
                AND is_active = 0
            '''.format(hours))
            deleted_properties = [dict(row) for row in cursor.fetchall()]
            
            return {
                'new': new_properties,
                'price_changes': price_changes,
                'deleted': deleted_properties
            }
    
    def log_monitoring_run(self, source: str, properties_found: int, 
                          new_properties: int, updated_properties: int, 
                          deleted_properties: int, errors: str = None, 
                          status: str = "success"):
        """Log a monitoring run"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO monitoring_log (
                    source, properties_found, new_properties, 
                    updated_properties, deleted_properties, errors, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (source, properties_found, new_properties, 
                  updated_properties, deleted_properties, errors, status))
            
            conn.commit()
    
    def get_current_listing_counts(self) -> Dict[str, Any]:
        """Get current listing counts by bedroom type"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get counts by bedroom type
            cursor.execute('''
                SELECT 
                    bedrooms,
                    COUNT(*) as count,
                    property_type,
                    listing_type
                FROM properties 
                WHERE is_active = 1 
                GROUP BY bedrooms, property_type, listing_type
                ORDER BY bedrooms, property_type
            ''')
            
            results = cursor.fetchall()
            
            # Organize data
            bedroom_counts = {}
            total_count = 0
            
            for row in results:
                bedrooms = row[0] or 0
                count = row[1]
                prop_type = row[2] or 'Unknown'
                listing_type = row[3] or 'rent'
                
                total_count += count
                
                key = f"{bedrooms}BHK" if bedrooms > 0 else "Studio"
                if key not in bedroom_counts:
                    bedroom_counts[key] = {'total': 0, 'rent': 0, 'sale': 0, 'types': {}}
                
                bedroom_counts[key]['total'] += count
                bedroom_counts[key][listing_type] += count
                
                if prop_type not in bedroom_counts[key]['types']:
                    bedroom_counts[key]['types'][prop_type] = 0
                bedroom_counts[key]['types'][prop_type] += count
            
            return {
                'total_active': total_count,
                'by_bedrooms': bedroom_counts,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

    def get_monitoring_stats(self) -> Dict[str, Any]:
        """Get monitoring statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Total active properties
            cursor.execute('SELECT COUNT(*) FROM properties WHERE is_active = 1')
            total_properties = cursor.fetchone()[0]
            
            # Last monitoring run
            cursor.execute('''
                SELECT timestamp FROM monitoring_log 
                ORDER BY timestamp DESC LIMIT 1
            ''')
            last_check_row = cursor.fetchone()
            last_check = last_check_row[0] if last_check_row else "Never"
            
            # Recent activity (24 hours)
            recent_data = self.get_properties_by_date_range(24)
            
            return {
                'total_properties': total_properties,
                'last_check': last_check,
                'new_today': len(recent_data['new']),
                'price_changes_today': len(recent_data['price_changes']),
                'deletions_today': len(recent_data['deleted'])
            }
