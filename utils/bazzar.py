import requests
import sqlite3
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HypixelBazaarTracker:
    def __init__(self, db_path: str = "db/bazaar.db"):
        """Initialize the bazaar tracker with database connection."""
        self.base_url = "https://api.hypixel.net/v2/skyblock/bazaar"
        self.db_path = db_path
        
        # Ensure db directory exists
        import os
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
        
        self.setup_database()
    
    def setup_database(self):
        """Create the database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main bazaar products table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bazaar_products (
                product_id TEXT,
                sell_price REAL,
                sell_volume INTEGER,
                sell_moving_week INTEGER,
                sell_orders INTEGER,
                buy_price REAL,
                buy_volume INTEGER,
                buy_moving_week INTEGER,
                buy_orders INTEGER,
                timestamp INTEGER,
                PRIMARY KEY (product_id, timestamp)
            )
        ''')
        
        # Current snapshot table (latest data only)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bazaar_current (
                product_id TEXT PRIMARY KEY,
                sell_price REAL,
                sell_volume INTEGER,
                sell_moving_week INTEGER,
                sell_orders INTEGER,
                buy_price REAL,
                buy_volume INTEGER,
                buy_moving_week INTEGER,
                buy_orders INTEGER,
                timestamp INTEGER
            )
        ''')
        
        # Indexes for faster queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_product_timestamp 
            ON bazaar_products(product_id, timestamp DESC)
        ''')
        
        # Table to track update cycles
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS update_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER,
                total_products INTEGER,
                duration_seconds REAL
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")
    
    def fetch_bazaar_data(self) -> Optional[Dict]:
        """Fetch bazaar data from the API."""
        try:
            logger.info("Fetching bazaar data...")
            response = requests.get(self.base_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('success'):
                logger.info(f"Successfully fetched data for {len(data.get('products', {}))} products")
                return data
            else:
                logger.error("API returned success=false")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching bazaar data: {e}")
            return None
    
    def store_bazaar_data(self, data: Dict, store_history: bool = True):
        """
        Store bazaar data in the database.
        
        Args:
            data: The bazaar data from the API
            store_history: If True, also store in history table for tracking over time
        """
        if not data or not data.get('success'):
            logger.error("Invalid data provided")
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        timestamp = data.get('lastUpdated', int(time.time() * 1000))
        products = data.get('products', {})
        
        # Clear current snapshot
        cursor.execute('DELETE FROM bazaar_current')
        
        current_data = []
        history_data = []
        
        for product_id, product_info in products.items():
            quick_status = product_info.get('quick_status', {})
            
            sell_price = quick_status.get('sellPrice', 0)  # Insta-buy price (lowest sell order)
            buy_price = quick_status.get('buyPrice', 0)    # Insta-sell price (highest buy order)
            
            current_data.append((
                product_id,
                sell_price,
                quick_status.get('sellVolume', 0),
                quick_status.get('sellMovingWeek', 0),
                quick_status.get('sellOrders', 0),
                buy_price,
                quick_status.get('buyVolume', 0),
                quick_status.get('buyMovingWeek', 0),
                quick_status.get('buyOrders', 0),
                timestamp
            ))
            
            if store_history:
                history_data.append((
                    product_id,
                    sell_price,
                    quick_status.get('sellVolume', 0),
                    quick_status.get('sellMovingWeek', 0),
                    quick_status.get('sellOrders', 0),
                    buy_price,
                    quick_status.get('buyVolume', 0),
                    quick_status.get('buyMovingWeek', 0),
                    quick_status.get('buyOrders', 0),
                    timestamp
                ))
        
        # Insert current snapshot
        cursor.executemany('''
            INSERT INTO bazaar_current VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', current_data)
        
        # Insert history if enabled
        if store_history and history_data:
            cursor.executemany('''
                INSERT INTO bazaar_products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', history_data)
        
        conn.commit()
        conn.close()
        
        logger.info(f"Stored {len(current_data)} products in database")
    
    def log_update_cycle(self, total_products: int, duration: float):
        """Log information about the update cycle."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO update_log (timestamp, total_products, duration_seconds)
            VALUES (?, ?, ?)
        ''', (int(time.time() * 1000), total_products, duration))
        
        conn.commit()
        conn.close()
    
    def get_product_info(self, product_id: str) -> Optional[Dict]:
        """Get current information for a specific product."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Try exact match first
        cursor.execute('''
            SELECT product_id, sell_price, buy_price, sell_volume, buy_volume,
                   sell_orders, buy_orders, sell_moving_week, buy_moving_week
            FROM bazaar_current
            WHERE product_id = ?
        ''', (product_id,))
        
        result = cursor.fetchone()
        
        # If not found and item has underscores (but NOT ENCHANTMENT_), try with spaces
        if not result and '_' in product_id and not product_id.startswith('ENCHANTMENT_'):
            product_id_with_spaces = product_id.replace('_', ' ')
            cursor.execute('''
                SELECT product_id, sell_price, buy_price, sell_volume, buy_volume,
                       sell_orders, buy_orders, sell_moving_week, buy_moving_week
                FROM bazaar_current
                WHERE product_id = ?
            ''', (product_id_with_spaces,))
            result = cursor.fetchone()
        
        # If still not found, try case-insensitive search
        if not result:
            cursor.execute('''
                SELECT product_id, sell_price, buy_price, sell_volume, buy_volume,
                       sell_orders, buy_orders, sell_moving_week, buy_moving_week
                FROM bazaar_current
                WHERE LOWER(product_id) = LOWER(?)
            ''', (product_id,))
            result = cursor.fetchone()
        
        # Last resort: try case-insensitive with spaces
        if not result and '_' in product_id and not product_id.startswith('ENCHANTMENT_'):
            product_id_with_spaces = product_id.replace('_', ' ')
            cursor.execute('''
                SELECT product_id, sell_price, buy_price, sell_volume, buy_volume,
                       sell_orders, buy_orders, sell_moving_week, buy_moving_week
                FROM bazaar_current
                WHERE LOWER(product_id) = LOWER(?)
            ''', (product_id_with_spaces,))
            result = cursor.fetchone()
        
        conn.close()
        
        if result:
            return {
                'product_id': result[0],
                'sell_price': result[1],  # Insta-buy price
                'buy_price': result[2],   # Insta-sell price
                'sell_volume': result[3],
                'buy_volume': result[4],
                'sell_orders': result[5],
                'buy_orders': result[6],
                'sell_moving_week': result[7],
                'buy_moving_week': result[8]
            }
        return None
    
    def search_products(self, search_term: str) -> List[Dict]:
        """Search for products by name (case-insensitive)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT product_id, sell_price, buy_price
            FROM bazaar_current
            WHERE LOWER(product_id) LIKE LOWER(?)
            ORDER BY product_id
        ''', (f'%{search_term}%',))
        
        results = cursor.fetchall()
        conn.close()
        
        return [
            {
                'product_id': row[0],
                'sell_price': row[1],
                'buy_price': row[2]
            }
            for row in results
        ]
    
    def get_all_products(self) -> List[str]:
        """Get a list of all product IDs."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT product_id FROM bazaar_current ORDER BY product_id')
        results = cursor.fetchall()
        conn.close()
        
        return [row[0] for row in results]
    
    def get_price_history(self, product_id: str, hours: int = 24) -> List[Tuple[int, float, float]]:
        """
        Get price history for a product.
        
        Args:
            product_id: The product ID
            hours: Number of hours of history to retrieve
            
        Returns:
            List of (timestamp, sell_price, buy_price) tuples
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cutoff_time = int((time.time() - hours * 3600) * 1000)
        
        cursor.execute('''
            SELECT timestamp, sell_price, buy_price
            FROM bazaar_products
            WHERE product_id = ? AND timestamp > ?
            ORDER BY timestamp ASC
        ''', (product_id, cutoff_time))
        
        results = cursor.fetchall()
        conn.close()
        
        return results
    
    def update(self, store_history: bool = True):
        """Fetch and store the latest bazaar data."""
        start_time = time.time()
        
        data = self.fetch_bazaar_data()
        if data:
            self.store_bazaar_data(data, store_history=store_history)
            duration = time.time() - start_time
            self.log_update_cycle(len(data.get('products', {})), duration)
            logger.info(f"Update complete in {duration:.2f}s")
            return True
        return False
    
    def run_continuous(self, update_interval: int = 1200, store_history: bool = True):
        """
        Run the tracker continuously, updating every update_interval seconds.
        
        Args:
            update_interval: Seconds between updates (default: 1200 = 20 minutes)
            store_history: Whether to store historical data (can grow large over time)
        """
        logger.info(f"Starting continuous tracking (updates every {update_interval/60:.1f} minutes)")
        logger.info(f"History tracking: {'enabled' if store_history else 'disabled'}")
        
        while True:
            try:
                self.update(store_history=store_history)
                logger.info(f"Waiting {update_interval/60:.1f} minutes until next update...")
                time.sleep(update_interval)
            except KeyboardInterrupt:
                logger.info("Stopping tracker...")
                break
            except Exception as e:
                logger.error(f"Error in continuous run: {e}")
                logger.info("Waiting 60 seconds before retry...")
                time.sleep(60)


def main():
    """Main function with example usage."""
    tracker = HypixelBazaarTracker()
    
    # Fetch data once
    print("\n=== Fetching bazaar data ===")
    tracker.update(store_history=True)
    
    # Search for specific items
    print("\n=== Searching for 'ENCHANTMENT' products (first 10) ===")
    enchants = tracker.search_products("ENCHANTMENT")
    for item in enchants[:10]:
        print(f"{item['product_id']:<50} | Buy: {item['sell_price']:>10,.2f} | Sell: {item['buy_price']:>10,.2f}")
    
    # Get specific product info
    print("\n=== Checking specific product: TARANTULA_WEB ===")
    product = tracker.get_product_info("TARANTULA_WEB")
    if product:
        print(f"Product: {product['product_id']}")
        print(f"  Insta-BUY price:  {product['sell_price']:>10,.2f} coins")
        print(f"  Insta-SELL price: {product['buy_price']:>10,.2f} coins")
        print(f"  Sell volume:      {product['sell_volume']:>10,}")
        print(f"  Buy volume:       {product['buy_volume']:>10,}")
    
    # Uncomment to run continuously (every 20 minutes)
    # print("\n=== Starting continuous mode ===")
    # tracker.run_continuous(update_interval=1200, store_history=True)


if __name__ == "__main__":
    main()
