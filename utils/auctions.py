import requests
import sqlite3
import time
import json
from datetime import datetime
from typing import Dict, List, Optional
import logging
from functools import lru_cache

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HypixelAuctionTracker:
    def __init__(self, db_path: str = "db/auctions.db"):
        """Initialize the auction tracker with database connection."""
        self.base_url = "https://api.hypixel.net/v2/skyblock/auctions"
        self.db_path = db_path
        
        # Ensure db directory exists
        import os
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
        
        self.setup_database()
    
    @staticmethod
    @lru_cache(maxsize=1000)
    def get_player_name(uuid: str) -> Optional[str]:
        """
        Get player name from UUID using Mojang API.
        Cached to avoid repeated lookups.
        """
        try:
            # Remove hyphens from UUID if present
            uuid_clean = uuid.replace('-', '')
            response = requests.get(
                f"https://sessionserver.mojang.com/session/minecraft/profile/{uuid_clean}",
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                return data.get('name')
            return None
        except Exception as e:
            logger.debug(f"Could not fetch player name for {uuid}: {e}")
            return None
    
    def setup_database(self):
        """Create the database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main auctions table (current snapshot)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS auctions (
                uuid TEXT PRIMARY KEY,
                auctioneer TEXT,
                profile_id TEXT,
                item_name TEXT,
                tier TEXT,
                category TEXT,
                starting_bid INTEGER,
                highest_bid_amount INTEGER,
                bin INTEGER,
                start_time INTEGER,
                end_time INTEGER,
                last_updated INTEGER,
                claimed INTEGER,
                fetch_timestamp INTEGER
            )
        ''')
        
        # Historical tracking table - stores snapshots of all BIN auctions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS auction_history (
                uuid TEXT,
                item_name TEXT,
                tier TEXT,
                starting_bid INTEGER,
                bin INTEGER,
                claimed INTEGER,
                end_time INTEGER,
                fetch_timestamp INTEGER,
                PRIMARY KEY (uuid, fetch_timestamp)
            )
        ''')
        
        # Sales tracking table - records when auctions disappear (sold)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS auction_sales (
                uuid TEXT PRIMARY KEY,
                item_name TEXT,
                tier TEXT,
                price INTEGER,
                first_seen INTEGER,
                last_seen INTEGER,
                sold_timestamp INTEGER
            )
        ''')
        
        # Index for faster BIN queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_bin_items 
            ON auctions(item_name, bin, highest_bid_amount)
            WHERE bin = 1
        ''')
        
        # Index for historical queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_history_item_time
            ON auction_history(item_name, fetch_timestamp)
        ''')
        
        # Index for sales queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_sales_item_time
            ON auction_sales(item_name, sold_timestamp)
        ''')
        
        # Table to track update cycles
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS update_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER,
                total_pages INTEGER,
                total_auctions INTEGER,
                duration_seconds REAL
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")
    
    def fetch_page(self, page: int = 0) -> Optional[Dict]:
        """Fetch a single page from the API."""
        try:
            response = requests.get(f"{self.base_url}?page={page}", timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page}: {e}")
            return None
    
    def fetch_all_auctions(self, delay_between_pages: float = 2.0):
        """Fetch all auction pages with rate limiting."""
        logger.info("Starting auction fetch cycle...")
        start_time = time.time()
        
        # Get first page to determine total pages
        first_page = self.fetch_page(0)
        if not first_page or not first_page.get('success'):
            logger.error("Failed to fetch first page")
            return
        
        total_pages = first_page.get('totalPages', 0)
        total_auctions = first_page.get('totalAuctions', 0)
        
        logger.info(f"Total pages: {total_pages}, Total auctions: {total_auctions}")
        
        all_auctions = []
        all_auctions.extend(first_page.get('auctions', []))
        
        # Fetch remaining pages
        for page in range(1, total_pages):
            logger.info(f"Fetching page {page}/{total_pages}...")
            
            page_data = self.fetch_page(page)
            if page_data and page_data.get('success'):
                all_auctions.extend(page_data.get('auctions', []))
            else:
                logger.warning(f"Failed to fetch page {page}, skipping...")
            
            # Rate limiting - wait between requests
            if page < total_pages - 1:  # Don't wait after last page
                time.sleep(delay_between_pages)
        
        # Store all auctions in database
        self.store_auctions(all_auctions)
        
        duration = time.time() - start_time
        self.log_update_cycle(total_pages, total_auctions, duration)
        
        logger.info(f"Fetch cycle complete! Processed {len(all_auctions)} auctions in {duration:.2f}s")
    
    def store_auctions(self, auctions: List[Dict]):
        """Store auctions in the database and track sales history."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        fetch_timestamp = int(time.time() * 1000)  # Current time in ms
        
        # Get all UUIDs from previous snapshot
        cursor.execute('SELECT uuid, item_name, tier, starting_bid, fetch_timestamp FROM auctions WHERE bin = 1 AND claimed = 0')
        previous_auctions = {row[0]: (row[1], row[2], row[3], row[4]) for row in cursor.fetchall()}
        
        # Track current UUIDs
        current_uuids = set()
        
        # Prepare data for insertion
        auction_data = []
        history_data = []
        
        for auction in auctions:
            uuid = auction.get('uuid')
            item_name = auction.get('item_name')
            tier = auction.get('tier')
            starting_bid = auction.get('starting_bid')
            is_bin = 1 if auction.get('bin') else 0
            claimed = 1 if auction.get('claimed') else 0
            end_time = auction.get('end')
            
            current_uuids.add(uuid)
            
            auction_data.append((
                uuid,
                auction.get('auctioneer'),
                auction.get('profile_id'),
                item_name,
                tier,
                auction.get('category'),
                starting_bid,
                auction.get('highest_bid_amount'),
                is_bin,
                auction.get('start'),
                end_time,
                auction.get('last_updated'),
                claimed,
                fetch_timestamp
            ))
            
            # Store BIN history for tracking
            if is_bin and not claimed:
                history_data.append((
                    uuid,
                    item_name,
                    tier,
                    starting_bid,
                    is_bin,
                    claimed,
                    end_time,
                    fetch_timestamp
                ))
        
        # Detect sold items (UUIDs that disappeared from previous snapshot)
        sold_uuids = set(previous_auctions.keys()) - current_uuids
        sales_data = []
        
        for uuid in sold_uuids:
            item_name, tier, price, first_seen = previous_auctions[uuid]
            sales_data.append((
                uuid,
                item_name,
                tier,
                price,
                first_seen,
                first_seen,  # last_seen = first_seen for now (will be updated in future cycles)
                fetch_timestamp  # sold_timestamp
            ))
        
        # Clear old snapshot and insert new data
        cursor.execute('DELETE FROM auctions')
        cursor.executemany('''
            INSERT OR REPLACE INTO auctions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', auction_data)
        
        # Insert historical snapshots
        if history_data:
            cursor.executemany('''
                INSERT OR IGNORE INTO auction_history VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', history_data)
        
        # Record sales
        if sales_data:
            cursor.executemany('''
                INSERT OR REPLACE INTO auction_sales VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', sales_data)
            logger.info(f"Detected {len(sales_data)} sold items")
        
        conn.commit()
        conn.close()
        logger.info(f"Stored {len(auction_data)} auctions and {len(history_data)} historical records")
    
    def log_update_cycle(self, total_pages: int, total_auctions: int, duration: float):
        """Log information about the update cycle."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO update_log (timestamp, total_pages, total_auctions, duration_seconds)
            VALUES (?, ?, ?, ?)
        ''', (int(time.time() * 1000), total_pages, total_auctions, duration))
        
        conn.commit()
        conn.close()
    
    def get_lowest_bin(self, item_name: str, include_player_name: bool = False) -> Optional[Dict]:
        """Get the lowest BIN price for a specific item."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Try exact match first
        cursor.execute('''
            SELECT uuid, item_name, starting_bid, tier, auctioneer
            FROM auctions
            WHERE item_name = ? AND bin = 1 AND claimed = 0
            ORDER BY starting_bid ASC
            LIMIT 1
        ''', (item_name,))
        
        result = cursor.fetchone()
        
        # If not found and item has underscores (but NOT ENCHANTMENT_), try with spaces
        if not result and '_' in item_name and not item_name.startswith('ENCHANTMENT_'):
            # Convert underscores to spaces
            item_name_with_spaces = item_name.replace('_', ' ')
            cursor.execute('''
                SELECT uuid, item_name, starting_bid, tier, auctioneer
                FROM auctions
                WHERE item_name = ? AND bin = 1 AND claimed = 0
                ORDER BY starting_bid ASC
                LIMIT 1
            ''', (item_name_with_spaces,))
            result = cursor.fetchone()
        
        # If still not found, try case-insensitive search
        if not result:
            cursor.execute('''
                SELECT uuid, item_name, starting_bid, tier, auctioneer
                FROM auctions
                WHERE LOWER(item_name) = LOWER(?) AND bin = 1 AND claimed = 0
                ORDER BY starting_bid ASC
                LIMIT 1
            ''', (item_name,))
            result = cursor.fetchone()
        
        # Last resort: try case-insensitive with spaces
        if not result and '_' in item_name and not item_name.startswith('ENCHANTMENT_'):
            item_name_with_spaces = item_name.replace('_', ' ')
            cursor.execute('''
                SELECT uuid, item_name, starting_bid, tier, auctioneer
                FROM auctions
                WHERE LOWER(item_name) = LOWER(?) AND bin = 1 AND claimed = 0
                ORDER BY starting_bid ASC
                LIMIT 1
            ''', (item_name_with_spaces,))
            result = cursor.fetchone()
        
        conn.close()
        
        if result:
            item_dict = {
                'uuid': result[0],
                'item_name': result[1],
                'price': result[2],
                'tier': result[3],
                'auctioneer': result[4]
            }
            
            if include_player_name:
                item_dict['player_name'] = self.get_player_name(result[4])
            
            return item_dict
        return None
    
    def get_all_bin_items(self) -> List[Dict]:
        """Get all unique items with their lowest BIN prices."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT item_name, MIN(starting_bid) as lowest_price, tier, COUNT(*) as count
            FROM auctions
            WHERE bin = 1 AND claimed = 0
            GROUP BY item_name
            ORDER BY item_name
        ''')
        
        results = cursor.fetchall()
        conn.close()
        
        return [
            {
                'item_name': row[0],
                'lowest_bin': row[1],
                'tier': row[2],
                'available_count': row[3]
            }
            for row in results
        ]
    
    def search_items(self, search_term: str) -> List[Dict]:
        """Search for items by name (case-insensitive)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT item_name, MIN(starting_bid) as lowest_price, tier, COUNT(*) as count
            FROM auctions
            WHERE bin = 1 AND claimed = 0 AND LOWER(item_name) LIKE LOWER(?)
            GROUP BY item_name
            ORDER BY lowest_price ASC
        ''', (f'%{search_term}%',))
        
        results = cursor.fetchall()
        conn.close()
        
        return [
            {
                'item_name': row[0],
                'lowest_bin': row[1],
                'tier': row[2],
                'available_count': row[3]
            }
            for row in results
        ]
    
    def get_cheapest_listings(self, item_name: str, limit: int = 10, include_player_names: bool = False) -> List[Dict]:
        """
        Get the cheapest BIN listings for a specific item.
        
        Args:
            item_name: Exact item name to search for
            limit: Maximum number of results
            include_player_names: If True, fetch player names (slower due to API calls)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT uuid, item_name, starting_bid, tier, auctioneer, end_time
            FROM auctions
            WHERE item_name = ? AND bin = 1 AND claimed = 0
            ORDER BY starting_bid ASC
            LIMIT ?
        ''', (item_name, limit))
        
        results = cursor.fetchall()
        conn.close()
        
        listings = []
        for row in results:
            listing = {
                'uuid': row[0],
                'item_name': row[1],
                'price': row[2],
                'tier': row[3],
                'auctioneer': row[4],
                'end_time': row[5]
            }
            
            if include_player_names:
                listing['player_name'] = self.get_player_name(row[4])
            
            listings.append(listing)
        
        return listings
    
    def get_sales_per_day(self, item_name: str, days: int = 7) -> Optional[Dict]:
        """
        Calculate average daily sales for an item based on historical data.
        
        Args:
            item_name: Exact item name to search for
            days: Number of days to look back (default 7)
            
        Returns:
            Dictionary with sales statistics or None if insufficient data
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Calculate time range
        now_ms = int(time.time() * 1000)
        days_ago_ms = now_ms - (days * 24 * 60 * 60 * 1000)
        
        # Get sales count in the time period
        cursor.execute('''
            SELECT COUNT(*), AVG(price), MIN(price), MAX(price)
            FROM auction_sales
            WHERE item_name = ? AND sold_timestamp >= ?
        ''', (item_name, days_ago_ms))
        
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0] > 0:
            total_sales = result[0]
            avg_price = result[1]
            min_price = result[2]
            max_price = result[3]
            
            return {
                'item_name': item_name,
                'total_sales': total_sales,
                'daily_sales': total_sales / days,
                'avg_sale_price': avg_price,
                'min_sale_price': min_price,
                'max_sale_price': max_price,
                'period_days': days
            }
        
        return None
    
    def get_item_sales_stats(self, search_term: str, days: int = 7) -> List[Dict]:
        """
        Get sales statistics for all items matching the search term.
        
        Args:
            search_term: Search pattern for item names
            days: Number of days to look back
            
        Returns:
            List of items with their sales statistics
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Calculate time range
        now_ms = int(time.time() * 1000)
        days_ago_ms = now_ms - (days * 24 * 60 * 60 * 1000)
        
        # Get sales grouped by item
        cursor.execute('''
            SELECT item_name, tier, COUNT(*) as sales_count, 
                   AVG(price) as avg_price, MIN(price) as min_price, MAX(price) as max_price
            FROM auction_sales
            WHERE LOWER(item_name) LIKE LOWER(?) AND sold_timestamp >= ?
            GROUP BY item_name, tier
            ORDER BY sales_count DESC
        ''', (f'%{search_term}%', days_ago_ms))
        
        results = cursor.fetchall()
        conn.close()
        
        stats = []
        for row in results:
            stats.append({
                'item_name': row[0],
                'tier': row[1],
                'total_sales': row[2],
                'daily_sales': row[2] / days,
                'avg_sale_price': row[3],
                'min_sale_price': row[4],
                'max_sale_price': row[5],
                'period_days': days
            })
        
        return stats
    
    def run_continuous(self, update_interval: int = 1200):
        """
        Run the tracker continuously, updating every update_interval seconds.
        Default is 1200 seconds (20 minutes).
        """
        logger.info(f"Starting continuous tracking (updates every {update_interval/60:.1f} minutes)")
        
        while True:
            try:
                self.fetch_all_auctions(delay_between_pages=2.0)
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
    tracker = HypixelAuctionTracker()
    
    # Example 1: Fetch all auctions once
    print("\n=== Fetching auction data ===")
    tracker.fetch_all_auctions(delay_between_pages=2.0)
    
    # Example 2: Search for specific items
    print("\n=== Searching for 'Aspect' items ===")
    results = tracker.search_items("Aspect")
    for item in results[:10]:  # Show first 10 results
        print(f"{item['item_name']:<50} | {item['lowest_bin']:>12,} coins | {item['tier']:<10} | {item['available_count']} available")
    
    # Example 3: Get lowest BIN for specific item
    print("\n=== Checking specific item ===")
    item = tracker.get_lowest_bin("Warped Aspect of the Void", include_player_name=True)
    if item:
        print(f"Lowest BIN for {item['item_name']}: {item['price']:,} coins")
        if item.get('player_name'):
            print(f"Seller: {item['player_name']}")
        print(f"Auctioneer UUID: {item['auctioneer']}")
    
    # Example 4: Get cheapest listings with player names
    print("\n=== Top 5 Cheapest Aspect of the End listings ===")
    listings = tracker.get_cheapest_listings("Aspect of the End", limit=5, include_player_names=True)
    for i, listing in enumerate(listings, 1):
        player_info = f" | Seller: {listing['player_name']}" if listing.get('player_name') else ""
        print(f"{i}. {listing['price']:>10,} coins | {listing['tier']:<10}{player_info}")
    
    # Uncomment to run continuously (every 20 minutes)
    # print("\n=== Starting continuous mode ===")
    # tracker.run_continuous(update_interval=1200)


if __name__ == "__main__":
    main()
