"""
Script to fetch Hypixel Skyblock auction house and bazaar data.
Run this to populate/update the databases with current market prices.
"""

import logging
from utils.auctions import HypixelAuctionTracker
from utils.bazzar import HypixelBazaarTracker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    print("=" * 80)
    print("HYPIXEL SKYBLOCK DATA FETCHER")
    print("=" * 80)
    print()
    
    # Fetch bazaar data (fast)
    print("📊 Fetching bazaar data...")
    print("-" * 80)
    try:
        bazaar = HypixelBazaarTracker()
        bazaar.update()
        print("✅ Bazaar data fetched successfully!")
    except Exception as e:
        print(f"❌ Error fetching bazaar data: {e}")
    
    print()
    print("-" * 80)
    
    # Fetch auction data (slow - ~5-10 minutes)
    print("🔨 Fetching auction house data...")
    print("⚠️  This will take 5-10 minutes due to API rate limiting...")
    print("-" * 80)
    try:
        auctions = HypixelAuctionTracker()
        auctions.fetch_all_auctions()
        print("✅ Auction house data fetched successfully!")
    except Exception as e:
        print(f"❌ Error fetching auction data: {e}")
    
    print()
    print("=" * 80)
    print("✅ Data fetch complete! Your databases are now up to date.")
    print("=" * 80)

if __name__ == "__main__":
    main()
