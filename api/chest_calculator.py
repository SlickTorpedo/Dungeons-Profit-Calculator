"""
Chest Value Calculator API

Accepts a JSON object with chest items and returns their market values.
"""

import sys
import os
import json
from typing import List, Dict, Optional, Union

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.auctions import HypixelAuctionTracker
from utils.bazzar import HypixelBazaarTracker


class ChestValueCalculator:
    """
    API for calculating chest values based on Hypixel Skyblock market data.
    
    Example usage:
        calculator = ChestValueCalculator()
        
        items = [
            {"name": "Aspect of the End", "quantity": 1},
            {"name": "Enchanted Diamond", "quantity": 32}
        ]
        
        result = calculator.calculate_chest_value(items)
        print(json.dumps(result, indent=2))
    """
    
    def __init__(self, auction_db: str = "db/auctions.db", bazaar_db: str = "db/bazaar.db"):
        """Initialize the calculator with database connections."""
        self.auction_tracker = HypixelAuctionTracker(auction_db)
        self.bazaar_tracker = HypixelBazaarTracker(bazaar_db)
    
    def get_last_update_times(self) -> Dict:
        """Get the last update timestamps for both auction house and bazaar."""
        import sqlite3
        from datetime import datetime
        
        # Get auction house last update
        conn = sqlite3.connect(self.auction_tracker.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(timestamp) FROM update_log')
        result = cursor.fetchone()
        conn.close()
        
        ah_timestamp = result[0] if result and result[0] else None
        ah_datetime = datetime.fromtimestamp(ah_timestamp / 1000).isoformat() if ah_timestamp else None
        
        # Get bazaar last update
        conn = sqlite3.connect(self.bazaar_tracker.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(timestamp) FROM update_log')
        result = cursor.fetchone()
        conn.close()
        
        bz_timestamp = result[0] if result and result[0] else None
        bz_datetime = datetime.fromtimestamp(bz_timestamp / 1000).isoformat() if bz_timestamp else None
        
        return {
            "auction_house": {
                "last_update_timestamp": ah_timestamp,
                "last_update_datetime": ah_datetime
            },
            "bazaar": {
                "last_update_timestamp": bz_timestamp,
                "last_update_datetime": bz_datetime
            }
        }
    
    def get_item_value(self, item_name: str, quantity: int = 1) -> Dict:
        """
        Get the market value for a single item.
        
        Args:
            item_name: Name of the item
            quantity: Number of items (default 1)
            
        Returns:
            Dictionary with item value information
        """
        result = {
            "item_name": item_name,
            "quantity": quantity,
            "found_in": [],
            "auction_house": None,
            "bazaar": None,
            "best_price": None,
            "total_value": 0,
            "market": None,
            "sales_per_day": -1  # -1 indicates insufficient data
        }
        
        # Check Auction House
        auction_item = self.auction_tracker.get_lowest_bin(item_name)
        if auction_item:
            result["found_in"].append("auction_house")
            result["auction_house"] = {
                "lowest_bin": auction_item["price"],
                "tier": auction_item.get("tier"),
                "total": auction_item["price"] * quantity
            }
            
            # Get sales velocity from auction house
            sales_data = self.auction_tracker.get_sales_per_day(item_name)
            if sales_data and sales_data.get('daily_sales'):
                result["sales_per_day"] = round(sales_data['daily_sales'], 2)
        
        # Check Bazaar
        bazaar_item = self.bazaar_tracker.get_product_info(item_name)
        if bazaar_item:
            result["found_in"].append("bazaar")
            # Use buy_price (what you'd get when selling to buy orders instantly)
            sell_price = bazaar_item["buy_price"]
            result["bazaar"] = {
                "insta_sell_price": sell_price,
                "insta_buy_price": bazaar_item["sell_price"],
                "total": sell_price * quantity
            }
            
            # For bazaar items, use moving week as sales indicator if available
            if result["sales_per_day"] == -1 and bazaar_item.get("buy_moving_week"):
                result["sales_per_day"] = round(bazaar_item["buy_moving_week"] / 7, 2)
        
        # Determine best price (highest value when selling)
        if result["auction_house"] and result["bazaar"]:
            ah_price = result["auction_house"]["lowest_bin"]
            bz_price = result["bazaar"]["insta_sell_price"]
            
            # Use auction house price as estimate of what you could sell for
            # Compare to bazaar insta-sell
            if ah_price >= bz_price:
                result["best_price"] = ah_price
                result["total_value"] = ah_price * quantity
                result["market"] = "auction_house"
            else:
                result["best_price"] = bz_price
                result["total_value"] = bz_price * quantity
                result["market"] = "bazaar"
        elif result["auction_house"]:
            result["best_price"] = result["auction_house"]["lowest_bin"]
            result["total_value"] = result["best_price"] * quantity
            result["market"] = "auction_house"
        elif result["bazaar"]:
            result["best_price"] = result["bazaar"]["insta_sell_price"]
            result["total_value"] = result["best_price"] * quantity
            result["market"] = "bazaar"
        
        return result
    
    def calculate_chest_value(self, items: List[Dict], chest_cost: Optional[float] = None) -> Dict:
        """
        Calculate total value of items in a chest.
        
        Args:
            items: List of dicts with 'name' and 'quantity' keys
                   Example: [{"name": "Aspect of the End", "quantity": 1}]
            chest_cost: Optional cost of opening the chest
            
        Returns:
            Dictionary with full valuation breakdown
        """
        # Get last update times
        last_updates = self.get_last_update_times()
        
        results = {
            "items": [],
            "summary": {
                "total_items": len(items),
                "items_found": 0,
                "items_not_found": 0,
                "total_value": 0,
                "chest_cost": chest_cost,
                "profit": None if chest_cost is None else 0
            },
            "last_updated": last_updates
        }
        
        for item in items:
            item_name = item.get("name", "")
            quantity = item.get("quantity", 1)
            
            if not item_name:
                continue
            
            item_value = self.get_item_value(item_name, quantity)
            results["items"].append(item_value)
            
            if item_value["best_price"] is not None:
                results["summary"]["items_found"] += 1
                results["summary"]["total_value"] += item_value["total_value"]
            else:
                results["summary"]["items_not_found"] += 1
        
        # Calculate profit if chest cost provided
        if chest_cost is not None:
            results["summary"]["profit"] = results["summary"]["total_value"] - chest_cost
            results["summary"]["is_profitable"] = results["summary"]["profit"] > 0
            results["summary"]["roi_percent"] = (results["summary"]["profit"] / chest_cost * 100) if chest_cost > 0 else 0
        
        return results
    
    def calculate_from_json(self, json_data: Union[str, Dict]) -> Dict:
        """
        Calculate chest value from JSON string or dict.
        
        Args:
            json_data: JSON string or dict with format:
                {
                    "items": [
                        {"name": "Item Name", "quantity": 1},
                        ...
                    ],
                    "chest_cost": 500000  // optional
                }
                
        Returns:
            Dictionary with valuation results
        """
        if isinstance(json_data, str):
            data = json.loads(json_data)
        else:
            data = json_data
        
        items = data.get("items", [])
        chest_cost = data.get("chest_cost")
        
        return self.calculate_chest_value(items, chest_cost)


def main():
    """Example usage of the API."""
    calculator = ChestValueCalculator()
    
    # Example 1: Simple item list
    print("=" * 80)
    print("EXAMPLE 1: Calculate value of specific items")
    print("=" * 80)
    
    items = [
        {"name": "Aspect of the End", "quantity": 1},
        {"name": "ENCHANTMENT_ULTIMATE_WISE_5", "quantity": 1},
        {"name": "Enchanted Diamond", "quantity": 32}
    ]
    
    result = calculator.calculate_chest_value(items)
    print(json.dumps(result, indent=2))
    
    # Example 2: With chest cost
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Calculate profit from dungeon chest")
    print("=" * 80)
    
    chest_data = {
        "items": [
            {"name": "Necron's Handle", "quantity": 1},
            {"name": "Wither Catalyst", "quantity": 3},
            {"name": "Premium Flesh", "quantity": 64}
        ],
        "chest_cost": 500000
    }
    
    result = calculator.calculate_from_json(chest_data)
    print(json.dumps(result, indent=2))
    
    if result["summary"]["profit"] is not None:
        if result["summary"]["is_profitable"]:
            print(f"\n✅ PROFITABLE! Profit: {result['summary']['profit']:,.0f} coins ({result['summary']['roi_percent']:.1f}% ROI)")
        else:
            print(f"\n❌ NOT PROFITABLE. Loss: {abs(result['summary']['profit']):,.0f} coins")
    
    # Example 3: From JSON string
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Calculate from JSON string")
    print("=" * 80)
    
    json_string = '''
    {
        "items": [
            {"name": "Spirit Wing", "quantity": 1},
            {"name": "Spirit Bone", "quantity": 2}
        ],
        "chest_cost": 100000
    }
    '''
    
    result = calculator.calculate_from_json(json_string)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
