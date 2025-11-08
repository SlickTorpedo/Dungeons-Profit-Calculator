"""
Flask Web API for Chest Value Calculator

Provides REST API endpoints for calculating chest values.

Endpoints:
    POST /api/calculate - Calculate chest value
    POST /api/item - Get single item value
    GET /api/health - Health check

Example usage:
    curl -X POST http://localhost:5000/api/calculate \
         -H "Content-Type: application/json" \
         -d '{"items":[{"name":"Necron'\''s Handle","quantity":1}],"chest_cost":500000}'
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import sys
import os
import threading
import time
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.chest_calculator import ChestValueCalculator
from utils.auctions import HypixelAuctionTracker
from utils.bazzar import HypixelBazaarTracker

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Initialize calculator
calculator = ChestValueCalculator()

# Initialize trackers for background updates
auction_tracker = HypixelAuctionTracker()
bazaar_tracker = HypixelBazaarTracker()


def fetch_data_background():
    """Background thread to fetch auction and bazaar data."""
    logger.info("🔄 Starting background data fetcher...")
    
    # Initial fetch
    try:
        logger.info("📊 Fetching initial bazaar data...")
        bazaar_tracker.update()
        logger.info("✅ Bazaar data fetched")
    except Exception as e:
        logger.error(f"❌ Error fetching bazaar data: {e}")
    
    try:
        logger.info("🔨 Fetching initial auction house data (this will take 5-10 minutes)...")
        auction_tracker.fetch_all_auctions()
        logger.info("✅ Auction house data fetched")
    except Exception as e:
        logger.error(f"❌ Error fetching auction data: {e}")
    
    # Continuous updates every 20 minutes
    while True:
        try:
            time.sleep(1200)  # 20 minutes
            logger.info("🔄 Updating market data...")
            
            # Update bazaar (fast)
            bazaar_tracker.update()
            logger.info("✅ Bazaar updated")
            
            # Update auctions (slow)
            auction_tracker.fetch_all_auctions()
            logger.info("✅ Auction house updated")
            
        except Exception as e:
            logger.error(f"❌ Error in background update: {e}")
            time.sleep(60)  # Wait 1 minute before retrying


def start_background_fetcher():
    """Start the background data fetcher thread."""
    fetcher_thread = threading.Thread(target=fetch_data_background, daemon=True)
    fetcher_thread.start()


@app.route('/')
def index():
    """API documentation page."""
    return jsonify({
        "name": "Hypixel Skyblock Chest Calculator API",
        "version": "1.0.0",
        "endpoints": {
            "/api/calculate": {
                "method": "POST",
                "description": "Calculate total chest value",
                "body": {
                    "items": [{"name": "string", "quantity": "number"}],
                    "chest_cost": "number (optional)"
                }
            },
            "/api/item": {
                "method": "POST",
                "description": "Get value for a single item",
                "body": {
                    "name": "string",
                    "quantity": "number (optional, default 1)"
                }
            },
            "/api/health": {
                "method": "GET",
                "description": "Health check endpoint"
            }
        },
        "example": {
            "curl": "curl -X POST http://localhost:5000/api/calculate -H 'Content-Type: application/json' -d '{\"items\":[{\"name\":\"Necron's Handle\",\"quantity\":1}],\"chest_cost\":500000}'"
        }
    })


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "service": "Chest Calculator API",
        "version": "1.0.0"
    })


@app.route('/api/calculate', methods=['POST'])
def calculate_chest():
    """
    Calculate chest value from JSON data.
    
    Request body:
    {
        "items": [
            {"name": "Item Name", "quantity": 1},
            ...
        ],
        "chest_cost": 500000  // optional
    }
    
    Returns:
    {
        "items": [...],
        "summary": {
            "total_value": number,
            "profit": number,
            "is_profitable": boolean,
            ...
        }
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "error": "No JSON data provided",
                "status": "error"
            }), 400
        
        if 'items' not in data:
            return jsonify({
                "error": "Missing 'items' field in request",
                "status": "error"
            }), 400
        
        if not isinstance(data['items'], list):
            return jsonify({
                "error": "'items' must be an array",
                "status": "error"
            }), 400
        
        # Calculate chest value
        result = calculator.calculate_from_json(data)
        result['status'] = 'success'
        
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500


@app.route('/api/item', methods=['POST'])
def get_item_value():
    """
    Get value for a single item.
    
    Request body:
    {
        "name": "Item Name",
        "quantity": 1  // optional, default 1
    }
    
    Returns:
    {
        "item_name": string,
        "quantity": number,
        "best_price": number,
        "total_value": number,
        "market": string,
        ...
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "error": "No JSON data provided",
                "status": "error"
            }), 400
        
        if 'name' not in data:
            return jsonify({
                "error": "Missing 'name' field in request",
                "status": "error"
            }), 400
        
        item_name = data['name']
        quantity = data.get('quantity', 1)
        
        # Get item value
        result = calculator.get_item_value(item_name, quantity)
        result['status'] = 'success'
        
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500


@app.route('/api/batch', methods=['POST'])
def batch_items():
    """
    Get values for multiple items (without chest cost calculation).
    
    Request body:
    {
        "items": ["Item Name 1", "Item Name 2", ...]
    }
    
    Returns:
    {
        "results": [
            {"item_name": string, "best_price": number, ...},
            ...
        ]
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'items' not in data:
            return jsonify({
                "error": "Missing 'items' field in request",
                "status": "error"
            }), 400
        
        if not isinstance(data['items'], list):
            return jsonify({
                "error": "'items' must be an array",
                "status": "error"
            }), 400
        
        results = []
        for item_name in data['items']:
            item_value = calculator.get_item_value(item_name, 1)
            results.append(item_value)
        
        return jsonify({
            "results": results,
            "total_items": len(results),
            "status": "success"
        }), 200
        
    except Exception as e:
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500


@app.errorhandler(404)
def not_found(e):
    """Handle 404 errors."""
    return jsonify({
        "error": "Endpoint not found",
        "status": "error",
        "available_endpoints": ["/api/calculate", "/api/item", "/api/batch", "/api/health"]
    }), 404


@app.errorhandler(500)
def internal_error(e):
    """Handle 500 errors."""
    return jsonify({
        "error": "Internal server error",
        "status": "error"
    }), 500


def main():
    """Run the Flask API server."""
    print("\n" + "=" * 80)
    print(" " * 20 + "CHEST CALCULATOR WEB API")
    print("=" * 80)
    print("\n🚀 Starting Flask server...")
    print("\nAPI Endpoints:")
    print("  POST http://localhost:5000/api/calculate  - Calculate chest value")
    print("  POST http://localhost:5000/api/item       - Get single item value")
    print("  POST http://localhost:5000/api/batch      - Get multiple item values")
    print("  GET  http://localhost:5000/api/health     - Health check")
    print("  GET  http://localhost:5000/               - API documentation")
    print("\n" + "=" * 80)
    print("\n💡 Example request:")
    print('  curl -X POST http://localhost:5000/api/calculate \\')
    print('       -H "Content-Type: application/json" \\')
    print('       -d \'{"items":[{"name":"Necron\'\'s Handle","quantity":1}],"chest_cost":500000}\'')
    print("\n" + "=" * 80)
    print("\n🔄 Background data fetcher: ENABLED")
    print("   - Initial fetch will start after server is running")
    print("   - Updates every 20 minutes automatically")
    print("\n" + "=" * 80)
    print("\nPress Ctrl+C to stop the server\n")
    
    # Start background data fetcher
    start_background_fetcher()
    
    # Run the server
    app.run(host='0.0.0.0', port=5000, debug=False)


if __name__ == '__main__':
    main()
