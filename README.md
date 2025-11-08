# Dungeons Profit Calculator

Flask API for calculating Hypixel Skyblock dungeon chest profitability.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the API server
python main.py
```

Server runs at `http://localhost:5000`

## API Usage

### Calculate Chest Value
```bash
POST /api/calculate
Content-Type: application/json

{
  "items": [
    {"name": "Necron's Handle", "quantity": 1}
  ],
  "chest_cost": 500000
}
```

### Get Single Item Value
```bash
POST /api/item
Content-Type: application/json

{
  "name": "Necron's Handle",
  "quantity": 1
}
```

## Project Structure

```
├── main.py              # Flask web API server
├── api/
│   └── chest_calculator.py  # Core calculation logic
├── utils/
│   ├── auctions.py      # Auction house data scraper
│   └── bazzar.py        # Bazaar data scraper
└── db/
    ├── auctions.db      # Auction house database
    └── bazaar.db        # Bazaar database
```
