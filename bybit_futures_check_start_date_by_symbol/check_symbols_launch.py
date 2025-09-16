#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit Futures Symbols Launch Date Checker
==========================================

This script retrieves launch dates and status information for all USDT perpetual
futures contracts on Bybit exchange and saves them to a JSON file.

Usage:
    python check_symbols_launch.py
    
Output:
    symbols_launch_dates.json - Contains all symbols with their launch dates
"""

import json
import sys
import os
from datetime import datetime
from typing import Dict, List, Any
from pybit.unified_trading import HTTP
from tqdm import tqdm
import time

# Add parent directory to path for imports if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class SymbolsLaunchChecker:
    """
    Retrieves and processes launch date information for Bybit futures symbols.
    """
    
    def __init__(self, testnet: bool = False):
        """
        Initialize the checker with Bybit API client.
        
        Args:
            testnet: Whether to use testnet (default: False for mainnet)
        """
        self.client = HTTP(testnet=testnet)
        self.symbols_data = []
        
    def timestamp_to_datetime(self, timestamp_ms: str) -> str:
        """
        Convert millisecond timestamp to readable datetime string.
        
        Args:
            timestamp_ms: Timestamp in milliseconds as string
            
        Returns:
            Formatted datetime string in YYYY-MM-DD HH:MM:SS format
        """
        try:
            # Convert milliseconds to seconds
            timestamp_sec = int(timestamp_ms) / 1000
            dt = datetime.fromtimestamp(timestamp_sec)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return "Unknown"
    
    def get_all_symbols(self) -> List[Dict[str, Any]]:
        """
        Retrieve all linear (USDT perpetual) symbols from Bybit with pagination.
        
        Returns:
            List of all instruments from all pages
        """
        print("🔍 Fetching all USDT perpetual symbols from Bybit...")
        
        all_instruments = []
        cursor = None
        page = 1
        
        try:
            while True:
                # Prepare request parameters
                params = {"category": "linear", "limit": 1000}
                if cursor:
                    params["cursor"] = cursor
                
                # Make API request
                response = self.client.get_instruments_info(**params)
                
                if response.get("retCode") != 0:
                    error_msg = response.get("retMsg", "Unknown error")
                    raise Exception(f"API Error: {error_msg}")
                
                # Extract instruments from this page
                result = response.get("result", {})
                instruments = result.get("list", [])
                all_instruments.extend(instruments)
                
                print(f"  📄 Page {page}: Retrieved {len(instruments)} symbols (Total: {len(all_instruments)})")
                
                # Check if there are more pages
                next_cursor = result.get("nextPageCursor")
                if not next_cursor:
                    break
                    
                cursor = next_cursor
                page += 1
                
                # Small delay between pages to be nice to the API
                time.sleep(0.5)
            
            print(f"✅ Successfully retrieved {len(all_instruments)} symbols across {page} page(s)")
            return all_instruments
            
        except Exception as e:
            print(f"❌ Error fetching symbols: {e}")
            raise
    
    def parse_instrument_info(self, instrument: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant information from instrument data.
        
        Args:
            instrument: Single instrument data from API response
            
        Returns:
            Dictionary with parsed symbol information
        """
        launch_time = instrument.get("launchTime", "")
        
        symbol_info = {
            "symbol": instrument.get("symbol", ""),
            "launch_timestamp": launch_time,
            "launch_date": self.timestamp_to_datetime(launch_time) if launch_time else "Unknown",
            "timezone": "UTC",
            "status": instrument.get("status", ""),
            "contract_type": instrument.get("contractType", ""),
            "base_coin": instrument.get("baseCoin", ""),
            "settle_coin": instrument.get("settleCoin", "")
        }
        
        return symbol_info
    
    def process_symbols(self, instruments: List[Dict[str, Any]]) -> None:
        """
        Process all symbols from API response.
        
        Args:
            instruments: List of instrument dictionaries from API
        """
        total_symbols = len(instruments)
        
        print(f"📊 Found {total_symbols} symbols. Processing...")
        
        # Process each instrument with progress bar
        for instrument in tqdm(instruments, desc="Processing symbols"):
            symbol_info = self.parse_instrument_info(instrument)
            self.symbols_data.append(symbol_info)
            
            # Small delay to be nice to the API (even though it's one request)
            time.sleep(0.01)
        
        # Sort by launch date (oldest first)
        self.symbols_data.sort(key=lambda x: x.get("launch_timestamp", "0"))
        
        print(f"✅ Processed {len(self.symbols_data)} symbols successfully")
    
    def generate_statistics(self) -> Dict[str, Any]:
        """
        Generate statistics about the symbols.
        
        Returns:
            Dictionary with statistics
        """
        stats = {
            "total_symbols": len(self.symbols_data),
            "active_trading": 0,
            "closed": 0,
            "pre_launch": 0,
            "by_year": {},
            "by_base_coin": {}
        }
        
        for symbol in self.symbols_data:
            # Count by status
            status = symbol.get("status", "").lower()
            if status == "trading":
                stats["active_trading"] += 1
            elif status == "closed":
                stats["closed"] += 1
            elif status == "prelaunch":
                stats["pre_launch"] += 1
            
            # Count by year
            launch_date = symbol.get("launch_date", "")
            if launch_date and launch_date != "Unknown":
                year = launch_date[:4]
                stats["by_year"][year] = stats["by_year"].get(year, 0) + 1
            
            # Count by base coin
            base_coin = symbol.get("base_coin", "")
            if base_coin:
                stats["by_base_coin"][base_coin] = stats["by_base_coin"].get(base_coin, 0) + 1
        
        return stats
    
    def save_to_json(self, filename: str = "symbols_launch_dates.json") -> None:
        """
        Save symbols data to JSON file.
        
        Args:
            filename: Output filename
        """
        # Generate statistics
        stats = self.generate_statistics()
        
        # Prepare final data structure
        output_data = {
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "timezone": "UTC",
            "total_symbols": len(self.symbols_data),
            "statistics": stats,
            "symbols": self.symbols_data
        }
        
        # Save to JSON file
        output_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            filename
        )
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Data saved to {filename}")
        
        # Print summary
        self.print_summary(stats)
    
    def print_summary(self, stats: Dict[str, Any]) -> None:
        """
        Print summary statistics.
        
        Args:
            stats: Statistics dictionary
        """
        print("\n" + "="*60)
        print("📈 SUMMARY STATISTICS")
        print("="*60)
        print(f"Total Symbols: {stats['total_symbols']}")
        print(f"  - Active Trading: {stats['active_trading']}")
        print(f"  - Closed: {stats['closed']}")
        print(f"  - Pre-Launch: {stats['pre_launch']}")
        
        print("\n📅 Symbols by Launch Year:")
        for year in sorted(stats['by_year'].keys()):
            print(f"  {year}: {stats['by_year'][year]} symbols")
        
        print("\n🪙 Top 10 Base Coins:")
        sorted_coins = sorted(stats['by_base_coin'].items(), key=lambda x: x[1], reverse=True)[:10]
        for coin, count in sorted_coins:
            print(f"  {coin}: {count} symbols")
        
        # Show oldest and newest symbols
        if self.symbols_data:
            print("\n🕰️ Timeline (UTC):")
            oldest = self.symbols_data[0]
            newest = self.symbols_data[-1]
            print(f"  Oldest: {oldest['symbol']} - {oldest['launch_date']}")
            print(f"  Newest: {newest['symbol']} - {newest['launch_date']}")
    
    def run(self) -> None:
        """
        Main execution method.
        """
        try:
            # Get all symbols with pagination
            instruments = self.get_all_symbols()
            
            # Process symbols
            self.process_symbols(instruments)
            
            # Save to JSON
            self.save_to_json()
            
            print("\n✨ Done! Check symbols_launch_dates.json for results.")
            
        except Exception as e:
            print(f"❌ Failed to complete: {e}")
            sys.exit(1)


def main():
    """
    Main entry point.
    """
    print("🚀 Bybit Futures Symbols Launch Date Checker")
    print("="*60)
    
    # Create checker instance
    checker = SymbolsLaunchChecker(testnet=False)
    
    # Run the checker
    checker.run()


if __name__ == "__main__":
    main()
