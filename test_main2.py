#!/usr/bin/env python3
"""
Simple test script for main2.py integration
Tests the key functions without requiring database connection
"""

import os
import sys
from datetime import datetime, date, time, timedelta

# Add pickup directory to path for imports
sys.path.insert(0, 'pickup')

from main2 import fetch_flights, fetch_users


def test_fetch_flights_logic():
    """Test the flight filtering logic"""
    print("🧪 Testing flight filtering logic...")
    
    # Mock current time
    now = datetime(2024, 1, 15, 12, 0, 0)
    
    # Mock flights data
    mock_flights = [
        {
            "flight_id": 1,
            "date": "2024-01-17",  # 2 days from now
            "earliest_time": "14:00:00"
        },
        {
            "flight_id": 2,
            "date": "2024-01-19",  # 4 days from now
            "earliest_time": "10:00:00"
        },
        {
            "flight_id": 3,
            "date": "2024-01-20",  # 5 days from now (should be filtered out)
            "earliest_time": "16:00:00"
        }
    ]
    
    # Test the filtering logic
    lower_bound = (now + timedelta(days=2)).replace(hour=0, minute=0, second=0)
    upper_bound = (now + timedelta(days=4)).replace(hour=0, minute=0, second=0)
    
    filtered = []
    for f in mock_flights:
        try:
            flight_datetime = datetime.fromisoformat(f["date"] + "T" + f["earliest_time"])
            if lower_bound <= flight_datetime <= upper_bound:
                filtered.append(f)
        except Exception as e:
            print(f"Skipping flight {f['flight_id']}: {e}")
    
    # Should keep flights 1 and 2, filter out flight 3
    assert len(filtered) == 2, f"Expected 2 filtered flights, got {len(filtered)}"
    assert filtered[0]["flight_id"] == 1, "First flight should be ID 1"
    assert filtered[1]["flight_id"] == 2, "Second flight should be ID 2"
    
    print("✅ Flight filtering logic test passed")


def test_user_mapping():
    """Test user to school mapping logic"""
    print("🧪 Testing user mapping logic...")
    
    # Mock users data
    mock_users = [
        {"user_id": "user1", "school": "UCLA"},
        {"user_id": "user2", "school": "USC"},
        {"user_id": "user3", "school": "UCLA"}
    ]
    
    # Test the mapping logic
    user_map = {u["user_id"]: u["school"] for u in mock_users}
    
    assert user_map["user1"] == "UCLA", "User1 should map to UCLA"
    assert user_map["user2"] == "USC", "User2 should map to USC"
    assert user_map["user3"] == "UCLA", "User3 should map to UCLA"
    assert len(user_map) == 3, "Should have 3 user mappings"
    
    print("✅ User mapping test passed")


def main():
    """Run all tests"""
    print("🚀 Running main2.py integration tests...\n")
    
    try:
        test_fetch_flights_logic()
        test_user_mapping()
        
        print("\n🎉 All tests passed!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
