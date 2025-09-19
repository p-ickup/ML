# main2.py - Streamlined Pickup System v0 MVP

## Overview
`main2.py` is a streamlined version of the pickup system that directly handles ride creation and matching, similar to the previous `main.py` approach but integrated with the new `matching2.py` engine.

## Key Features
✅ **Direct Ride Creation** - Creates proper `ride_id` values  
✅ **Streamlined Flow** - Simple fetch → match → persist pipeline  
✅ **Proper Database Schema** - Uses existing `Matches` table structure  
✅ **Automatic ride_id Assignment** - Increments from max existing ride_id  
✅ **Flight Marking** - Sets `Flights.matched = true` after successful matching  

## How It Works

### 1. **Flight Fetching**
- Retrieves flights scheduled 2-4 days from now
- Filters to `matched = false` only
- Validates date/time format

### 2. **User School Mapping**
- Fetches school info for all users with flights
- Maps `user_id` → `school` for spatial scoring

### 3. **Matching Engine**
- Uses `MatchingEngine2` from `matching2.py`
- Runs in dry-run mode to get groups without persistence
- Returns matched groups with member details

### 4. **Ride Creation**
- Assigns sequential `ride_id` values
- Creates entries in `Matches` table with:
  - `ride_id` - Unique ride identifier
  - `user_id` - User in the ride
  - `flight_id` - Flight being matched
  - `created_at` - Timestamp of match creation

### 5. **Flight Marking**
- Updates `Flights.matched = true` for all matched flights
- Prevents double-matching

## Database Schema Used

### Matches Table
```sql
CREATE TABLE Matches (
    ride_id INTEGER,      -- Generated sequentially
    user_id TEXT,         -- User identifier
    flight_id INTEGER,    -- Flight identifier
    created_at TIMESTAMP  -- Match creation time
);
```

### Flights Table
```sql
-- Updates matched = true after successful matching
UPDATE Flights SET matched = true WHERE flight_id IN (...);
```

## Environment Variables
```bash
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
GOOGLE_MAPS_API_KEY=your_google_api_key  # Optional
```

## Usage
```bash
# Run the matching system
python main2.py

# The system will:
# 1. Fetch eligible flights (2-4 days ahead)
# 2. Run matching engine
# 3. Create ride groups
# 4. Persist to database
# 5. Mark flights as matched
```

## Output Example
```
Processing 15 eligible flights
Matched 4 groups
[Group] Size: 3 | Airport: LAX
[Group] Size: 2 | Airport: LAX
[Group] Size: 4 | Airport: SFO
[Group] Size: 2 | Airport: SFO
✅ Inserted 11 match entries.
```

## Key Differences from Previous main.py

| Feature | Previous main.py | New main2.py |
|---------|------------------|---------------|
| **Matching** | KMeans clustering | `MatchingEngine2` |
| **Ride IDs** | Manual assignment | Auto-increment from max |
| **Database** | Direct Supabase calls | Repository abstraction |
| **Features** | Raw feature extraction | Domain objects |
| **Flow** | Complex pipeline | Simple 3-step process |

## Integration Points
- **`matching2.py`** - Core matching logic
- **`location_cache.py`** - Spatial coordinates (optional)
- **`pickup/` modules** - Repository, audit, domain

## Benefits
1. **Simpler** - Fewer functions, clearer flow
2. **More Robust** - Better error handling and validation
3. **Proper IDs** - Sequential ride_id assignment
4. **Cleaner Schema** - Uses existing table structure
5. **Better Integration** - Leverages new matching engine

The new `main2.py` maintains the streamlined approach of the original while integrating with the improved `matching2.py` engine and proper database schema handling.
