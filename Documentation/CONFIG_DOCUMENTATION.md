# Pickup System Configuration Documentation

This document provides simple explanations of all configuration parameters in `config.py`.

## Configuration Parameters

### Time & Spatial Bucket Sizes
* [TIME_SLOT_MIN]: discretizes pickup/dropoff times into manageable buckets for matching algorithm
* [H3_RES]: H3 resolution for spatial bucketing - creates ~174m hexagons for precise location grouping
* [PICKUP_KRING]: expands pickup search area by 2 hexagon rings around origin for better matching opportunities

### Detour Limits
* [MAX_DETOUR_DEFAULT_MIN]: maximum detour time in minutes for drivers to pick up additional passengers

### Weights for Matching Algorithm
* [W_TIME]: weight for time-based matching - prioritizes time compatibility between pickup/dropoff schedules
* [W_ROUTE]: weight for route efficiency - prioritizes routes that minimize total travel distance/time
* [W_TERMINAL]: weight for terminal proximity - prioritizes matches near airport terminals
* [W_BUDGET]: weight for budget considerations - prioritizes cost-effective matches
* [W_BAGS]: weight for bag capacity - prioritizes matches that optimize bag space utilization

### Bag Targets
* [BAG_TARGET_TOTAL]: maximum number of bags that can be carried in a single vehicle
* [BAG_TARGET_LARGE]: maximum number of large/oversized bags that can be accommodated

### Policy Toggles
* [TERMINAL_MODE]: terminal matching policy - strict enforces exact terminal matches, soft allows nearby terminal flexibility

### Scheduler Configuration
* [SCHEDULER_INTERVAL_MIN]: how often the matching algorithm runs in minutes
* [SCHEDULER_BATCH_SIZE]: number of requests to process in each scheduler run
* [SCHEDULER_MAX_RETRIES]: maximum retry attempts for failed matching attempts
* [SCHEDULER_TIMEOUT_SEC]: maximum time to wait for matching completion

### API Configuration
* [GOOGLE_MAPS_API_KEY]: Google Maps API key for geocoding and routing - set via environment variable
* [API_RATE_LIMIT_PER_MIN]: rate limiting for external API calls to avoid hitting limits
* [API_TIMEOUT_SEC]: timeout for API calls - maximum time to wait for external API responses

### Database Configuration
* [SUPABASE_URL]: Supabase project URL for database operations - set via environment variable
* [SUPABASE_KEY]: Supabase API key for authentication - set via environment variable

### Cache Configuration
* [LOCATION_CACHE_TTL_HOURS]: how long to keep location coordinates in cache before refreshing
* [OD_CACHE_TTL_HOURS]: how long to keep route/ETA calculations in cache (shorter due to traffic changes)
* [CACHE_MAX_SIZE]: maximum cache entries to prevent memory overflow

### Logging Configuration
* [LOG_LEVEL]: controls verbosity of system logs (DEBUG, INFO, WARNING, ERROR)
* [AUDIT_LOG_ENABLED]: enables audit logging for explainability and compliance
* [AUDIT_LOG_RETENTION_DAYS]: how long to keep audit logs before cleanup