# Matching logic for pickup system v0 MVP - Direct approach
# Uses buckets.py and location_cache.py for core functionality

import math
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone
from collections import defaultdict
import config
from domain import FlightInfo, Rider
from repository import Repository
from service.audit import AuditLogger
from location_cache import LocationCache
from buckets import to_grid_id, haversine_m
import h3

class MatchingEngine2:
    """Streamlined matching engine using direct approach"""
    
    def __init__(self, repository: Repository, audit_logger: AuditLogger, 
                 location_cache: LocationCache):
        self.repository = repository
        self.audit = audit_logger
        self.location_cache = location_cache
        
        # Config
        self.time_slot_min = config.TIME_SLOT_MIN
        self.grid_res = config.GRID_RES
        self.grid_kring = config.GRID_KRING
        self.max_spread_m = config.MAX_PICKUP_SPREAD_M
        self.w_time = config.W_TIME
        self.w_spatial = config.W_SPATIAL
        self.terminal_mode = config.TERMINAL_MODE
        self.read_horizon = config.READ_HORIZON_MIN
        self.default_tz = timezone.pst
    
    def run_cycle(self, now_ts: Optional[datetime] = None, dry_run: bool = False) -> Dict:
        """Main matching cycle"""
        if now_ts is None:
            now_ts = datetime.now(timezone.pst)
        
        self.audit.log_matching_cycle_start(0, self.read_horizon)
        
        try:
            # Fetch and prepare
            flights = self.repository.fetch_eligible_flights(now_ts, self.read_horizon)
            if not flights:
                return self._empty_results()
            
            riders = [self._to_rider(f) for f in flights]
            groups = self._bucket_and_match(riders)
            
            # Commit
            if not dry_run:
                rides_created = self._commit_groups(groups)
            else:
                rides_created = len(groups)
            
            self.audit.log_matching_cycle_end(len(groups), rides_created)
            return self._build_metrics(len(flights), rides_created, groups)
            
        except Exception as e:
            self.audit.log_error("matching_cycle", str(e))
            raise
    
    def run_cycle_dry_run(self, now_ts: Optional[datetime] = None) -> Dict:
        return self.run_cycle(now_ts, dry_run=True)
    
    def _to_rider(self, flight: FlightInfo) -> Rider:
        """Convert FlightInfo to Rider"""
        # Time
        earliest = datetime.combine(flight.date, flight.earliest_time).replace(tzinfo=self.default_tz)
        latest = datetime.combine(flight.date, flight.latest_time).replace(tzinfo=self.default_tz)
        time_slot = self._time_slot(earliest)
        
        # Spatial
        anchor = flight.school if flight.to_airport else flight.airport
        lat, lon = self.location_cache.get_coordinates(anchor)
        if lat is None or lon is None:
            raise ValueError(f"No coordinates for {anchor}")
        
        grid_id = to_grid_id(lat, lon, self.grid_res)
        
        return Rider(
            flight_id=flight.flight_id,
            user_id=flight.user_id,
            airport=flight.airport.upper().strip(),
            to_airport=flight.to_airport,
            school=flight.school,
            earliest_ts=earliest,
            latest_ts=latest,
            time_slot_start=time_slot,
            grid_id=grid_id,
            terminal=self._normalize_terminal(flight.terminal)
        )
    
    def _normalize_terminal(self, terminal: Optional[str]) -> Optional[str]:
        """Quick terminal normalization"""
        if not terminal:
            return None
        terminal = terminal.upper().strip()
        if terminal.startswith("TERMINAL "):
            return f"T{terminal[9:]}"
        if terminal.isdigit():
            return f"T{terminal}"
        return terminal
    
    def _time_slot(self, ts: datetime) -> str:
        """Compute time slot"""
        minutes = (ts.minute // self.time_slot_min) * self.time_slot_min
        slot = ts.replace(minute=minutes, second=0, microsecond=0)
        return slot.strftime("%H:%M")
    
    def _bucket_and_match(self, riders: List[Rider]) -> List[Dict]:
        """Bucket riders and match them in one pass"""
        buckets = defaultdict(list)
        
        # Bucket by primary constraints
        for rider in riders:
            key = (rider.to_airport, rider.airport, rider.time_slot_start)
            buckets[key].append(rider)
        
        # Match each bucket immediately
        all_groups = []
        for bucket_key, bucket_riders in buckets.items():
            if len(bucket_riders) < 2:
                continue
            
            groups = self._match_bucket(bucket_riders, bucket_key)
            all_groups.extend(groups)
        
        return all_groups
    
    def _match_bucket(self, riders: List[Rider], bucket_key: Tuple) -> List[Dict]:
        """Match riders within a bucket"""
        if len(riders) < 2:
            return []
        
        # Build and score pairs
        pairs = []
        for i in range(len(riders)):
            for j in range(i + 1, len(riders)):
                score = self._score_group([riders[i], riders[j]], validate=True)
                if score > float('-inf'):
                    pairs.append((i, j, score))
        
        # Sort by score and select non-overlapping pairs
        pairs.sort(key=lambda x: x[2], reverse=True)
        selected_pairs = []
        used = set()
        
        for i, j, score in pairs:
            if i not in used and j not in used:
                selected_pairs.append((i, j))
                used.add(i)
                used.add(j)
        
        # Create groups from pairs
        groups = []
        for i, j in selected_pairs:
            pair = [riders[i], riders[j]]
            
            # Try to expand to 3-4
            expanded = self._expand_group(pair, riders)
            
            # Simple departure time (midpoint of intersection)
            start = max(member.earliest_ts for member in expanded)
            end = min(member.latest_ts for member in expanded)
            departure = start + (end - start) / 2
            
            group = {
                "airport": expanded[0].airport,
                "to_airport": expanded[0].to_airport,
                "member_flights": [{"user_id": r.user_id, "flight_id": r.flight_id} for r in expanded],
                "planned_departure_ts": departure,
                "bucket_key": bucket_key
            }
            groups.append(group)
        
        return groups
    
    def _score_group(self, members: List[Rider], validate: bool = False) -> float:
        """Score any group size (2-4) with optional validation"""
        if validate and not self._is_valid_group(members):
            return float('-inf')
        
        # Time intersection
        start = max(member.earliest_ts for member in members)
        end = min(member.latest_ts for member in members)
        time_fit = (end - start).total_seconds() / 60
        
        # Spatial cohesion (average of pairwise spatial scores)
        spatial_scores = []
        for idx_a in range(len(members)):
            for idx_b in range(idx_a + 1, len(members)):
                if members[idx_a].grid_id == members[idx_b].grid_id:
                    spatial_scores.append(1.0)
                elif self._spatial_neighbor(members[idx_a], members[idx_b]):
                    spatial_scores.append(0.6)
                else:
                    spatial_scores.append(0.1)
        
        spatial = sum(spatial_scores) / len(spatial_scores) if spatial_scores else 0.0
        
        # Terminal matching (optional scoring)
        terminal_bonus = 0.0
        if self.terminal_mode == "strict":
            terminals = [member.terminal for member in members if member.terminal]
            if len(set(terminals)) > 1:
                terminal_bonus = -0.5
        
        return self.w_time * time_fit + self.w_spatial * spatial + terminal_bonus
    
    def _spatial_neighbor(self, rider_a: Rider, rider_b: Rider) -> bool:
        """Check if riders are spatial neighbors"""
        if self.grid_kring == 0:
            return rider_a.grid_id == rider_b.grid_id
        
        neighbors = h3.grid_ring(rider_a.grid_id, self.grid_kring)
        return rider_b.grid_id in neighbors
    
    def _expand_group(self, members: List[Rider], candidates: List[Rider]) -> List[Rider]:
        """Expand group to 3-4 members using greedy approach"""
        while len(members) < 4:
            best = None
            best_score = float('-inf')
            
            for candidate in candidates:
                if candidate.user_id in [member.user_id for member in members]:
                    continue
                
                test_group = members + [candidate]
                score = self._score_group(test_group, validate=True)
                if score > best_score:
                    best_score = score
                    best = candidate
            
            if best is None:
                break
            
            members.append(best)
        
        return members
    
    def _is_valid_group(self, members: List[Rider]) -> bool:
        """Check if group is valid"""
        if len(members) < 2 or len(members) > 4:
            return False
        
        # Hard rule: Time intersection must exist
        start = max(member.earliest_ts for member in members)
        end = min(member.latest_ts for member in members)
        return start < end
    
    def _commit_groups(self, groups: List[Dict]) -> int:
        """Commit groups to database"""
        rides_created = 0
        
        for group in groups:
            try:
                ride_date = group["planned_departure_ts"].date()
                ride_id = self.repository.create_ride(ride_date)
                
                self.repository.add_members(ride_id, group["member_flights"])
                
                flight_ids = [member["flight_id"] for member in group["member_flights"]]
                self.repository.mark_flights_matched(flight_ids)
                
                rides_created += 1
                
                self.audit.log_ride_created(
                    ride_id, len(group["member_flights"]),
                    ride_date.isoformat(), group["airport"]
                )
                
            except Exception as e:
                self.audit.log_error("ride_creation", str(e))
                continue
        
        return rides_created
    
    def _build_metrics(self, eligible_count: int, rides_created: int, groups: List[Dict]) -> Dict:
        """Build metrics"""
        size_dist = {}
        for group in groups:
            size = len(group["member_flights"])
            size_dist[size] = size_dist.get(size, 0) + 1
        
        return {
            'eligible_count': eligible_count,
            'rides_created': rides_created,
            'size_distribution': size_dist,
            'total_rides': rides_created
        }
    
    def _empty_results(self) -> Dict:
        return {
            'eligible_count': 0,
            'rides_created': 0,
            'size_distribution': {},
            'total_rides': 0
        }
