"""
Nepo matching - Force match specific user_ids if they're in the same bucket.
Easy to disable by setting NEPO_MODE = False in config.py.
"""

from typing import List, Optional, Tuple

import config
from rider_data import RiderLite
from ruleMatching import Match, _group_to_match, _is_valid_group


def force_nepo_match(riders: List[RiderLite], bucket_key: Optional[str] = None) -> Tuple[Optional[Match], List[RiderLite]]:
    """
    Force match user_ids specified in config.NEPO_USER_IDS if they're all in the same bucket.
    
    Args:
        riders: List of riders in the current bucket
        bucket_key: The bucket key for this group
        
    Returns:
        Tuple of (forced_match or None, remaining_riders)
        If a forced match is created, those riders are removed from the remaining list.
    """
    # Check if nepo mode is enabled
    if not getattr(config, 'NEPO_MODE', False):
        return None, riders
    
    # Get the list of user_ids to force match
    nepo_user_ids = getattr(config, 'NEPO_USER_IDS', [])
    if not nepo_user_ids:
        return None, riders
    
    # Find riders with matching user_ids in this bucket
    nepo_riders = [r for r in riders if r.user_id in nepo_user_ids]
    
    # Need at least 2 riders to form a match
    if len(nepo_riders) < 2:
        return None, riders
    
    # Check if all specified user_ids are present in this bucket
    found_user_ids = {r.user_id for r in nepo_riders}
    if found_user_ids != set(nepo_user_ids):
        # Not all specified users are in this bucket, skip force matching
        return None, riders
    
    # Check if the group is valid (time overlap, bags, etc.)
    if not _is_valid_group(nepo_riders):
        print(f"  [NEPO] Cannot force match {nepo_user_ids}: group validation failed")
        return None, riders
    
    # Create the forced match
    forced_match = _group_to_match(nepo_riders, bucket_key=bucket_key)
    
    # Remove matched riders from the remaining list
    matched_flight_ids = {r.flight_id for r in nepo_riders}
    remaining_riders = [r for r in riders if r.flight_id not in matched_flight_ids]
    
    print(f"  [NEPO] Force matched {len(nepo_riders)} riders: {nepo_user_ids}")
    
    return forced_match, remaining_riders



