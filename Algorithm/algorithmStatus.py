"""
AlgorithmStatus tracking module for pickup matching system.
Handles creation, updates, and tracking of algorithm execution status.
"""

import uuid
from datetime import datetime
from typing import List, Optional

from rider_data import RiderLite
from supabase import Client


def determine_target_scope(riders: List[RiderLite]) -> str:
    """
    Determine target_scope based on riders' schools.
    - If all riders are from Pomona: "Pomona"
    - If all riders are from other schools (not Pomona): "Other"
    - If mixed: "All"
    """
    if not riders:
        return "All"
    
    schools = {r.school.upper().strip() if r.school else "" for r in riders}
    schools = {s for s in schools if s}  # Remove empty strings
    
    if not schools:
        return "All"
    
    all_pomona = all(s == "POMONA" for s in schools)
    all_other = all(s != "POMONA" for s in schools)
    
    if all_pomona:
        return "Pomona"
    elif all_other:
        return "Other"
    else:
        return "All"


def get_or_create_algorithm_status(sb: Client, algorithm_name: str, target_scope: str) -> Optional[str]:
    """
    Check for a scheduled algorithm run and return its id, or create a new one.
    Returns the status record id (uuid as string) or None if creation fails.
    """
    now = datetime.now()
    
    # Check for scheduled runs (status = 'scheduled' and scheduled_for <= now)
    scheduled_resp = (
        sb.table("AlgorithmStatus")
        .select("id")
        .eq("algorithm_name", algorithm_name)
        .eq("target", target_scope)
        .eq("status", "scheduled")
        .lte("scheduled_for", now.isoformat())
        .order("scheduled_for", desc=False)
        .limit(1)
        .execute()
    )
    
    if scheduled_resp.data and len(scheduled_resp.data) > 0:
        status_id = scheduled_resp.data[0]["id"]
        # Update to 'running' status
        sb.table("AlgorithmStatus").update({
            "status": "running",
            "started_at": now.isoformat()
        }).eq("id", status_id).execute()
        return status_id
    
    # No scheduled run found, create a new one
    run_id = str(uuid.uuid4())
    new_status = {
        "algorithm_name": algorithm_name,
        "target": target_scope,
        "scheduled_for": now.isoformat(),
        "started_at": now.isoformat(),
        "status": "running",
        "run_id": run_id
    }
    
    resp = sb.table("AlgorithmStatus").insert(new_status).execute()
    if resp.data and len(resp.data) > 0:
        return resp.data[0]["id"]
    
    return None


def update_algorithm_status(
    sb: Client,
    status_id: Optional[str],
    status: str,
    error_message: Optional[str] = None,
    run_id: Optional[str] = None
) -> None:
    """
    Update algorithm status to 'success' or 'failed'.
    """
    if not status_id:
        return
    
    update_data = {
        "status": status,
        "finished_at": datetime.now().isoformat()
    }
    
    if error_message:
        update_data["error_message"] = error_message
    
    if run_id:
        update_data["run_id"] = run_id
    
    sb.table("AlgorithmStatus").update(update_data).eq("id", status_id).execute()

