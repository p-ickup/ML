"""
Supabase Storage helper functions for file operations.
Handles downloading, uploading, and archiving files in Supabase Storage buckets.
"""

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

from supabase import Client


def download_file(sb: Client, bucket_name: str, file_path: str, local_path: Optional[str] = None) -> str:
    """
    Download a file from Supabase Storage to a local file.
    
    Args:
        sb: Supabase client
        bucket_name: Name of the storage bucket
        file_path: Path to file in bucket (e.g., "active.csv" or "archive/file.csv")
        local_path: Optional local file path. If None, creates a temporary file.
    
    Returns:
        Path to the local file
    
    Raises:
        Exception: If download fails
    """
    if local_path is None:
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv')
        local_path = temp_file.name
        temp_file.close()
    
    try:
        # Download file from Supabase Storage
        response = sb.storage.from_(bucket_name).download(file_path)
        
        # Write to local file
        with open(local_path, 'wb') as f:
            f.write(response)
        
        return local_path
    except Exception as e:
        # Clean up temp file on error
        if local_path and Path(local_path).exists():
            Path(local_path).unlink(missing_ok=True)
        raise Exception(f"Failed to download {file_path} from {bucket_name}: {str(e)}")


def upload_file(sb: Client, bucket_name: str, file_path: str, local_path: str, content_type: str = "text/csv") -> None:
    """
    Upload a local file to Supabase Storage.
    
    Args:
        sb: Supabase client
        bucket_name: Name of the storage bucket
        file_path: Path where file should be stored in bucket
        local_path: Path to local file to upload
        content_type: MIME type of the file (default: text/csv)
    
    Raises:
        Exception: If upload fails
    """
    if not Path(local_path).exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")
    
    with open(local_path, 'rb') as f:
        file_data = f.read()
    
    try:
        # Upload file to Supabase Storage (overwrites if exists)
        # Note: Supabase Python client uses file_options parameter
        sb.storage.from_(bucket_name).upload(
            file_path,
            file_data,
            file_options={"content-type": content_type, "upsert": "true"}
        )
    except Exception as e:
        raise Exception(f"Failed to upload {file_path} to {bucket_name}: {str(e)}")


def archive_file(sb: Client, bucket_name: str, source_path: str, archive_folder: str = "archive") -> None:
    """
    Archive a file by copying it to the archive folder with a timestamp.
    
    Args:
        sb: Supabase client
        bucket_name: Name of the storage bucket
        source_path: Path to file in bucket to archive
        archive_folder: Folder name for archived files (default: "archive")
    """
    # Download the source file
    temp_file = download_file(sb, bucket_name, source_path)
    
    try:
        # Create archive path with timestamp
        source_filename = Path(source_path).name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_filename = f"{timestamp}_{source_filename}"
        archive_path = f"{archive_folder}/{archive_filename}"
        
        # Upload to archive location
        upload_file(sb, bucket_name, archive_path, temp_file)
        print(f"Archived {source_path} to {archive_path}")
    finally:
        # Clean up temp file
        Path(temp_file).unlink(missing_ok=True)


def file_exists(sb: Client, bucket_name: str, file_path: str) -> bool:
    """
    Check if a file exists in Supabase Storage.
    
    Args:
        sb: Supabase client
        bucket_name: Name of the storage bucket
        file_path: Path to file in bucket
    
    Returns:
        True if file exists, False otherwise
    """
    try:
        # Try to list files in the directory
        parent_path = str(Path(file_path).parent) if Path(file_path).parent != Path('.') else ""
        files = sb.storage.from_(bucket_name).list(parent_path)
        filename = Path(file_path).name
        return any(f.get("name") == filename for f in files)
    except Exception:
        # If listing fails, try downloading (will raise exception if not found)
        try:
            sb.storage.from_(bucket_name).download(file_path)
            return True
        except Exception:
            return False


def upload_csv_string(sb: Client, bucket_name: str, file_path: str, csv_content: str) -> None:
    """
    Upload CSV content (as string) directly to Supabase Storage.
    
    Args:
        sb: Supabase client
        bucket_name: Name of the storage bucket
        file_path: Path where file should be stored in bucket
        csv_content: CSV content as string
    """
    # Convert string to bytes
    file_data = csv_content.encode('utf-8')
    
    # Upload to Supabase Storage
    sb.storage.from_(bucket_name).upload(
        file_path,
        file_data,
        file_options={"content-type": "text/csv", "upsert": "true"}
    )

