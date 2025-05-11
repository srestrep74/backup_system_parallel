from pathlib import Path
import pyzipper
import zipfile
import shutil
import pandas as pd
from typing import List
import click
from src.utils.DatabaseManager import DatabaseManager

def restore_backup(zip_path: Path, output_dir: Path, password: str = None) -> None:
    """Restore a backup from a single zip file"""
    if not zip_path.exists():
        raise FileNotFoundError(f"File not found: {zip_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        if password:
            with pyzipper.AESZipFile(zip_path, 'r') as zf:
                zf.pwd = password.encode()
                zf.extractall(output_dir)
        else:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(output_dir)

        print(f"Restore completed successfully in: {output_dir}")
    except RuntimeError as e:
        raise RuntimeError("Decryption failed. Is the password incorrect?") from e
    except Exception as e:
        raise RuntimeError(f"An error occurred during restore: {e}") from e

def restore_fragmented_backup(filename: str, output_dir: Path, password: str = None) -> None:
    """Restore a backup that was fragmented across multiple devices"""
    db_manager = DatabaseManager()
    fragments = db_manager.list_fragmented_files(filename)
    
    if fragments.empty:
        raise FileNotFoundError(f"No fragments found for {filename} in the database")
    
    # Create temporary directory for operations
    temp_dir = output_dir / "temp_restore"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Step 1: Copy all fragments to temporary directory
        for fragment_path in fragments:
            fragment_path = Path(fragment_path)
            if not fragment_path.exists():
                raise FileNotFoundError(f"Fragment not found: {fragment_path}")
            shutil.copy2(fragment_path, temp_dir)
        
        # Step 2: Reassemble the original zip file
        fragment_files = sorted(list(temp_dir.glob(f"{Path(filename).stem}.part*")))
        if not fragment_files:
            raise FileNotFoundError("No fragment files found in temporary directory")
        
        assembled_zip = temp_dir / filename
        with open(assembled_zip, 'wb') as outfile:
            for fragment in fragment_files:
                with open(fragment, 'rb') as infile:
                    shutil.copyfileobj(infile, outfile)
        
        # Step 3: Extract the assembled zip file
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            if password:
                with pyzipper.AESZipFile(assembled_zip, 'r') as zf:
                    zf.pwd = password.encode()
                    zf.extractall(output_dir)
            else:
                with zipfile.ZipFile(assembled_zip, 'r') as zf:
                    zf.extractall(output_dir)
            
            print(f"Successfully restored and extracted {filename} from {len(fragment_files)} fragments")
            print(f"Contents extracted to: {output_dir}")
        
        except RuntimeError as e:
            raise RuntimeError("Decryption failed. Is the password incorrect?") from e
        except Exception as e:
            raise RuntimeError(f"An error occurred during extraction: {e}") from e
        
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
