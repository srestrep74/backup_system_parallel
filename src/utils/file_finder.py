import os
from pathlib import Path
import dask.bag as db
from dask.diagnostics import ProgressBar
import multiprocessing


class FileFinder:
    @staticmethod
    def find_files(directories):
        """Find files in directories using sequential scanning."""
        file_paths = set()
        for dir_path in directories:
            for root, _, files in os.walk(dir_path):
                for file in files:
                    full_path = Path(root) / file
                    file_paths.add(full_path.resolve())
        return sorted(file_paths)
    
    @staticmethod
    def find_files_parallel(directories, max_workers=None):
        """
        Find files in parallel using Dask.
        
        Args:
            directories: List of directory paths to scan
            max_workers: Maximum number of parallel workers
            
        Returns:
            List of sorted unique file paths
        """
        if max_workers is None:
            max_workers = min(8, max(1, multiprocessing.cpu_count() - 1))
        
        print(f"Scanning {len(directories)} directories with {max_workers} workers...")
        
        dir_bag = db.from_sequence(directories, npartitions=min(len(directories), max_workers))
        
        def scan_directory(dir_path):
            result_files = []
            try:
                for root, _, files in os.walk(dir_path):
                    for file in files:
                        full_path = Path(root) / file
                        result_files.append(str(full_path.resolve()))
            except Exception as e:
                print(f"Error scanning {dir_path}: {str(e)}")
            return result_files
        
        with ProgressBar():
            all_files = dir_bag.map(scan_directory).flatten().compute()
        
        unique_files = sorted(set(all_files))
        print(f"Found {len(unique_files)} files")
        return unique_files