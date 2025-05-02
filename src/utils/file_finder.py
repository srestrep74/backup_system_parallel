import os
from pathlib import Path
import dask.bag as db
from dask.diagnostics import ProgressBar
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

class FileFinder:
    @staticmethod
    def find_files(directories):
        file_paths = set()
        for dir_path in directories:
            for root, _, files in os.walk(dir_path):
                for file in files:
                    full_path = Path(root) / file
                    file_paths.add(full_path.resolve())
        return sorted(file_paths)
    
    @staticmethod
    def find_files_parallel(directories, max_workers=None):
        """Find files in parallel using Dask"""
        if max_workers is None:
            max_workers = min(8, max(1, multiprocessing.cpu_count() - 1))
        
        # This function will be executed in parallel
        def scan_directory(dir_path):
            result_files = []
            try:
                for root, _, files in os.walk(dir_path):
                    for file in files:
                        full_path = Path(root) / file
                        result_files.append(full_path.resolve())
            except Exception as e:
                print(f"Error scanning directory {dir_path}: {str(e)}")
            return result_files
        
        print(f"Scanning {len(directories)} directories in parallel...")
        
        # First level parallelization: scan each top directory in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for directory in directories:
                futures.append(executor.submit(scan_directory, directory))
            
            all_files = []
            for future in futures:
                try:
                    all_files.extend(future.result())
                except Exception as e:
                    print(f"Error collecting directory scan results: {str(e)}")
        
        # Remove duplicates and sort
        unique_files = sorted(set(all_files))
        print(f"Found {len(unique_files)} files in total")
        return unique_files
