import os
import shutil
import tempfile
from pathlib import Path

import pyzipper
import dask.bag as db
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster
import multiprocessing

class ParallelZipCompressor:
    def __init__(self, compression_level=6, chunk_size=1000):
        self.compression_level = compression_level
        self.chunk_size = chunk_size
        self.temp_dir = None

    def _compress_chunk(self, chunk_data):
        """Compress a chunk of files into a temporary zip file."""
        chunk_files, chunk_index, password = chunk_data
        temp_zip = os.path.join(self.temp_dir, f"chunk_{chunk_index}.zip")

        with pyzipper.AESZipFile(
            temp_zip,
            "w",
            compression=pyzipper.ZIP_DEFLATED,
            compresslevel=self.compression_level,
            encryption=pyzipper.WZ_AES if password else None,
        ) as zipf:
            if password:
                zipf.setpassword(password.encode())

            for file_path, rel_path in chunk_files:
                try:
                    zipf.write(file_path, arcname=rel_path)
                except Exception as e:
                    print(f"Error adding {file_path}: {e}")

        return temp_zip

    def _merge_chunks(self, chunk_files, output_path, password=None):
        """Merge chunk files into final zip file."""
        with pyzipper.AESZipFile(
            output_path,
            "w",
            compression=pyzipper.ZIP_DEFLATED,
            compresslevel=self.compression_level,
            encryption=pyzipper.WZ_AES if password else None,
        ) as final_zip:
            if password:
                final_zip.setpassword(password.encode())

            for chunk_file in chunk_files:
                with pyzipper.AESZipFile(chunk_file, "r") as chunk_zip:
                    for item in chunk_zip.infolist():
                        final_zip.writestr(item, chunk_zip.read(item.filename))
        
        return output_path

    def compress(self, files, output_path, password=None):
        """Compress files in parallel using Dask."""
        self.temp_dir = tempfile.mkdtemp(prefix="parallel_zip_")
        n_workers = min(16, max(1, multiprocessing.cpu_count() - 1))
        
        try:
            print(f"Processing {len(files)} files with {n_workers} workers...")
            
            # Optionally, use Dask distributed for better scale-out
            # cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1)
            # client = Client(cluster)
            # print(f"Dask dashboard available at: {client.dashboard_link}")
            
            # Step 1: Prepare file paths using Dask
            def prepare_paths(file_path):
                try:
                    rel_path = Path(file_path).relative_to(Path(file_path).anchor)
                    return str(file_path), str(rel_path)
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
                    return None
            
            bag = db.from_sequence(files, npartitions=n_workers)
            
            with ProgressBar():
                print("Preparing file paths...")
                file_pairs = bag.map(prepare_paths).filter(lambda x: x).compute()
            
            print(f"Preparing to compress {len(file_pairs)} files...")
            
            # Step 2: Create chunks
            chunks = [
                file_pairs[i : i + self.chunk_size]
                for i in range(0, len(file_pairs), self.chunk_size)
            ]
            
            # Step 3: Prepare data for chunk compression
            chunk_data = [(chunk, idx, password) for idx, chunk in enumerate(chunks)]
            
            # Step 4: Compress chunks in parallel using Dask
            chunk_bag = db.from_sequence(chunk_data, npartitions=min(len(chunk_data), n_workers))
            
            with ProgressBar():
                print(f"Compressing in {len(chunks)} parallel chunks...")
                chunk_files = chunk_bag.map(self._compress_chunk).compute()
            
            # Step 5: Merge chunks (this could be a bottleneck, so it's done sequentially)
            print(f"Merging {len(chunk_files)} chunk files into final zip: '{output_path}'")
            result_path = self._merge_chunks(chunk_files, output_path, password)
            
            print(f"Compression completed: {result_path}")
            return Path(result_path).absolute()
            
        finally:
            # Cleanup
            if self.temp_dir and os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                self.temp_dir = None