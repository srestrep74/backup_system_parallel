import os
import shutil
import tempfile
from pathlib import Path
import multiprocessing

import pyzipper
import dask.bag as db
from dask.diagnostics import ProgressBar


def compress_chunk(chunk_data):
    chunk_files, chunk_index, temp_dir, compression_level, password = chunk_data
    temp_zip = os.path.join(temp_dir, f"chunk_{chunk_index}.zip")

    with pyzipper.AESZipFile(
        temp_zip,
        "w",
        compression=pyzipper.ZIP_DEFLATED,
        compresslevel=compression_level,
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


def process_chunk_for_merge(chunk_file):
    try:
        items = []
        with pyzipper.AESZipFile(chunk_file, "r") as chunk_zip:
            for item in chunk_zip.infolist():
                content = chunk_zip.read(item.filename)
                items.append((item.filename, content))
        return items
    except Exception as e:
        print(f"Error processing chunk {chunk_file}: {e}")
        return []


class ParallelZipCompressor:
    def __init__(self, compression_level=6, chunk_size=1000, min_files_for_chunking=500):
        self.compression_level = compression_level
        self.chunk_size = chunk_size
        self.min_files_for_chunking = min_files_for_chunking
        self.temp_dir = None

    def _compress_direct(self, files, output_path, password=None):
        print(f"Using direct compression for {len(files)} files...")
        
        files = [Path(f) for f in files]
        n_workers = min(16, max(1, multiprocessing.cpu_count() - 1))
        
        def prepare_paths(file_path):
            try:
                rel_path = file_path.relative_to(file_path.anchor)
                return str(file_path), str(rel_path)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
                return None
        
        bag = db.from_sequence(files, npartitions=n_workers)
        
        with ProgressBar():
            print("Preparing file paths...")
            file_pairs = bag.map(prepare_paths).filter(lambda x: x).compute()
        
        print(f"Creating ZIP archive directly: {output_path}")
        
        with pyzipper.AESZipFile(
            output_path,
            "w",
            compression=pyzipper.ZIP_DEFLATED,
            compresslevel=self.compression_level,
            encryption=pyzipper.WZ_AES if password else None,
        ) as zipf:
            if password:
                zipf.setpassword(password.encode())
            
            def add_file_to_zip(file_pair):
                file_path, rel_path = file_pair
                try:
                    with open(file_path, 'rb') as f:
                        content = f.read()
                    return rel_path, content
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
                    return None
            
            chunk_size = 100
            for i in range(0, len(file_pairs), chunk_size):
                chunk = file_pairs[i:i+chunk_size]
                chunk_bag = db.from_sequence(chunk, npartitions=min(len(chunk), n_workers))
                
                with ProgressBar():
                    print(f"Processing files {i+1}-{min(i+chunk_size, len(file_pairs))} of {len(file_pairs)}...")
                    file_contents = chunk_bag.map(add_file_to_zip).filter(lambda x: x).compute()
                
                for rel_path, content in file_contents:
                    zipf.writestr(rel_path, content)
        
        print(f"Direct compression completed: {output_path}")
        return Path(output_path).absolute()

    def _compress_chunked(self, files, output_path, password=None):
        self.temp_dir = tempfile.mkdtemp(prefix="parallel_zip_")
        n_workers = min(16, max(1, multiprocessing.cpu_count() - 1))
        
        try:
            print(f"Using chunked compression for {len(files)} files with {n_workers} workers...")
            
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
            
            print(f"Preparing chunked compression for {len(file_pairs)} files...")
            
            adaptive_chunk_size = min(self.chunk_size, max(100, len(file_pairs) // (n_workers * 2)))
            
            chunks = [
                file_pairs[i : i + adaptive_chunk_size]
                for i in range(0, len(file_pairs), adaptive_chunk_size)
            ]
            
            chunk_data = [
                (chunk, idx, self.temp_dir, self.compression_level, password) 
                for idx, chunk in enumerate(chunks)
            ]
            
            print(f"Compressing in {len(chunks)} parallel chunks...")
            
            chunk_bag = db.from_sequence(chunk_data, npartitions=min(len(chunk_data), n_workers))
            
            with ProgressBar():
                print("Compressing chunks...")
                chunk_files = chunk_bag.map(compress_chunk).compute()
            
            print(f"Merging {len(chunk_files)} chunk files into final zip: '{output_path}'")
            
            with pyzipper.AESZipFile(
                output_path,
                "w",
                compression=pyzipper.ZIP_DEFLATED,
                compresslevel=self.compression_level,
                encryption=pyzipper.WZ_AES if password else None,
            ) as final_zip:
                if password:
                    final_zip.setpassword(password.encode())
                
                batch_size = max(1, len(chunk_files) // n_workers)
                
                for i in range(0, len(chunk_files), batch_size):
                    batch = chunk_files[i:i+batch_size]
                    
                    chunk_bag = db.from_sequence(batch, npartitions=min(len(batch), n_workers))
                    
                    with ProgressBar():
                        print(f"Processing merge batch {i//batch_size + 1}/{(len(chunk_files)-1)//batch_size + 1}...")
                        batch_items = chunk_bag.map(process_chunk_for_merge).flatten().compute()
                    
                    for filename, content in batch_items:
                        final_zip.writestr(filename, content)
            
            print(f"Chunked compression completed: {output_path}")
            return Path(output_path).absolute()
            
        finally:
            if self.temp_dir and os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                self.temp_dir = None

    def compress(self, files, output_path, password=None):
        if len(files) < self.min_files_for_chunking:
            return self._compress_direct(files, output_path, password)
        else:
            return self._compress_chunked(files, output_path, password)