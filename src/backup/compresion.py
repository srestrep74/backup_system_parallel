import os
import shutil
import tempfile
import multiprocessing
from pathlib import Path
import concurrent.futures

import pyzipper
import dask.bag as db
from dask.diagnostics import ProgressBar
from tqdm import tqdm

class ParallelZipCompressor:
    def __init__(self, compression_level=6, chunk_size=1000):
        self.compression_level = compression_level
        self.chunk_size = chunk_size
        self.temp_dir = None

    def _compress_chunk(self, chunk_files, chunk_index, password=None):
        """Compress a chunk of files into a temporary zip file."""
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

    def compress(self, files, output_path, password=None):
        """Compress files in parallel using chunked approach."""
        self.temp_dir = tempfile.mkdtemp(prefix="parallel_zip_")

        try:
            print(f"Processing {len(files)} files...")

            def prepare_paths(file_path):
                try:
                    rel_path = Path(file_path).relative_to(Path(file_path).anchor)
                    return str(file_path), str(rel_path)
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
                    return None

            bag = db.from_sequence(files, npartitions=min(len(files), 16))

            with ProgressBar():
                file_pairs = bag.map(prepare_paths).filter(lambda x: x).compute()

            print(f"Preparing to compress {len(file_pairs)} files...")

            chunks = [
                file_pairs[i : i + self.chunk_size]
                for i in range(0, len(file_pairs), self.chunk_size)
            ]

            print(f"Compressing in {len(chunks)} parallel chunks...")

            max_workers = min(len(chunks), max(1, multiprocessing.cpu_count() - 1))
            chunk_files = []

            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._compress_chunk, chunk, idx, password): idx
                    for idx, chunk in enumerate(chunks)
                }

                with tqdm(total=len(chunks), desc="Compressing chunks") as pbar:
                    for future in concurrent.futures.as_completed(futures):
                        idx = futures[future]
                        try:
                            chunk_files.append(future.result())
                        except Exception as e:
                            print(f"Error in chunk {idx}: {e}")
                        pbar.update(1)

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

                for chunk_file in tqdm(chunk_files, desc="Merging chunks"):
                    with pyzipper.AESZipFile(chunk_file, "r") as chunk_zip:
                        for item in chunk_zip.infolist():
                            final_zip.writestr(item, chunk_zip.read(item.filename))

            print(f"Compression completed: {output_path}")
            return Path(output_path).absolute()
        finally:
            if self.temp_dir and os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                self.temp_dir = None

