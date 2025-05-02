# Parallel Backup System

A high-performance backup utility that compresses directories in parallel using multiple CPU cores.

## Features

- Multi-threaded file discovery and compression
- Password-protected ZIP archives
- Automatic CPU core optimization
- Progress visualization
- Handles large file sets through chunked processing

## Installation

### Requirements

- Python 3.8+
- Dependencies:
  - click
  - pyzipper
  - dask
  - pathlib

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/backup_system_parallel.git
cd backup_system_parallel

# Install dependencies
pip install -r requirements.txt
```

## Usage

```bash
python -m src.main [OPTIONS] [FOLDERS]...
```

### Arguments

- `FOLDERS`: One or more directories to back up (required)

### Options

- `-o, --output PATH`: Custom output filename for the backup (default: `backup_YYYYMMDD_HHMMSS.zip`)
- `-p, --password TEXT`: Password to encrypt the archive
- `-w, --workers INTEGER`: Number of worker processes (default: CPU count - 1, max 8)
- `-c, --chunk-size INTEGER`: Size of chunks for parallel compression (default: 1000)

### Examples

```bash
# Basic backup of a single directory
python -m src.main /path/to/documents

# Back up multiple directories with a custom filename
python -m src.main /path/to/documents /path/to/photos -o my_backup.zip

# Create an encrypted backup using 4 worker processes
python -m src.main /path/to/important/files -p mysecretpassword -w 4

# Adjust chunk size for better performance with many small files
python -m src.main /path/to/many/small/files -c 5000
```

## How It Works

### Architecture

The system is built around three main components:

1. **Command-line Interface** (`src/main.py`):
   - Processes user input and coordinates the backup process
   - Uses Click library for argument parsing and command handling

2. **Parallel File Discovery** (`src/utils/file_finder.py`):
   - Scans multiple directories simultaneously
   - Uses Dask for parallel processing and task distribution

3. **Parallel Compression** (`src/backup/compresion.py`):
   - Utilizes a multi-stage compression approach
   - For smaller file sets: Direct compression to ZIP
   - For larger file sets: Chunked compression with parallel merging

### Performance Optimization

The system automatically:
- Selects the optimal number of worker processes based on CPU cores
- Decides between direct or chunked compression based on file count
- Adjusts chunk sizes for optimal memory usage and performance

### Technical Details

- Uses Dask for parallel task distribution and execution
- Uses pyzipper for ZIP compression with AES encryption
- Employs progress bars to visualize the backup process
- Handles relative paths to maintain directory structure in the archive

## License

[MIT License](LICENSE)