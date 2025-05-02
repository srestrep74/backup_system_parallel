import click
from datetime import datetime
from pathlib import Path
import multiprocessing
from .utils.file_finder import FileFinder
from .backup.compresion import ParallelZipCompressor

@click.command()
@click.argument('folders', nargs=-1, type=click.Path(exists=True, path_type=Path))
@click.option('--output', '-o', type=click.Path(), default=None, help='Output file name')
@click.option('--password', '-p', type=str, default=None, help='Password for encryption')
@click.option('--workers', '-w', type=int, default=None, help='Number of worker processes (default: auto)')
@click.option('--chunk-size', '-c', type=int, default=1000, help='Size of chunks for parallel compression')
def main(folders, output, password, workers, chunk_size):
    if not folders:
        raise click.UsageError('At least one folder must be provided')
    
    # Set default workers if not specified
    if workers is None:
        workers = min(8, max(1, multiprocessing.cpu_count() - 1))
    
    click.echo(f"Starting backup with {workers} workers and chunk size {chunk_size}")
    
    try:
        # Use parallel file discovery
        files = FileFinder.find_files_parallel(folders, max_workers=workers)
        if not files:
            raise click.ClickException('No files found in the provided folders')
        
        output_path = output or f'backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
        compressor = ParallelZipCompressor(compression_level=6, chunk_size=chunk_size)
        result_path = compressor.compress(files, output_path, password)

        click.echo(f'Backup completed successfully: {result_path}')
    except Exception as e:
        click.echo(f'An error occurred: {e}', err=True)
        raise

if __name__ == '__main__':
    main()

