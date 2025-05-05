import click
from datetime import datetime
from pathlib import Path
import multiprocessing

from .utils.file_finder import FileFinder
from .backup.compresion import ParallelZipCompressor
from .utils.storage import storage_menu
from .backup.drive import upload_to_drive_service, restore_backup_drive
from .backup.local_restore import restore_backup

@click.group()
def cli():
    pass

# ---------------------- BACKUP COMMAND ---------------------- #
@cli.command()
@click.argument('folders', nargs=-1, type=click.Path(exists=True, path_type=Path))
@click.option('--output', '-o', type=click.Path(), default=None, help='Output file name')
@click.option('--password', '-p', type=str, default=None, help='Password for encryption (optional)')
@click.option('--workers', '-w', type=int, default=None, help='Number of processes (default: auto)')
@click.option('--chunk-size', '-c', type=int, default=1000, help='Chunk size for parallel compression')
def backup(folders, output, password, workers, chunk_size):
    if not folders:
        raise click.UsageError('You must specify at least one folder')
    
    if workers is None:
        workers = min(8, max(1, multiprocessing.cpu_count() - 1))
    
    click.echo(f"... Starting backup with {workers} processes and chunk size {chunk_size}...")
    
    try:
        files = FileFinder.find_files_parallel(folders, max_workers=workers)
        if not files:
            raise click.ClickException('No files found in the provided folders')
        
        output_path = output or f'backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
        compressor = ParallelZipCompressor(compression_level=6, chunk_size=chunk_size)
        result_path = compressor.compress(files, output_path, password)

        click.echo(f'✔ Backup completed successfully: {result_path}')
        
        if click.confirm('\nDo you want to save a copy to external storage?'):
            storage_menu(Path(result_path))
        
        if click.confirm('\nDo you want to upload the file to Google Drive (service account)?'):
            folder_id = click.prompt('Enter the folder ID on Drive', type=str)
            config_path = Path('settings.yaml')
            upload_to_drive_service(Path(result_path), folder_id, config_path)

    except Exception as e:
        click.echo(f'X  Error during the backup: {e}', err=True)
        raise

# ---------------------- RESTORE GROUP ---------------------- #
@cli.group()
def restore():
    """Commands to restore backups"""
    pass

# ---- Subcommand: Restore from local file ---- #
@restore.command('local')
@click.option('--zip-path', '-z', required=True, type=click.Path(exists=True, path_type=Path), help='Path to the backup file (.zip, .gz, .bz2)')
@click.option('--output-dir', '-o', required=True, type=click.Path(path_type=Path), help='Directory to restore the contents')
@click.option('--password', '-p', default=None, help='Password if the file is encrypted')
def restore_local(zip_path, output_dir, password):
    try:
        restore_backup(zip_path, output_dir, password)
        click.echo(f"✔ Backup restored to: {output_dir}")
    except Exception as e:
        click.echo(f" X  Error during local restore: {e}", err=True)
        raise

# ---- Subcommand: Restore from Google Drive ---- #
@restore.command('drive')
@click.option('--file-id', '-f', required=True, help='File ID in Google Drive')
@click.option('--config-path', '-c', default='settings.yaml', type=click.Path(exists=True, path_type=Path), help='Path to the authentication configuration file')
@click.option('--output-dir', '-o', required=True, type=click.Path(path_type=Path), help='Directory to restore the contents')
@click.option('--password', '-p', default=None, help='Password if the file is encrypted')
def restore_drive(file_id, config_path, output_dir, password):
    try:
        restore_backup_drive(file_id, Path(config_path), output_dir, password)
        click.echo(f"✔ Backup restored from Drive to: {output_dir}")
    except Exception as e:
        click.echo(f" X  Error during restore from Drive: {e}", err=True)
        raise

if __name__ == '__main__':
    cli()
