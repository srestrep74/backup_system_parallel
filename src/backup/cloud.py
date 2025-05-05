from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pathlib import Path
import pyzipper
import gzip
import bz2
import shutil


def upload_to_drive_service(file_path: Path, folder_id: str, config_path: Path):
    gauth = GoogleAuth(settings_file=str(config_path))
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)

    file = drive.CreateFile({
        'title': file_path.name,
        'parents': [{'id': folder_id}]
    })
    file.SetContentFile(str(file_path))
    file.Upload()

    print("File successfully uploaded to Google Drive:", file['title'])

def authenticate_drive(config_path: Path) -> GoogleDrive:
    gauth = GoogleAuth(settings_file=str(config_path))
    gauth.ServiceAuth()
    return GoogleDrive(gauth)

def download_backup_file(file_id: str, output_path: Path, config_path: Path) -> Path:
    drive = authenticate_drive(config_path)
    file = drive.CreateFile({'id': file_id})
    local_file_path = output_path / file['title']
    file.GetContentFile(str(local_file_path))
    return local_file_path

def decrypt_and_extract_zip(zip_path: Path, output_dir: Path, password: str = None):
    with pyzipper.AESZipFile(zip_path, 'r') as zf:
        if password:
            zf.pwd = password.encode('utf-8')
        zf.extractall(path=output_dir)

def decompress_gzip(gz_path: Path, output_dir: Path):
    output_file = output_dir / gz_path.stem
    with gzip.open(gz_path, 'rb') as f_in, open(output_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def decompress_bzip2(bz_path: Path, output_dir: Path):
    output_file = output_dir / bz_path.stem
    with bz2.open(bz_path, 'rb') as f_in, open(output_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def restore_backup_drive(file_id: str, config_path: Path, output_dir: Path, password: str = None):
    print("[*] Downloading file from Drive...")
    backup_path = download_backup_file(file_id, output_dir, config_path)

    print("[*] File downloaded:", backup_path.name)

    if backup_path.suffix == '.zip':
        print("[*] Extracting ZIP file...")
        decrypt_and_extract_zip(backup_path, output_dir, password)
    elif backup_path.suffix == '.gz':
        print("[*] Extracting GZIP file...")
        decompress_gzip(backup_path, output_dir)
    elif backup_path.suffix == '.bz2':
        print("[*] Extracting BZIP2 file...")
        decompress_bzip2(backup_path, output_dir)
    else:
        print("[!] Unrecognized or unsupported file format:", backup_path.suffix)

    print("[✔] Restoration completed.")
