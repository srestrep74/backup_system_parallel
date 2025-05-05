from pathlib import Path
import pyzipper
import zipfile

def restore_backup(zip_path: Path, output_dir: Path, password: str = None) -> None:
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
