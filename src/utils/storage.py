import os
import shutil
from pathlib import Path
from typing import Tuple, List
import click
import psutil


def get_connected_devices() -> List[Tuple[str, str]]:
    system_paths = {'C:\\', '/', '/boot', '/mnt', '/media'}
    devices = []

    for part in psutil.disk_partitions(all=False):
        if part.mountpoint not in system_paths and os.path.exists(part.mountpoint):
            fs_type = part.fstype.upper()
            devices.append((part.mountpoint, fs_type))

    return devices


def copy_to_device(src: Path, dest: str) -> Tuple[bool, str]:
    try:
        target = Path(dest) / src.name
        shutil.copy2(src, target)
        return True, str(target)
    except Exception as e:
        return False, str(e)


def is_probably_usb(fs_type: str) -> bool:
    return fs_type in {"FAT32", "EXFAT"}


def storage_menu(file_path: Path) -> None:
    click.echo("\nStorage options:\n1. External Hard Disk\n2. USB Fragmentation")
    choice = click.prompt("Choose an option", type=int)

    devices = get_connected_devices()
    if not devices:
        click.echo("No devices found!")
        return

    usb_devices = [d for d in devices if is_probably_usb(d[1])]
    non_usb_devices = [d for d in devices if not is_probably_usb(d[1])]

    if choice == 1:
        if not non_usb_devices:
            click.echo("No external hard disks detected.")
            return

        click.echo("\nDetected External Hard Disks:")
        for i, (mountpoint, fs_type) in enumerate(non_usb_devices, 1):
            click.echo(f"{i}. {mountpoint} ({fs_type} - Hard Drive)")

        idx = click.prompt("Select device", type=int)
        if 1 <= idx <= len(non_usb_devices):
            mountpoint = non_usb_devices[idx - 1][0]
            ok, msg = copy_to_device(file_path, mountpoint)
            click.echo(f"Success: {msg}" if ok else f"Error: {msg}")

    elif choice == 2:
        if not usb_devices:
            click.echo("No USB devices detected.")
            return

        file_size = file_path.stat().st_size
        click.echo(f"\nTotal file size: {round(file_size / (1024 * 1024), 2)} MB")
        remaining = file_size
        part_index = 1

        click.echo("Assign space for each USB device. Press Enter to store the remaining file size in the device.")

        while remaining > 0:
            click.echo("\nAvailable USB devices:")
            for i, (mountpoint, fs_type) in enumerate(usb_devices, 1):
                click.echo(f"{i}. {mountpoint} ({fs_type})")

            idx = click.prompt("Select device number", type=int)
            if not (1 <= idx <= len(usb_devices)):
                click.echo("Invalid selection. Try again.")
                continue

            mountpoint = usb_devices[idx - 1][0]
            max_mb = round(remaining / (1024 * 1024), 2)
            user_input = click.prompt(f"How many MB to copy to {mountpoint}? (Remaining: {max_mb} MB) or press Enter to use all",
                                      default='', show_default=False)

            if user_input.strip() == '':
                bytes_to_assign = remaining
            else:
                try:
                    mb = float(user_input)
                    if mb <= 0:
                        continue
                    bytes_to_assign = int(min(mb * 1024 * 1024, remaining))
                except ValueError:
                    click.echo("Invalid input. Please enter a number or press Enter.")
                    continue

            part_path = file_path.parent / f"{file_path.stem}.part{part_index:03d}"
            with open(file_path, 'rb') as src:
                src.seek(file_size - remaining)
                with open(part_path, 'wb') as part_file:
                    part_file.write(src.read(bytes_to_assign))

            ok, msg = copy_to_device(part_path, mountpoint)
            click.echo(f"{'Copied' if ok else 'Error'}: {msg}")

            if ok:
                try:
                    part_path.unlink()
                    click.echo(f"Deleted partition file: {part_path.name}")
                except Exception as e:
                    click.echo(f"Error deleting {part_path.name}: {e}")

                remaining -= bytes_to_assign
                part_index += 1
            else:
                click.echo("Copy failed. Try a different device or size.")

    else:
        click.echo("Invalid choice!")
