"""
ART.stdf - STDF to Data Conversion Module

This module handles the conversion of binary STDF (Standard Test Data Format) files
into efficient columnar Parquet format using Polars DataFrames. It supports various
compression formats and decompression methods.

Key Features:
    - Automatic decompression of .gz, .7z, .zip, .bz2, .xz, .tar, .rar formats
    - Conversion to Apache Parquet columnar storage
    - Support for large files through streaming
    - Efficient memory usage with Polars

Supported File Formats:
    Input:  .std, .std.gz, .std.7z, .std.zip, .std.bz2, .std.xz, .std.tar, .std.rar
    Output: .parquet files (one per STDF record type)

Main Function:
    stdf2data_converter(): Convert STDF file to multiple Parquet files

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import os
import shutil
import subprocess
import gzip
import py7zr
import zipfile
import tarfile

from pystdf.Importer import STDF2ParquetFiles


def remove_directory_recursive(directory):
    """Recursively remove directory and all contents."""
    if not os.path.exists(directory):
        return

    for root, dirs, files in os.walk(directory, topdown=False):
        for name in files:
            try:
                os.remove(os.path.join(root, name))
            except:
                pass
        for name in dirs:
            try:
                os.rmdir(os.path.join(root, name))
            except:
                pass


def stdf2data_converter(path_fin, path_fout, option=""):
    temp_dir = None

    # Verifica se il file è compresso
    compression_exts = [".gz", ".7z", ".zip", ".bz2", ".xz", ".tar", ".rar"]
    is_compressed = any(path_fin.lower().endswith(ext) for ext in compression_exts)

    # Crea cartella temporanea
    temp_dir = "tmp"

    # Se la cartella esiste, rimuovi file manualmente
    if os.path.exists(temp_dir):
        remove_directory_recursive(temp_dir)
    else:
        os.makedirs(temp_dir)

    # Copia il file in tmp (compresso o meno)
    temp_file = os.path.join(temp_dir, os.path.basename(path_fin))
    shutil.copy2(path_fin, temp_file)

    if is_compressed:
        # Estrae il file compresso
        filename = os.path.basename(path_fin).lower()
        try:
            if filename.endswith(".zip"):
                with zipfile.ZipFile(temp_file, "r") as zip_ref:
                    zip_ref.extractall(temp_dir)
            elif filename.endswith(".gz"):
                with gzip.open(temp_file, "rb") as gz_file:
                    output_filename = os.path.basename(temp_file)[:-3]
                    output_path = os.path.join(temp_dir, output_filename)
                    with open(output_path, "wb") as output_file:
                        shutil.copyfileobj(gz_file, output_file)
            elif filename.endswith((".tar", ".tar.gz", ".tar.bz2", ".tar.xz")):
                with tarfile.open(temp_file, "r:*") as tar_ref:
                    tar_ref.extractall(temp_dir)
            elif filename.endswith(".7z"):
                if py7zr:
                    with py7zr.SevenZipFile(temp_file, mode="r") as z:
                        z.extractall(temp_dir)
                else:
                    subprocess.run(
                        f'7z x "{temp_file}" -o"{temp_dir}"',
                        shell=True,
                        stderr=subprocess.DEVNULL,
                    )
            elif filename.endswith(".rar"):
                subprocess.run(
                    f'unrar x "{temp_file}" "{temp_dir}\\"',
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )
            elif filename.endswith(".bz2"):
                subprocess.run(f'bzip2 -dk "{temp_file}"', shell=True)
            elif filename.endswith(".xz"):
                subprocess.run(f'xz -dk "{temp_file}"', shell=True)
        except Exception as e:
            print(f"Errore durante l'estrazione: {e}")

        # Trova il file STDF estratto
        stdf_extensions = (".std", ".stdf", ".STDF")
        for file in os.listdir(temp_dir):
            if file.endswith(stdf_extensions):
                path_fin = os.path.join(temp_dir, file)
                break
    else:
        # Se non compresso, comprimi in .gz
        original_filename = os.path.basename(path_fin)
        compressed_filename = original_filename + ".gz"
        compressed_path = os.path.join(os.path.dirname(path_fin), compressed_filename)

        # Comprimi il file originale
        with open(path_fin, "rb") as f_in:
            with gzip.open(compressed_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Copia il file compresso in tmp
        temp_compressed = os.path.join(temp_dir, compressed_filename)
        shutil.copy2(compressed_path, temp_compressed)

        # Cancella il file originale
        os.remove(path_fin)

        # Usa il nuovo file compresso come path_fin
        path_fin = compressed_path

    try:
        # Ultra-optimized STDF to Parquet conversion with Polars
        created_files = STDF2ParquetFiles(
            path_fin,
            path_fout,
            use_polars=True,
            compression='lz4',
            ultra_fast=True,
            batch_size=1000
        )
    finally:
        # Pulisce la cartella temporanea
        if temp_dir and os.path.exists(temp_dir):
            try:
                remove_directory_recursive(temp_dir)

            except PermissionError:
                # Su Windows, prova comando rmdir forzato
                import platform

                if platform.system() == "Windows":
                    subprocess.run(
                        f'rmdir /s /q "{temp_dir}"',
                        shell=True,
                        stderr=subprocess.DEVNULL,
                    )
                else:
                    subprocess.run(
                        f'rm -rf "{temp_dir}"', shell=True, stderr=subprocess.DEVNULL
                    )


