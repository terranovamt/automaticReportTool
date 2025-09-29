import os
import shutil
import subprocess
import gzip
import py7zr
import zipfile
import tarfile
import jupiter.utility as uty
import polars as pl

debug = False
FILENAME = os.path.abspath("src/run.log")


def rename_files(folder, old_ext, new_ext):
    uty.write_log(f"Rename .csv", FILENAME)
    if not os.path.exists(folder):
        print(f"Error: The folder {folder} does not exist.")
        return []
    renamed_files = []
    for filename in os.listdir(folder):
        if filename.endswith(old_ext):
            base = os.path.splitext(filename)[0]
            new_name = f"{base}{new_ext}"
            os.rename(os.path.join(folder, filename), os.path.join(folder, new_name))
            renamed_files.append(os.path.join(folder, new_name))
    return renamed_files


def convert_files(folder, hex_file, option):
    uty.write_log(f"Extract .csv", FILENAME)
    if not os.path.exists(folder):
        print(f"Error: The folder {folder} does not exist.")
        return
    for filename in os.listdir(folder):
        if filename.endswith(".std"):
            cmd = f'"{hex_file}" "{os.path.join(folder, filename)}" {option}'
            debug and print(cmd)
            subprocess.run(cmd, shell=True)


def move_csv_files(src_folder, dest_folder):
    uty.write_log(f"Convert .csv to Parquet (Polars) and move", FILENAME)
    if not os.path.exists(src_folder):
        print(f"Error: The source folder {src_folder} does not exist.")
        return []
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    converted_files = []
    for filename in os.listdir(src_folder):
        if filename.lower().endswith(".csv"):
            src_path = os.path.join(src_folder, filename)
            base_name = filename[:-4]  # Rimuove l'estensione .csv

            try:
                print(
                    f"[STDF2DATA] Convert {filename}".ljust(150),
                    end="\r",
                    flush=True,
                )
                # Leggi il CSV con Polars preservando i tipi
                df = pl.read_csv(
                    src_path,
                    try_parse_dates=True,  # Gestione automatica delle date
                    infer_schema=False,
                    # ignore_errors=True,  # Ignora errori di parsing
                    null_values=["", "NA", "N/A", "null", "NULL"],  # Valori nulli
                    encoding="utf8",  # Codifica caratteri
                    low_memory=False,  # Massima accuratezza nei tipi
                )
                # Converti in Parquet nella cartella di destinazione
                parquet_path = os.path.join(dest_folder, f"{base_name}.parquet")
                df.write_parquet(
                    parquet_path,
                    compression="lz4",  # Compressione efficiente
                    statistics=False,  # Piu leggero
                )
                converted_files.append(base_name)
                # Rimuovi il file CSV originale solo dopo conversione riuscita
                os.remove(src_path)

            except Exception as e:
                print(f"Errore nella conversione di {filename}: {str(e)}")
                # In caso di errore, il CSV rimane nella cartella originale

    return converted_files


def get_folder_size(folder):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def delete_related_files(csv_folder, std_file_prefix):
    uty.write_log(f"MAX CAHCHE remove old .csv", FILENAME)
    related_files = [f for f in os.listdir(csv_folder) if f.startswith(std_file_prefix)]
    for f in related_files:
        os.remove(os.path.join(csv_folder, f))


def remove_directory_recursive(directory):
    """Rimuove ricorsivamente una directory usando solo os"""
    if not os.path.exists(directory):
        return

    for root, dirs, files in os.walk(directory, topdown=False):
        # Rimuovi tutti i file
        for name in files:
            try:
                os.remove(os.path.join(root, name))
            except:
                pass
        # Rimuovi le directory
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

    if is_compressed:
        # Crea cartella temporanea
        temp_dir = "tmp"

        # Se la cartella esiste, rimuovi file manualmente
        if os.path.exists(temp_dir):
            remove_directory_recursive(temp_dir)
        else:
            os.makedirs(temp_dir)

        temp_compressed = os.path.join(temp_dir, os.path.basename(path_fin))
        shutil.copy2(path_fin, temp_compressed)

        # Estrae il file compresso
        filename = os.path.basename(path_fin).lower()
        try:
            if filename.endswith(".zip"):
                with zipfile.ZipFile(temp_compressed, "r") as zip_ref:
                    zip_ref.extractall(temp_dir)
            elif filename.endswith(".gz"):
                with gzip.open(temp_compressed, "rb") as gz_file:
                    output_filename = os.path.basename(temp_compressed)[:-3]
                    output_path = os.path.join(temp_dir, output_filename)
                    with open(output_path, "wb") as output_file:
                        shutil.copyfileobj(gz_file, output_file)
            elif filename.endswith((".tar", ".tar.gz", ".tar.bz2", ".tar.xz")):
                with tarfile.open(temp_compressed, "r:*") as tar_ref:
                    tar_ref.extractall(temp_dir)
            elif filename.endswith(".7z"):
                if py7zr:
                    with py7zr.SevenZipFile(temp_compressed, mode="r") as z:
                        z.extractall(temp_dir)
                else:
                    subprocess.run(
                        f'7z x "{temp_compressed}" -o"{temp_dir}"',
                        shell=True,
                        stderr=subprocess.DEVNULL,
                    )
            elif filename.endswith(".rar"):
                subprocess.run(
                    f'unrar x "{temp_compressed}" "{temp_dir}\\"',
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )
            elif filename.endswith(".bz2"):
                subprocess.run(f'bzip2 -dk "{temp_compressed}"', shell=True)
            elif filename.endswith(".xz"):
                subprocess.run(f'xz -dk "{temp_compressed}"', shell=True)
        except Exception as e:
            print(f"Errore durante l'estrazione: {e}")

        # Trova il file STDF estratto
        stdf_extensions = (".std", ".stdf", ".STDF")
        for file in os.listdir(temp_dir):
            if file.endswith(stdf_extensions):
                path_fin = os.path.join(temp_dir, file)
                break

    try:
        hex_file = os.path.abspath("src/STDF2CSV.exe")
        cmd = f'"{hex_file}" "{os.path.join(path_fin)}" -t'
        debug and print(cmd)
        subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL)
        cmd = f'"{hex_file}" "{os.path.join(path_fin)}" {option}'
        debug and print(cmd)
        subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL)
        move_csv_files(os.path.dirname(path_fin), os.path.dirname(path_fout))
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


def stdf2csv(stdf_folders, csv_folder, option=""):

    # if  os.path.exists(csv_folder):
    #     shutil.rmtree(csv_folder)

    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)

    csv_name = []

    for stdf_folder in stdf_folders:
        rename_files(stdf_folder, ".stdf", ".std")

        std_files = [f for f in os.listdir(stdf_folder) if f.endswith(".std")]

        existing_csv_files = list(
            set(f[:-8] for f in os.listdir(csv_folder) if f.endswith(".csv"))
        )

        if any(f not in existing_csv_files for f in std_files):
            convert_files(stdf_folder, os.path.abspath("src/STDF2CSV.exe"), option)
            csv_name.append(move_csv_files(stdf_folder, csv_folder))
        else:
            csv_name = std_files

        convert_files(stdf_folder, os.path.abspath("src/STDF2CSV.exe"), option)
        csv_name.append(move_csv_files(stdf_folder, csv_folder))

        # while get_folder_size(csv_folder) > 1 * 1024 * 1024 * 1024:
        #     files = [
        #         (f, os.path.getmtime(os.path.join(csv_folder, f)))
        #         for f in os.listdir(csv_folder)
        #     ]
        #     files.sort(key=lambda x: x[1])

        #     if files:
        #         oldest_file = files[0][0]
        #         std_file_prefix = oldest_file.split(".")[0]
        #         delete_related_files(csv_folder, std_file_prefix)

    return csv_name


if __name__ == "__main__":
    print("\n\n--- REPORT GENERATOR ---")
    debug = True
    stdf_folders = [
        os.path.abspath("./STDF/P6AX86/P6AX86_01/X30"),
        os.path.abspath("./STDF/P6AX86/P6AX86_02/STD"),
        os.path.abspath("./STDF/P6AX86/P6AX86_03/X30"),
        os.path.abspath("./STDF/P6AX86/P6AX86_04/X30"),
        os.path.abspath("./STDF/P6AX86/P6AX86_05/X30"),
        os.path.abspath("./STDF/P6AX86/P6AX86_06/X30"),
        os.path.abspath("./STDF/P6AX86/P6AX86_07/X30"),
    ]
    csv_folder = os.path.abspath("./src/csv")

    memory = stdf2csv(stdf_folders, csv_folder)

    print("\n|-->END\n")
