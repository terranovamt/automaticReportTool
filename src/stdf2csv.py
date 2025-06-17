import os
import shutil
import subprocess
import jupiter.utility as uty
import json
import datetime

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
    uty.write_log(f"Move .csv", FILENAME)
    if not os.path.exists(src_folder):
        print(f"Error: The source folder {src_folder} does not exist.")
        return []
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    csv_name = ""
    for filename in os.listdir(src_folder):
        if filename.endswith(".csv"):
            shutil.move(
                os.path.join(src_folder, filename), os.path.join(dest_folder, filename)
            )
            csv_name = filename[:-8]
    return csv_name


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

def stdf2csv_converter(path_fin, path_fout, option=""):
    hex_file = os.path.abspath("src/STDF2CSV.exe")
    cmd = f'"{hex_file}" "{os.path.join(path_fin)}" -t'
    debug and print(cmd)
    subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL)
    cmd = f'"{hex_file}" "{os.path.join(path_fin)}" {option}'
    debug and print(cmd)
    subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL)
    move_csv_files(os.path.dirname(path_fin), os.path.dirname(path_fout))


def stdf2csv(stdf_folders, csv_folder, option=""):

    # if  os.path.exists(csv_folder):
    #     shutil.rmtree(csv_folder)

    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)

    csv_name = []

    for stdf_folder in stdf_folders:
        rename_files(stdf_folder, ".stdf", ".std")

        std_files = [f for f in os.listdir(stdf_folder) if f.endswith('.std')]

        existing_csv_files = list(set(f[:-8] for f in os.listdir(csv_folder) if f.endswith('.csv')))

        if any(f not in existing_csv_files for f in std_files):
            convert_files(stdf_folder, os.path.abspath("src/STDF2CSV.exe"),option)
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
