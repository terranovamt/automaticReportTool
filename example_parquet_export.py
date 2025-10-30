#!/usr/bin/env python3
"""
Esempio di utilizzo della funzione STDF2ParquetFiles

Questa funzione salva ogni tabella STDF come file Parquet separato.
Formato nome: nomefile.std.tabellanome.parquet (tutto minuscolo)
"""

import sys
import os

# Aggiungi src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from pystdf.Importer import STDF2ParquetFiles


def main():
    """Esempio di utilizzo"""

    # Esempio 1: File STDF compresso
    print("="*70)
    print("Esempio 1: File STDF compresso (.gz)")
    print("="*70)

    path_fin = "myfile.std.gz"  # Il tuo file STDF
    path_fout = "output/parquet"  # Directory di output

    print(f"\nInput:  {path_fin}")
    print(f"Output: {path_fout}/")
    print("\nFile che verranno creati:")
    print("  - myfile.std.ptr.parquet   (Parametric Test Records)")
    print("  - myfile.std.prr.parquet   (Part Result Records)")
    print("  - myfile.std.tsr.parquet   (Test Synopsis Records)")
    print("  - myfile.std.mir.parquet   (Master Information Record)")
    print("  - ... e altre tabelle presenti nel file STDF")

    # Decomment per eseguire:
    # created_files = STDF2ParquetFiles(path_fin, path_fout)
    # print(f"\n✅ Creati {len(created_files)} file Parquet!")

    # Esempio 2: File STDF non compresso
    print("\n" + "="*70)
    print("Esempio 2: File STDF non compresso")
    print("="*70)

    path_fin = "test.stdf"
    path_fout = "output"

    print(f"\nInput:  {path_fin}")
    print(f"Output: {path_fout}/")
    print("\nComando:")
    print(f"  created_files = STDF2ParquetFiles('{path_fin}', '{path_fout}')")

    # Esempio 3: Con opzioni personalizzate
    print("\n" + "="*70)
    print("Esempio 3: Con compressione personalizzata")
    print("="*70)

    print("""
# Usa compressione Zstandard per migliore compressione
created_files = STDF2ParquetFiles(
    path_fin='bigfile.std.gz',
    path_fout='output',
    use_polars=True,      # Usa Polars (raccomandato)
    compression='zstd'    # Zstandard: migliore compressione
)

# Altre opzioni compressione:
# - 'lz4'    : Veloce (default)
# - 'snappy' : Bilanciato
# - 'gzip'   : Alta compressione ma lento
# - 'zstd'   : Ottima compressione
""")

    # Esempio 4: Uso in pipeline
    print("="*70)
    print("Esempio 4: Integrazione in pipeline esistente")
    print("="*70)

    print("""
# Nel tuo codice stdf2data.py è già integrato!

from stdf2data import stdf2data_converter

# Questa funzione ora usa automaticamente STDF2ParquetFiles
stdf2data_converter(
    path_fin='myfile.std.gz',
    path_fout='output/dir'
)

# Risultato: file Parquet ottimizzati salvati direttamente!
""")

    print("\n" + "="*70)
    print("Per usare realmente la funzione, decomenta le righe negli esempi sopra!")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()
