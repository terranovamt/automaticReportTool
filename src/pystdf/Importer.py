#
# PySTDF - The Pythonic STDF Parser
# Copyright (C) 2006 Casey Marshall
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
# Modified: 2017 Minh-Hai Nguyen
#

import numpy as np
import pandas as pd
import polars as pl
import gzip
import os
from pystdf.IO import Parser
from pystdf.Writers import TextWriter
from collections import defaultdict


class MemoryWriter:
    def __init__(self):
        self.data = []

    def after_send(self, dataSource, data):
        self.data.append(data)

    def write(self, line):
        self.data.append(line)

    def flush(self):
        pass  # Do nothing


class OptimizedMemoryWriter:
    """Versione ottimizzata che usa defaultdict per accumulo più veloce"""

    def __init__(self):
        # Usa defaultdict di liste per accumulo più efficiente
        self.data_dict = defaultdict(lambda: defaultdict(list))
        self.record_count = 0

    def after_send(self, dataSource, data):
        RecType = data[0].__class__.__name__.upper()
        rec_dict = self.data_dict[RecType]

        # Accumula direttamente in dict per tipo di record
        for field_info, value in zip(data[0].fieldMap, data[1]):
            rec_dict[field_info[0]].append(value)

        self.record_count += 1

    def to_dataframes_polars(self):
        """Converte a Polars DataFrame - molto più veloce di Pandas"""
        result = {}
        print(
            f"\nConverting {len(self.data_dict)} record types to Polars DataFrames...",
            flush=True,
        )

        for rec_type, fields_dict in self.data_dict.items():
            try:
                # Polars crea DataFrame direttamente da dict di liste - molto veloce!
                result[rec_type] = pl.DataFrame(fields_dict)
            except Exception as e:
                print(f"Warning: Could not convert {rec_type} to Polars DataFrame: {e}")
                # Fallback a pandas se necessario
                result[rec_type] = pd.DataFrame(fields_dict)

        return result

    def to_dataframes_pandas(self):
        """Fallback a Pandas DataFrame per compatibilità"""
        result = {}
        for rec_type, fields_dict in self.data_dict.items():
            result[rec_type] = pd.DataFrame(fields_dict)
        return result


def ImportSTDF(fname):
    with open(fname, "rb") as fin:
        p = Parser(inp=fin)
        storage = MemoryWriter()
        p.addSink(storage)
        p.parse()
    return storage.data


def STDF2Text(fname, delimiter="|"):
    """Convert STDF to a list of text representation"""
    with open(fname, "rb") as fin:
        p = Parser(inp=fin)
        storage = MemoryWriter()
        p.addSink(TextWriter(storage, delimiter=delimiter))
        p.parse()
        return storage.data
    return None


def STDF2Dict(fname):
    """Convert STDF to a list of dictionary objects"""
    data = ImportSTDF(fname)
    data_out = []
    for datum in data:
        datum_out = {}
        RecType = datum[0].__class__.__name__.upper()
        datum_out["RecType"] = RecType
        for k, v in zip(datum[0].fieldMap, datum[1]):
            datum_out[k[0]] = v
        data_out.append(datum_out)
    return data_out


def STDF2DataFrame(fname, use_polars=True, optimized=True):
    """Convert STDF to a dictionary of DataFrame objects

    Args:
        fname: Path to STDF file
        use_polars: Use Polars (molto più veloce) invece di Pandas. Default: True
        optimized: Usa versione ottimizzata con accumulo diretto. Default: True

    Returns:
        Dictionary di DataFrame (Polars o Pandas a seconda del parametro)
    """
    if optimized:
        # VERSIONE OTTIMIZZATA - Accumulo diretto senza conversioni intermedie
        with open(fname, "rb") as fin:
            p = Parser(inp=fin)
            storage = OptimizedMemoryWriter()
            p.addSink(storage)
            p.parse()

        # Conversione finale a DataFrame
        if use_polars:
            return storage.to_dataframes_polars()
        else:
            return storage.to_dataframes_pandas()
    else:
        # VERSIONE ORIGINALE - Mantiene compatibilità
        data = ImportSTDF(fname)
        BigTable = {}
        print(f"Create Dataframe\r", end="", flush=True)
        for datum in data:
            RecType = datum[0].__class__.__name__.upper()
            if RecType not in BigTable.keys():
                BigTable[RecType] = {}
            Rec = BigTable[RecType]
            for k, v in zip(datum[0].fieldMap, datum[1]):
                if k[0] not in Rec.keys():
                    Rec[k[0]] = []
                Rec[k[0]].append(v)
        print(f"Return Dataframe\r", end="", flush=True)

        if use_polars:
            # Converti a Polars
            for k, v in BigTable.items():
                BigTable[k] = pl.DataFrame(v)
        else:
            # Usa Pandas (originale)
            for k, v in BigTable.items():
                BigTable[k] = pd.DataFrame(v)
        return BigTable


def STDF2DataFrameFast(fname):
    """Alias per la versione più veloce con Polars ottimizzato"""
    return STDF2DataFrame(fname, use_polars=True, optimized=True)


def open_stdf_file(fname):
    """Apre un file STDF (anche se compresso con gzip) in modo ottimizzato

    Args:
        fname: Path al file STDF (può essere .std, .stdf, .gz, etc.)

    Returns:
        File handle aperto in modalità binaria con buffering ottimizzato
    """
    # Controlla se è un file gzip
    if fname.lower().endswith(".gz"):
        # Per file gzip, usa decompressione ottimizzata
        return gzip.open(fname, "rb")
    else:
        # Per file non compressi, apri con buffering ottimizzato
        return open(fname, "rb", buffering=2 * 1024 * 1024)  # 2MB buffer


def STDF2DataFrameOptimized(fname, use_polars=True):
    """Versione completamente ottimizzata con gestione automatica compressione

    Questa è la versione RACCOMANDATA per massime performance.

    Args:
        fname: Path al file STDF (gestisce automaticamente .gz)
        use_polars: Usa Polars (default: True, raccomandato per performance)

    Returns:
        Dictionary di DataFrame ottimizzati
    """
    with open_stdf_file(fname) as fin:
        p = Parser(inp=fin)
        storage = OptimizedMemoryWriter()
        p.addSink(storage)
        p.parse()

    if use_polars:
        return storage.to_dataframes_polars()
    else:
        return storage.to_dataframes_pandas()


def STDF2ParquetFiles(path_fin, path_fout, use_polars=True, compression="lz4"):
    """Salva ogni tabella STDF come file Parquet separato

    Questa è la versione OTTIMIZZATA che salva direttamente a Parquet.

    Args:
        path_fin: Path al file STDF di input (gestisce .gz automaticamente)
        path_fout: Directory di output dove salvare i file Parquet
        use_polars: Usa Polars per performance massime (default: True)
        compression: Tipo di compressione ('lz4', 'snappy', 'gzip', 'zstd'). Default: 'lz4'

    Returns:
        List di file Parquet creati

    Example:
        >>> created_files = STDF2ParquetFiles('myfile.std.gz', '/output/dir/')
        >>> # Crea: myfile.std.ptr.parquet, myfile.std.prr.parquet, etc.
    """
    import os
    from pathlib import Path

    # Crea directory di output se non esiste
    os.makedirs(path_fout, exist_ok=True)

    # Estrai il nome base del file (senza estensioni .gz, .std, etc.)
    base_name = os.path.basename(path_fin)
    # Rimuovi .gz se presente
    if base_name.lower().endswith(".gz"):
        base_name = base_name[:-3]
    # Mantieni il nome fino a .std/.stdf
    if ".std" in base_name.lower():
        # Trova l'indice di .std o .stdf
        for ext in [".stdf", ".STDF", ".std", ".STD"]:
            if ext in base_name:
                idx = base_name.index(ext)
                base_name = base_name[: idx + len(ext)]
                break

    # print(f"[STDF2Parquet] Parsing {os.path.basename(path_fin)}...")

    # Parsing ottimizzato
    with open_stdf_file(path_fin) as fin:
        p = Parser(inp=fin)
        storage = OptimizedMemoryWriter()
        p.addSink(storage)
        p.parse()

    # print(f"\n[STDF2Parquet] Saving {len(storage.data_dict)} tables to Parquet...")

    created_files = []

    for rec_type, fields_dict in storage.data_dict.items():
        # Nome file: nomefile.std.tabellanome.parquet (tutto minuscolo)
        table_name = rec_type.lower()
        output_filename = f"{base_name}.{table_name}.parquet"
        output_path = os.path.join(path_fout, output_filename)

        try:
            if use_polars:
                # Usa Polars - molto più veloce
                df = pl.DataFrame(fields_dict)
                df.write_parquet(
                    output_path,
                    compression=compression,
                    statistics=False,  # Più veloce senza statistiche
                )
            else:
                # Usa Pandas per compatibilità
                df = pd.DataFrame(fields_dict)
                df.to_parquet(output_path, compression=compression, index=False)

            created_files.append(output_path)

        except Exception as e:
            print(f"Warning: Could not save {table_name}: {e}")

    return created_files
