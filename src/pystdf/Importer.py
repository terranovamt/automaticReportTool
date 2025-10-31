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
from typing import Dict, List, Any

class MemoryWriter:
    def __init__(self):
        self.data = []
    def after_send(self, dataSource, data):
        self.data.append(data)
    def write(self,line):
        self.data.append(line)
    def flush(self):
        pass # Do nothing

class UltraFastMemoryWriter:
    """Versione ultra-ottimizzata con pre-allocazione e batching

    Ottimizzazioni:
    - Batching: accumula record in batch prima di appendere
    - Riduzione overhead: elimina operazioni non necessarie
    - Minimal conditionals: meno controlli nel hot path
    """
    def __init__(self, batch_size=1000):
        self.data_dict = defaultdict(lambda: defaultdict(list))
        self.record_count = 0
        self.batch_size = batch_size
        self.batch_data = defaultdict(lambda: defaultdict(list))
        self.batch_counts = defaultdict(int)

    def after_send(self, dataSource, data):
        RecType = data[0].__class__.__name__.upper()
        batch_dict = self.batch_data[RecType]

        # Accumula nel batch locale
        field_map = data[0].fieldMap
        fields = data[1]
        for i in range(len(field_map)):
            batch_dict[field_map[i][0]].append(fields[i])

        self.batch_counts[RecType] += 1
        self.record_count += 1

        # Flush batch when it reaches target size
        if self.batch_counts[RecType] >= self.batch_size:
            self._flush_batch(RecType)

        # Periodic feedback every 100000 records
        if self.record_count % 100000 == 0:
            print(f"Processed {self.record_count} records\r", end='', flush=True)

    def _flush_batch(self, RecType):
        """Trasferisce il batch al data_dict principale"""
        batch_dict = self.batch_data[RecType]
        main_dict = self.data_dict[RecType]

        for field_name, values in batch_dict.items():
            main_dict[field_name].extend(values)

        # Reset batch
        self.batch_data[RecType] = defaultdict(list)
        self.batch_counts[RecType] = 0

    def _flush_all_batches(self):
        """Flush all remaining batches"""
        for RecType in list(self.batch_data.keys()):
            if self.batch_counts[RecType] > 0:
                self._flush_batch(RecType)

    def to_dataframes_polars(self):
        """Convert to Polars DataFrame - much faster than Pandas"""
        self._flush_all_batches()
        result = {}
        print(f"\nConverting {len(self.data_dict)} record types to Polars DataFrames...", flush=True)

        for rec_type, fields_dict in self.data_dict.items():
            try:
                result[rec_type] = pl.DataFrame(fields_dict)
            except Exception as e:
                print(f"Warning: Could not convert {rec_type} to Polars DataFrame: {e}")
                result[rec_type] = pd.DataFrame(fields_dict)

        return result

    def to_dataframes_pandas(self):
        """Fallback a Pandas DataFrame per compatibilità"""
        self._flush_all_batches()
        result = {}
        for rec_type, fields_dict in self.data_dict.items():
            result[rec_type] = pd.DataFrame(fields_dict)
        return result

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
        # Ottimizzazione: evita zip creando tuple, accedi direttamente agli indici
        field_map = data[0].fieldMap
        fields = data[1]
        for i in range(len(field_map)):
            rec_dict[field_map[i][0]].append(fields[i])

        self.record_count += 1

        # Periodic feedback every 50000 records (reduced print overhead)
        if self.record_count % 50000 == 0:
            print(f"Processed {self.record_count} records\r", end='', flush=True)

    def to_dataframes_polars(self):
        """Convert to Polars DataFrame - much faster than Pandas"""
        result = {}
        print(f"\nConverting {len(self.data_dict)} record types to Polars DataFrames...", flush=True)

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
    with open(fname,'rb') as fin:
        p = Parser(inp=fin)
        storage = MemoryWriter()
        p.addSink(storage)
        p.parse()
    return storage.data

def STDF2Text(fname,delimiter='|'):
    """ Convert STDF to a list of text representation
    """
    with open(fname,'rb') as fin:
        p = Parser(inp=fin)
        storage = MemoryWriter()
        p.addSink(TextWriter(storage,delimiter=delimiter))
        p.parse()
        return storage.data
    return None

def STDF2Dict(fname):
    """ Convert STDF to a list of dictionary objects
    """
    data = ImportSTDF(fname)
    data_out = []
    for datum in data:
        datum_out = {}
        RecType = datum[0].__class__.__name__.upper()
        datum_out['RecType'] = RecType
        for k,v in zip(datum[0].fieldMap,datum[1]):
            datum_out[k[0]] = v
        data_out.append(datum_out)
    return data_out

def STDF2DataFrame(fname, use_polars=True, optimized=True):
    """ Convert STDF to a dictionary of DataFrame objects

    Args:
        fname: Path to STDF file
        use_polars: Use Polars (much faster) instead of Pandas. Default: True
        optimized: Use optimized version with direct accumulation. Default: True

    Returns:
        Dictionary of DataFrames (Polars or Pandas depending on parameter)
    """
    if optimized:
        # OPTIMIZED VERSION - Direct accumulation without intermediate conversions
        with open(fname, 'rb') as fin:
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
        print(f"Create Dataframe\r", end='', flush=True)
        for datum in data:
            RecType = datum[0].__class__.__name__.upper()
            if RecType not in BigTable.keys():
                BigTable[RecType] = {}
            Rec = BigTable[RecType]
            for k,v in zip(datum[0].fieldMap,datum[1]):
                if k[0] not in Rec.keys():
                    Rec[k[0]] = []
                Rec[k[0]].append(v)
        print(f"Return Dataframe\r", end='', flush=True)

        if use_polars:
            # Converti a Polars
            for k,v in BigTable.items():
                BigTable[k] = pl.DataFrame(v)
        else:
            # Usa Pandas (originale)
            for k,v in BigTable.items():
                BigTable[k] = pd.DataFrame(v)
        return BigTable

def STDF2DataFrameFast(fname):
    """Alias per la versione più veloce con Polars ottimizzato"""
    return STDF2DataFrame(fname, use_polars=True, optimized=True)

def STDF2DataFrameUltraFast(fname, batch_size=1000):
    """Versione ULTRA-VELOCE con batching e ottimizzazioni massime

    This is the MAXIMALLY OPTIMIZED version for speed.
    Uses batching, 4MB buffering, and optimized accumulation.

    Args:
        fname: Path to STDF file (automatically handles .gz)
        batch_size: Batch size for accumulation (default: 1000)

    Returns:
        Dictionary of optimized Polars DataFrames
    """
    with open_stdf_file(fname) as fin:
        p = Parser(inp=fin)
        storage = UltraFastMemoryWriter(batch_size=batch_size)
        p.addSink(storage)
        p.parse()

    return storage.to_dataframes_polars()

def open_stdf_file(fname):
    """Opens an STDF file (even if compressed with gzip) in optimized mode

    Args:
        fname: Path to STDF file (can be .std, .stdf, .gz, etc.)

    Returns:
        File handle opened in binary mode with optimized buffering
    """
    # Controlla se è un file gzip
    if fname.lower().endswith('.gz'):
        # Per file gzip, usa decompressione ottimizzata
        return gzip.open(fname, 'rb')
    else:
        # Per file non compressi, apri con buffering ottimizzato (4MB per massime performance)
        return open(fname, 'rb', buffering=4*1024*1024)  # 4MB buffer

def STDF2DataFrameOptimized(fname, use_polars=True):
    """Versione completamente ottimizzata con gestione automatica compressione

    This is the RECOMMENDED version for maximum performance.

    Args:
        fname: Path to STDF file (automatically handles .gz)
        use_polars: Use Polars (default: True, recommended for performance)

    Returns:
        Dictionary of optimized DataFrames
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

def STDF2ParquetFiles(path_fin, path_fout, use_polars=True, compression='lz4', ultra_fast=True, batch_size=1000):
    """Salva ogni tabella STDF come file Parquet separato

    This is the ULTRA-OPTIMIZED version that saves directly to Parquet.

    Args:
        path_fin: Path to input STDF file (automatically handles .gz)
        path_fout: Output directory where to save Parquet files
        use_polars: Use Polars for maximum performance (default: True)
        compression: Tipo di compressione ('lz4', 'snappy', 'gzip', 'zstd'). Default: 'lz4'
        ultra_fast: Usa UltraFastMemoryWriter con batching (default: True)
        batch_size: Dimensione batch per ultra_fast mode (default: 1000)

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
    if base_name.lower().endswith('.gz'):
        base_name = base_name[:-3]
    # Mantieni il nome fino a .std/.stdf
    if '.std' in base_name.lower():
        # Trova l'indice di .std o .stdf
        for ext in ['.stdf', '.STDF', '.std', '.STD']:
            if ext in base_name:
                idx = base_name.index(ext)
                base_name = base_name[:idx + len(ext)]
                break

    print(f"[STDF2Parquet] Parsing {os.path.basename(path_fin)}...")

    # Parsing ultra-ottimizzato con batching
    with open_stdf_file(path_fin) as fin:
        p = Parser(inp=fin)
        if ultra_fast:
            storage = UltraFastMemoryWriter(batch_size=batch_size)
        else:
            storage = OptimizedMemoryWriter()
        p.addSink(storage)
        p.parse()

    # Flush batch rimanenti se ultra_fast
    if ultra_fast:
        storage._flush_all_batches()

    print(f"\n[STDF2Parquet] Saving {len(storage.data_dict)} tables to Parquet...")

    created_files = []

    for rec_type, fields_dict in storage.data_dict.items():
        # Filename: filename.std.tablename.parquet (all lowercase)
        table_name = rec_type.lower()
        output_filename = f"{base_name}.{table_name}.parquet"
        output_path = os.path.join(path_fout, output_filename)

        try:
            if use_polars:
                # Use Polars - much faster
                df = pl.DataFrame(fields_dict)
                df.write_parquet(
                    output_path,
                    compression=compression,
                    statistics=False  # Più veloce senza statistiche
                )
            else:
                # Usa Pandas per compatibilità
                df = pd.DataFrame(fields_dict)
                df.to_parquet(
                    output_path,
                    compression=compression,
                    index=False
                )

            created_files.append(output_path)
            print(f"   ✅ Saved: {output_filename} ({len(df):,} records)", end='\r', flush=True)

        except Exception as e:
            print(f"\n   ⚠️  Warning: Could not save {table_name}: {e}")

    print(f"\n[STDF2Parquet] ✅ Completed! Created {len(created_files)} Parquet files")

    return created_files
