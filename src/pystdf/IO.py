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

import sys
import struct
import re
import io

from pystdf.Types import *
from pystdf import V4
from pystdf.Pipeline import DataSource

# Pre-compilazione regex per performance
ARRAY_PATTERN = re.compile(r"k(\d+)([A-Z][a-z0-9]+)")


def appendFieldParser(fn, action):
    """Append a field parsing function to a record parsing function.
    This is used to build record parsing functions based on the record type specification.
    """

    def newRecordParser(*args):
        fields = fn(*args)
        try:
            fields.append(action(*args))
        except EndOfRecordException:
            pass
        return fields

    return newRecordParser


class RecordHeader:
    """Ottimizzato con __slots__ per ridurre memoria"""

    __slots__ = ("len", "typ", "sub")

    def __init__(self):
        self.len = 0
        self.typ = 0
        self.sub = 0


class Parser(DataSource):

    def _get_struct(self, fmt):
        """Cache di struct.Struct pre-compilati per massima performance"""
        key = self.endian + fmt
        if key not in self._struct_cache:
            self._struct_cache[key] = struct.Struct(key)
        return self._struct_cache[key]

    def readAndUnpack(self, header, fmt):
        # Usa size pre-calcolato se disponibile
        size = self._format_sizes.get(fmt)
        if size is None:
            size = struct.calcsize(fmt)

        if size > header.len:
            self.inp.read(header.len)
            header.len = 0
            raise EndOfRecordException()

        buf = self.inp.read(size)
        if not buf:
            self.eof = 1
            raise EofException()

        header.len -= size

        # Usa struct pre-compilato
        s = self._get_struct(fmt)
        val = s.unpack(buf)[0]

        return val.decode("ascii") if isinstance(val, bytes) else val

    def readAndUnpackDirect(self, fmt):
        size = self._format_sizes.get(fmt) or struct.calcsize(fmt)
        buf = self.inp.read(size)
        if not buf:
            self.eof = 1
            raise EofException()

        # Usa struct pre-compilato
        s = self._get_struct(fmt)
        return s.unpack(buf)[0]

    def readField(self, header, stdfFmt):
        fmt = packFormatMap[stdfFmt]
        return self.readAndUnpack(header, fmt)

    def readFieldDirect(self, stdfFmt):
        fmt = packFormatMap[stdfFmt]
        return self.readAndUnpackDirect(fmt)

    def readCn(self, header):
        """Ottimizzato: decode diretto senza struct.unpack"""
        if header.len == 0:
            raise EndOfRecordException()

        slen = self.readField(header, "U1")

        if slen > header.len:
            self.inp.read(header.len)
            header.len = 0
            raise EndOfRecordException()

        if slen == 0:
            return ""

        buf = self.inp.read(slen)
        if not buf:
            self.eof = 1
            raise EofException()

        header.len -= slen  # Usa slen invece di len(buf)
        return buf.decode("latin1")  # Decode diretto

    def readBn(self, header):
        """Ottimizzato: pre-allocazione lista"""
        blen = self.readField(header, "U1")
        result = [None] * blen
        for i in range(blen):
            result[i] = self.readField(header, "B1")
        return result

    def readDn(self, header):
        """Ottimizzato: pre-allocazione lista"""
        dbitlen = self.readField(header, "U2")
        dlen = dbitlen // 8 + (1 if dbitlen % 8 > 0 else 0)
        dlen_int = int(dlen)
        result = [None] * dlen_int
        for i in range(dlen_int):
            result[i] = self.readField(header, "B1")
        return result

    def readVn(self, header):
        """Ottimizzato: pre-allocazione e cache vnMap"""
        vlen = self.readField(header, "U2")
        vn = []  # Non pre-allocabile perché dipende da fldtype
        vnMap = self.vnMap  # Cache locale per evitare lookup ripetuti
        for _ in range(vlen):
            fldtype = self.readField(header, "B1")
            if fldtype in vnMap:
                vn.append(vnMap[fldtype](header))
        return vn

    def readArray(self, header, indexValue, stdfFmt):
        """Ottimizzato: cache della funzione di parsing + pre-allocazione liste"""
        if stdfFmt == "N1":
            self.readArray(header, indexValue // 2 + indexValue % 2, "U1")
            return

        parse_fn = self.unpackMap[stdfFmt]  # Lookup una volta sola

        # Pre-alloca la lista per performance migliori
        count = int(indexValue)
        result = [None] * count
        for i in range(count):
            result[i] = parse_fn(header, stdfFmt)
        return result

    def readHeader(self):
        """Ottimizzato: legge tutto in un colpo solo"""
        buf = self.inp.read(4)  # U2+U1+U1 = 4 bytes
        if not buf or len(buf) < 4:
            self.eof = 1
            raise EofException()

        hdr = RecordHeader()
        # Usa struct pre-compilato
        s = self._get_struct("HBB")
        hdr.len, hdr.typ, hdr.sub = s.unpack(buf)
        return hdr

    def __detectEndian(self):
        self.eof = 0
        header = self.readHeader()
        if header.typ != 0 or header.sub != 10:
            raise InitialSequenceException()
        cpuType = self.readFieldDirect("U1")
        if self.reopen_fn:
            self.inp = self.reopen_fn()
        else:
            self.inp.seek(0)
        return "<" if cpuType == 2 else ">"

    def header(self, header):
        pass

    def parse_records(self, count=0):
        i = 0
        self.eof = 0
        self._part_id_counter = 0
        self._site_to_part_id = {}
        self._seen_sites_in_part = set()
        self._pir_count_in_part = 0
        self._expected_sites = None

        # Cache for performance
        recordMap = self.recordMap
        recordParsers = self.recordParsers
        send = self.send
        inp = self.inp

        # Pre-identify record types that need PART_ID
        pir_keys = set()
        prr_keys = set()
        test_keys = set()
        for key, recType in recordMap.items():
            if isinstance(recType, V4.Pir):
                pir_keys.add(key)
            elif isinstance(recType, V4.Prr):
                prr_keys.add(key)
            elif isinstance(recType, (V4.Ptr, V4.Mpr, V4.Ftr)):
                test_keys.add(key)

        try:
            while self.eof == 0:
                header = self.readHeader()
                self.header(header)
                key = (header.typ, header.sub)

                # Use try/except instead of 'in' check for better performance
                try:
                    recType = recordMap[key]
                    recParser = recordParsers[key]
                    fields = recParser(self, header, [])

                    if key in pir_keys:
                        site_num = fields[1] if len(fields) > 1 else 0

                        # Detect new part
                        is_new_part = site_num in self._seen_sites_in_part or (
                            self._expected_sites
                            and len(self._seen_sites_in_part) >= self._expected_sites
                        )

                        if is_new_part:
                            self._part_id_counter += 1
                            self._seen_sites_in_part.clear()
                            self._site_to_part_id.clear()
                            self._pir_count_in_part = 0
                            print(
                                f"PART_ID: {self._part_id_counter}" + " " * 60,
                                end="\r",
                                flush=True,
                            )
                        elif self._part_id_counter == 0:
                            self._part_id_counter = 1

                        self._seen_sites_in_part.add(site_num)
                        self._site_to_part_id[site_num] = self._part_id_counter
                        self._pir_count_in_part += 1

                        if self._expected_sites is None and self._pir_count_in_part > 1:
                            self._expected_sites = self._pir_count_in_part

                        fields.append(self._part_id_counter)

                    elif key in prr_keys:
                        # Add PART_ID to PRR records
                        site_num = fields[2] if len(fields) > 2 else 0
                        part_id = self._site_to_part_id.get(
                            site_num, self._part_id_counter
                        )
                        fields.append(part_id)

                    elif key in test_keys:
                        # Add PART_ID to PTR, FTR, MPR records
                        site_num = fields[1] if len(fields) > 1 else 0
                        part_id = self._site_to_part_id.get(
                            site_num, self._part_id_counter
                        )
                        fields.append(part_id)

                    if len(fields) < len(recType.columnNames):
                        fields.extend([None] * (len(recType.columnNames) - len(fields)))

                    send((recType, fields))

                    if header.len > 0:
                        inp.read(header.len)
                        header.len = 0

                except KeyError:
                    # Record type non riconosciuto
                    inp.read(header.len)

                if count:
                    i += 1
                    if i >= count:
                        break
        except EofException:
            pass

    def auto_detect_endian(self):
        if self.inp.tell() == 0:
            self.endian = "@"
            self.endian = self.__detectEndian()

    def parse(self, count=0):
        self.begin()
        try:
            self.auto_detect_endian()
            self.parse_records(count)
            self.complete()
        except Exception as exception:
            self.cancel(exception)
            raise

    def getFieldParser(self, fieldType):
        if fieldType.startswith("k"):
            match = ARRAY_PATTERN.match(fieldType)
            fieldIndex, arrayFmt = match.groups()
            return lambda self, header, fields: self.readArray(
                header, fields[int(fieldIndex)], arrayFmt
            )
        else:
            parseFn = self.unpackMap[fieldType]
            return lambda self, header, fields: parseFn(header, fieldType)

    def createRecordParser(self, recType):
        fn = lambda self, header, fields: fields
        for stdfType in recType.fieldStdfTypes:
            fn = appendFieldParser(fn, self.getFieldParser(stdfType))
        return fn

    def __init__(self, recTypes=V4.records, inp=sys.stdin, reopen_fn=None, endian=None):
        DataSource.__init__(self, ["header"])
        self.eof = 1
        self.recTypes = set(recTypes)
        self.reopen_fn = reopen_fn
        self.endian = endian
        self._part_id_counter = 0

        # Cache per struct pre-compilati (OTTIMIZZAZIONE CRITICA)
        self._struct_cache = {}

        # Pre-calcola i size dei formati comuni (OTTIMIZZAZIONE CRITICA)
        self._format_sizes = {}
        for stdf_type in [
            "C1",
            "B1",
            "U1",
            "U2",
            "U4",
            "U8",
            "I1",
            "I2",
            "I4",
            "I8",
            "R4",
            "R8",
        ]:
            if stdf_type in packFormatMap:
                fmt = packFormatMap[stdf_type]
                self._format_sizes[fmt] = struct.calcsize(fmt)

        # Aggiungi size per readHeader
        self._format_sizes["HBB"] = 4

        # Buffering ottimizzato per I/O - usa 2MB invece di 65KB per file grandi
        # 2MB è un ottimo compromesso tra memoria e performance I/O
        if hasattr(inp, "read"):
            self.inp = (
                io.BufferedReader(inp, buffer_size=2*1024*1024)  # 2MB buffer
                if not isinstance(inp, io.BufferedReader)
                else inp
            )
        else:
            self.inp = inp

        self.recordMap = {(recType.typ, recType.sub): recType for recType in recTypes}

        self.unpackMap = {
            "C1": self.readField,
            "B1": self.readField,
            "U1": self.readField,
            "U2": self.readField,
            "U4": self.readField,
            "U8": self.readField,
            "I1": self.readField,
            "I2": self.readField,
            "I4": self.readField,
            "I8": self.readField,
            "R4": self.readField,
            "R8": self.readField,
            "Cn": lambda header, fmt: self.readCn(header),
            "Bn": lambda header, fmt: self.readBn(header),
            "Dn": lambda header, fmt: self.readDn(header),
            "Vn": lambda header, fmt: self.readVn(header),
        }

        self.recordParsers = {
            (recType.typ, recType.sub): self.createRecordParser(recType)
            for recType in recTypes
        }

        self.vnMap = {
            0: lambda header: self.inp.read(1),
            1: lambda header: self.readField(header, "U1"),
            2: lambda header: self.readField(header, "U2"),
            3: lambda header: self.readField(header, "U4"),
            4: lambda header: self.readField(header, "I1"),
            5: lambda header: self.readField(header, "I2"),
            6: lambda header: self.readField(header, "I4"),
            7: lambda header: self.readField(header, "R4"),
            8: lambda header: self.readField(header, "R8"),
            10: lambda header: self.readCn(header),
            11: lambda header: self.readBn(header),
            12: lambda header: self.readDn(header),
            13: lambda header: self.readField(header, "U1"),
        }
