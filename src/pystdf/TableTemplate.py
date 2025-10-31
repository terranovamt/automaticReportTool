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

class TableTemplate(object):
    """
    Base template class for STDF record types.

    This class provides a simple structure to define column names and types
    for STDF record tables. It's used as a base class for RecordType and
    UnknownRecord in the pystdf library.

    Attributes:
        name (str): Name of the table/record type
        columnNames (list): List of column names
        columnTypes (list): List of column types
    """

    def __init__(self, columnNames, columnTypes, name=None):
        """
        Initialize a TableTemplate.

        Args:
            columnNames (list): List of column names
            columnTypes (list): List of corresponding column types
            name (str, optional): Name for this table. If None, uses module.class name
        """
        if name is None:
            self.name = self.__module__ + '.' + self.__class__.__name__
        else:
            self.name = name
        self.columnNames = columnNames
        self.columnTypes = columnTypes

    def __repr__(self):
        return f"<TableTemplate '{self.name}' with {len(self.columnNames)} columns>"
