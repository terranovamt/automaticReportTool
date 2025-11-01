"""
ART.stdf - Line Count Rotating File Handler

Custom rotating file handler that rotates log files based on line count
instead of file size. Extracted from original polling.py module.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import os
from logging.handlers import BaseRotatingHandler


class LineCountRotatingFileHandler(BaseRotatingHandler):
    """
    Custom rotating file handler that rotates log files based on line count
    instead of file size.

    This handler is more suitable for log files where each line represents
    a meaningful event, and you want to keep a fixed number of recent events.

    Attributes:
        max_lines (int): Maximum number of lines before rotation
        backup_count (int): Number of backup files to keep
        line_count (int): Current line count in the file
    """

    def __init__(self, filename, max_lines=1000, backup_count=1, **kwargs):
        """
        Initialize the handler.

        Args:
            filename (str): Path to the log file
            max_lines (int): Maximum number of lines before rotation (default: 1000)
            backup_count (int): Number of backup files to keep (default: 1)
            **kwargs: Additional keyword arguments for BaseRotatingHandler
        """
        super().__init__(filename, "a", **kwargs)
        self.max_lines = max_lines
        self.backup_count = backup_count
        self.line_count = 0
        self._open()

    def _open(self):
        """
        Open the log file and count existing lines.

        This method opens the file and counts the existing lines to maintain
        accurate line count tracking across application restarts.
        """
        self.stream = open(self.baseFilename, self.mode, encoding='utf-8')

        # Count existing lines if file exists and has content
        if os.path.exists(self.baseFilename):
            with open(self.baseFilename, 'r', encoding='utf-8') as f:
                self.line_count = sum(1 for _ in f)

        # Move to the end of the file for appending
        self.stream.seek(0, 2)

    def shouldRollover(self, record):
        """
        Determine if log file should be rotated.

        Args:
            record: Log record being written

        Returns:
            bool: True if rotation should occur (line count >= max_lines)
        """
        return self.line_count >= self.max_lines

    def doRollover(self):
        """
        Perform the log file rotation.

        This method:
        1. Closes the current log file
        2. Rotates existing backup files (file.log.1 -> file.log.2, etc.)
        3. Renames current log to file.log.1
        4. Opens a new log file
        """
        if self.stream:
            self.stream.close()
            self.stream = None

        # Rotate existing backup files
        # Example: file.log.1 -> file.log.2, file.log.2 -> file.log.3
        for i in range(self.backup_count - 1, 0, -1):
            source_file = f"{self.baseFilename}.{i}"
            dest_file = f"{self.baseFilename}.{i + 1}"

            if os.path.exists(source_file):
                if os.path.exists(dest_file):
                    os.remove(dest_file)
                os.rename(source_file, dest_file)

        # Move current log to backup position (.1)
        dest_file = f"{self.baseFilename}.1"
        if os.path.exists(dest_file):
            os.remove(dest_file)

        self.rotate(self.baseFilename, dest_file)

        # Reset line count and open new file
        self.line_count = 0
        self._open()

    def emit(self, record):
        """
        Emit a log record and increment line count.

        Args:
            record: Log record to emit
        """
        if self.shouldRollover(record):
            self.doRollover()

        super().emit(record)
        self.line_count += 1
