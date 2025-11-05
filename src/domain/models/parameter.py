"""
ART.stdf - Parameter Domain Model

Domain model for test parameters extracted from STDF file paths.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class FileCorner:
    """
    Represents a test corner for a specific file.

    Attributes:
        corner: Corner identifier (e.g., "TTTT", temperature/voltage corner)
        path: Full path to the file
    """

    corner: str
    path: str

    def to_dict(self) -> Dict:
        """
        Convert FileCorner to dictionary.

        Returns:
            Dictionary with corner and path
        """
        return {
            "corner": self.corner,
            "path": self.path
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "FileCorner":
        """
        Create FileCorner from dictionary.

        Args:
            data: Dictionary with corner and path

        Returns:
            FileCorner instance
        """
        return cls(
            corner=data.get("corner", ""),
            path=data.get("path", "")
        )


@dataclass
class Parameter:
    """
    Domain model for test parameters.

    This class encapsulates all metadata extracted from STDF file paths
    and used throughout the reporting system.

    Attributes:
        code: Product code (3 characters, e.g., "44E")
        cut: Product cut (4 characters, e.g., "44EA")
        flow: Test flow type (e.g., "EWS1", "FT")
        type: Report type (e.g., "VOLUME", "LOOP")
        lot: Lot identifier
        wafer: Wafer identifier
        title: Report title
        com: Composite name
        product: Full product name
        revision: Report revision
        file: Dictionary mapping wafer IDs to FileCorner objects
        author: Report author
        mail: Author email
        site: Test site location
        group: Test group
        test_num: List of test numbers to include
        data: Additional data or filename
        main: Main file path
    """

    code: str
    cut: str
    flow: str
    type: str
    lot: str
    wafer: str
    title: str = ""
    com: str = ""
    product: str = ""
    revision: str = "0.1"
    file: Dict[str, FileCorner] = field(default_factory=dict)
    author: str = "Matteo Terranova"
    mail: str = "matteo.terranova@st.com"
    site: str = "Catania"
    group: str = "MDRF - EP - GPAM"
    test_num: List[str] = field(default_factory=list)
    data: str = ""
    main: str = ""

    def __post_init__(self):
        """Post-initialization processing."""
        # Convert file dict values to FileCorner if they're plain dicts
        if self.file:
            for wafer_id, file_data in self.file.items():
                if isinstance(file_data, dict):
                    self.file[wafer_id] = FileCorner(**file_data)

        # Compute title if not provided
        if not self.title:
            self.title = f"{self.lot}_{self.wafer}_{self.code}_{self.cut}_{self.flow}_{self.type}"

        # Compute com if not provided
        if not self.com:
            self.com = f"{self.code} {self.cut} {self.lot} {self.flow} WAFER:{self.wafer}"

    @property
    def product_cut(self) -> str:
        """Get product cut (alias for cut attribute)."""
        return self.cut

    @property
    def flow_upper(self) -> str:
        """Get flow in uppercase."""
        return self.flow.upper()

    @property
    def type_upper(self) -> str:
        """Get type in uppercase."""
        return self.type.upper()

    @property
    def type_lower(self) -> str:
        """Get type in lowercase."""
        return self.type.lower()

    @property
    def is_ews_flow(self) -> bool:
        """Check if this is an EWS flow."""
        return self.flow.upper().startswith("EWS")

    @property
    def is_loop_type(self) -> bool:
        """Check if this is a LOOP type test."""
        return "LOOP" in self.type.upper()

    @property
    def is_volume_type(self) -> bool:
        """Check if this is a VOLUME type test."""
        return "VOLUME" in self.type.upper()

    @property
    def is_char_type(self) -> bool:
        """Check if this is a CHAR type test."""
        return "CHAR" in self.type.upper()

    def get_file_for_wafer(self, wafer_id: Optional[str] = None) -> Optional[FileCorner]:
        """
        Get file information for a specific wafer.

        Args:
            wafer_id: Wafer identifier (default: use self.wafer)

        Returns:
            FileCorner object or None if not found
        """
        if wafer_id is None:
            wafer_id = self.wafer

        return self.file.get(wafer_id)

    def add_file(self, wafer_id: str, corner: str, path: str):
        """
        Add file information for a wafer.

        Args:
            wafer_id: Wafer identifier
            corner: Corner identifier
            path: File path
        """
        self.file[wafer_id] = FileCorner(corner=corner, path=path)

    def to_dict(self) -> Dict:
        """
        Convert parameter to dictionary format.

        Returns:
            Dictionary representation (compatible with legacy format)
        """
        # Convert FileCorner objects to dicts
        file_dict = {}
        for wafer_id, file_corner in self.file.items():
            if isinstance(file_corner, FileCorner):
                file_dict[wafer_id] = {
                    "corner": file_corner.corner,
                    "path": file_corner.path
                }
            else:
                file_dict[wafer_id] = file_corner

        return {
            "TITLE": self.title,
            "COM": self.com,
            "FLOW": self.flow,
            "TYPE": self.type,
            "PRODUCT": self.product,
            "CODE": self.code,
            "LOT": self.lot,
            "WAFER": self.wafer,
            "CUT": self.cut,
            "REVISION": self.revision,
            "FILE": file_dict,
            "AUTHOR": self.author,
            "MAIL": self.mail,
            "SITE": self.site,
            "GROUP": self.group,
            "TEST_NUM": self.test_num,
            "DATA": self.data,
            "MAIN": self.main,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "Parameter":
        """
        Create Parameter from dictionary.

        Args:
            data: Dictionary with parameter data

        Returns:
            Parameter instance
        """
        # Convert file dict to proper format
        file_data = data.get("FILE", {})
        file_corners = {}

        for wafer_id, file_info in file_data.items():
            if isinstance(file_info, dict):
                file_corners[wafer_id] = FileCorner(
                    corner=file_info.get("corner", ""),
                    path=file_info.get("path", "")
                )

        return cls(
            code=data.get("CODE", ""),
            cut=data.get("CUT", ""),
            flow=data.get("FLOW", ""),
            type=data.get("TYPE", ""),
            lot=data.get("LOT", ""),
            wafer=data.get("WAFER", ""),
            title=data.get("TITLE", ""),
            com=data.get("COM", ""),
            product=data.get("PRODUCT", ""),
            revision=data.get("REVISION", "0.1"),
            file=file_corners,
            author=data.get("AUTHOR", "Matteo Terranova"),
            mail=data.get("MAIL", "matteo.terranova@st.com"),
            site=data.get("SITE", "Catania"),
            group=data.get("GROUP", "MDRF - EP - GPAM"),
            test_num=data.get("TEST_NUM", []),
            data=data.get("DATA", ""),
            main=data.get("MAIN", ""),
        )
