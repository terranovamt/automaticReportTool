"""Base processor interface"""

from abc import ABC, abstractmethod
from pathlib import Path
from src.core.models import ProcessingResult


class BaseProcessor(ABC):
    """Base class for all processors."""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def process(self, input_path: Path, output_path: Path) -> ProcessingResult:
        """
        Process input file and generate output.

        Args:
            input_path: Input file path
            output_path: Output directory path

        Returns:
            ProcessingResult with status and metadata
        """
        pass

    @abstractmethod
    def validate_input(self, input_path: Path) -> bool:
        """Validate input file."""
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"
