"""
ART.stdf - Base Report Generator

Abstract base class for all HTML report generators.
Replaces Jupyter notebook-based approach with pure Python HTML generation.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import os
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

from src.domain.models.parameter import Parameter


class BaseReportGenerator(ABC):
    """
    Abstract base class for report generators.

    Provides common HTML structure, template loading, and utility methods.
    Each report type (VOLUME, LOOP, TTIME, YIELD, CONDITION) extends this class.
    """

    def __init__(
        self,
        parameter: Parameter,
        template_dir: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize base report generator.

        Args:
            parameter: Parameter object with report metadata
            template_dir: Directory containing HTML templates
            logger: Logger instance
        """
        self.parameter = parameter
        self.logger = logger or logging.getLogger(__name__)

        if template_dir is None:
            self.template_dir = Path(__file__).parent.parent / "templates"
        else:
            self.template_dir = Path(template_dir)

    def get_template_content(self, filename: str) -> str:
        """
        Load content from template file.

        Args:
            filename: Name of template file

        Returns:
            Template content as string
        """
        file_path = self.template_dir / filename

        if not file_path.exists():
            self.logger.warning(f"Template {filename} not found at {file_path}")
            return f"<!-- Template {filename} not found -->"

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            self.logger.error(f"Error reading template {filename}: {e}")
            return f"<!-- Error reading template: {e} -->"

    def get_product_image(self, product: str, main_path: str = ".\\STDF") -> str:
        """
        Get SVG content for product image.

        Args:
            product: Product code
            main_path: Base path to STDF data

        Returns:
            SVG content or empty string
        """
        folder_path = os.path.join(main_path, product)

        if not os.path.exists(folder_path):
            return ""

        for filename in os.listdir(folder_path):
            if filename.lower().endswith(".svg"):
                svg_path = os.path.join(folder_path, filename)
                try:
                    with open(svg_path, "r", encoding="utf-8") as f:
                        return f.read()
                except Exception as e:
                    self.logger.warning(f"Error reading SVG {svg_path}: {e}")
                    return ""

        return ""

    def build_html_header(self, title: str) -> str:
        """
        Build standard HTML header section.

        Args:
            title: Page title

        Returns:
            HTML header string
        """
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <link rel="shortcut icon" type="image/png" href="https://www.st.com/etc/clientlibs/st-site/media/app/images/favicon.ico">
    <style>{self.get_template_content("style.css")}</style>
</head>
<body>
<main>
    {self.get_template_content("navbar.html")}
"""

    def build_html_footer(self) -> str:
        """
        Build standard HTML footer section.

        Returns:
            HTML footer string
        """
        return f"""
    {self.get_template_content("gotop.html")}
</main>
    {self.get_template_content("footer.html")}
</body>
</html>
"""

    def build_report_info_table(self) -> str:
        """
        Build standard report information table.

        Returns:
            HTML table with report metadata
        """
        return f"""
<table border="1" align="center" style="height: 100%; width: 100%;">
    <tr>
        <td>FLOW</td>
        <td>{self.parameter.flow}</td>
    </tr>
    <tr>
        <td>Part ID-CUT</td>
        <td>{self.parameter.cut}</td>
    </tr>
    <tr>
        <td>Lot</td>
        <td>{self.parameter.lot}</td>
    </tr>
    <tr>
        <td>Wafer</td>
        <td>{self.parameter.wafer}</td>
    </tr>
    <tr>
        <td>Date</td>
        <td>{datetime.now().strftime("%d %B %Y %H:%M")}</td>
    </tr>
</table>
"""

    def save_report(self, html_content: str, output_path: Path) -> None:
        """
        Save HTML content to file.

        Args:
            html_content: Complete HTML document
            output_path: Path where to save the report

        Raises:
            IOError: If file cannot be written
        """
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html_content)

            self.logger.info(f"[REPORT] Generated: {output_path}")

        except Exception as e:
            self.logger.error(f"[REPORT] Failed to save {output_path}: {e}")
            raise IOError(f"Cannot write report to {output_path}: {e}")

    @abstractmethod
    def generate(
        self,
        data_path: str,
        output_path: Path,
        **kwargs
    ) -> Path:
        """
        Generate the report (must be implemented by subclasses).

        Args:
            data_path: Path to input data (Parquet files)
            output_path: Path where to save the report
            **kwargs: Additional parameters specific to report type

        Returns:
            Path to generated report file

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement generate()")

    @abstractmethod
    def get_report_type(self) -> str:
        """
        Get the report type identifier.

        Returns:
            Report type string (e.g., "VOLUME", "LOOP", etc.)
        """
        raise NotImplementedError("Subclasses must implement get_report_type()")
