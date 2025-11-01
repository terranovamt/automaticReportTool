"""
ART.stdf - Setup Configuration

Package setup for the Automatic Report Tool (ART.stdf).

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

# Read requirements
requirements_file = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_file.exists():
    with open(requirements_file, "r", encoding="utf-8") as f:
        requirements = [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#")
        ]

# Read dev requirements
dev_requirements_file = Path(__file__).parent / "requirements-dev.txt"
dev_requirements = []
if dev_requirements_file.exists():
    with open(dev_requirements_file, "r", encoding="utf-8") as f:
        dev_requirements = [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#") and not line.startswith("-r")
        ]

setup(
    name="art-stdf",
    version="2.0.0",
    author="Matteo Terranova",
    author_email="matteo.terranova@st.com",
    description="Automatic Report Tool for STDF (Standard Test Data Format) Processing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/terranovamt/automaticReportTool",
    project_urls={
        "Bug Tracker": "https://github.com/terranovamt/automaticReportTool/issues",
        "Documentation": "https://github.com/terranovamt/automaticReportTool#readme",
        "Source Code": "https://github.com/terranovamt/automaticReportTool",
    },
    packages=find_packages(where=".", include=["src*", "config*", "scripts*"]),
    package_dir={"": "."},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Electronic Design Automation (EDA)",
        "Topic :: Software Development :: Testing",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": dev_requirements,
    },
    entry_points={
        "console_scripts": [
            "art-stdf=main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.md", "*.txt", "*.html", "*.css", "*.js", "*.svg", "*.ipynb"],
    },
    zip_safe=False,
)
