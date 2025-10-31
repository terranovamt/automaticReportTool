"""
ART.stdf - Automatic Report Tool
Entry Point

This is the main entry point for the ART.stdf system. It initializes the
polling system which monitors directories for STDF files and automatically
generates test reports.

Usage:
    python main.py                      # Use default STDF directory
    python main.py "path/to/directory"  # Monitor specific directory

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import sys
import os

if __name__ == "__main__":
    # Add src directory to Python path for imports
    sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

    from system import polling

    # Check if custom watch path provided as command line argument
    if len(sys.argv) > 1:
        polling.main(sys.argv[1])
    else:
        # Use default path from polling module
        polling.main()
