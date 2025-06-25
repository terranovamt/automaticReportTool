#!/usr/bin/env python3
"""
Optimized Shmoo Data Visualizer - Process One File at a Time
Reads .shm files, generates HTML plots, then moves .shm files to 'shm' subfolder
Optimized for speed and simplicity with optional debug output and progress tracking
"""

import os
import sys
import re
import shutil
import time
import threading
import numpy as np
import plotly.graph_objects as go
from pathlib import Path
import argparse
from collections import defaultdict

class ShmooVisualizer:
    def __init__(self, debug=False):
        self.debug = debug
        self._progress_lock = threading.Lock()
        self._last_progress_time = 0
    
    def _show_progress(self, current: int, total: int, start_time: float, phase: str = "Processing"):
        """Show progress bar with percentage and performance metrics"""
        current_time = time.time()
        # Update only every 0.1 seconds to avoid performance impact
        if current_time - self._last_progress_time < 0.1 and current < total:
            return

        with self._progress_lock:
            percentage = (current / total) * 100 if total > 0 else 0
            elapsed = current_time - start_time
            speed = current / elapsed if elapsed > 0 else 0
            eta = (total - current) / speed if speed > 0 and current < total else 0
        
            # Progress bar visualization
            bar_width = 50
            # Show full bar at 98% or higher
            if percentage >= 98.0 or current >= total:
                filled = bar_width
            else:
                filled = int(bar_width * current / total) if total > 0 else 0
            bar = '█' * filled + '░' * (bar_width - filled)
        
            progress_text = f"\r{phase}: [{bar}] {percentage:6.1f}% | {current:,}/{total:,}"
            if current < total and eta > 0:
                progress_text += f" | ETA: {eta:4.0f}s"
            else:
                progress_text += " | Complete!  "
        
            # Write to same line for live updates
            sys.stdout.write(progress_text)
            sys.stdout.flush()
        
            self._last_progress_time = current_time
            
            # Clear the progress bar when complete
            if current >= total:
                # Clear the line by overwriting with spaces and return to start
                sys.stdout.write('\r' + ' ' * len(progress_text) + '\r')
                sys.stdout.flush()

    def parse_shmoo_file(self, filepath, debug=False):
        """
        Parse shmoo file (.shm) to extract multi-DUT test data
        Optimized version with simplified parsing logic
        
        Args:
            filepath: Path to .shm file
            debug: Enable debug output
            
        Returns:
            dict: Dictionary containing all parsed data organized by DUT
        """
        debug and print(f"Starting to parse file: {filepath}")
        
        data = {'general': {}, 'duts': {}}
        
        try:
            # Read file content once
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as file:
                content = file.read()
            
            debug and print(f"File content length: {len(content)} characters")
            
            # Split into sections using regex - more efficient
            sections = re.split(r'\[([^\]]+)\]', content)
            
            # Process sections in pairs (name, content)
            for i in range(1, len(sections), 2):
                if i + 1 < len(sections):
                    section_name = sections[i].strip()
                    section_content = sections[i + 1].strip()
                    
                    if section_content:  # Skip empty sections
                        self._parse_section_fast(data, section_name, section_content, debug)
            
            debug and print(f"Parsed {len(data['duts'])} DUTs: {list(data['duts'].keys())}")
            return data
            
        except Exception as e:
            debug and print(f"Error parsing file {filepath}: {e}")
            return None

    def _parse_section_fast(self, data, section_name, content, debug=False):
        """Fast section parsing with optimized logic"""
        lines = [line for line in content.split('\n') if '=' in line]  # Filter only relevant lines
        
        if section_name == 'General':
            for line in lines:
                key, value = line.split('=', 1)
                data['general'][key.strip()] = value.strip()
            return
        
        # Determine DUT ID and section type
        dut_id, section_type = self._get_dut_and_section(section_name)
        
        # Initialize DUT if not exists
        if dut_id not in data['duts']:
            data['duts'][dut_id] = {
                'common': {}, 'axis1': {}, 'axis2': {}, 
                'header': {}, 'results': []
            }
        
        # Parse based on section type
        if section_type in ['common', 'axis1', 'axis2', 'header']:
            target_dict = data['duts'][dut_id][section_type]
            for line in lines:
                key, value = line.split('=', 1)
                target_dict[key.strip()] = value.strip()
        
        elif section_type == 'result':
            result = {}
            for line in lines:
                key, value = line.split('=', 1)
                result[key.strip()] = value.strip()
            if result:
                data['duts'][dut_id]['results'].append(result)

    def _get_dut_and_section(self, section_name):
        """Extract DUT ID and section type from section name - optimized logic"""
        # Modern format: Section_DUT_X
        if '_' in section_name:
            parts = section_name.split('_')
            
            if section_name.startswith('Common_'):
                return parts[1], 'common'
            elif section_name.startswith('Axis_') and section_name.endswith('_1'):
                return parts[1], 'axis1'
            elif section_name.startswith('Axis_') and section_name.endswith('_2'):
                return parts[1], 'axis2'
            elif section_name.startswith('Header_'):
                return parts[1], 'header'
            elif section_name.startswith('ResultData_') and len(parts) >= 3:
                return parts[1], 'result'
        
        # Legacy format: no DUT specified, assume DUT 0
        legacy_map = {
            'Common': 'common',
            'Axis1': 'axis1', 
            'Axis2': 'axis2',
            'Header': 'header'
        }
        
        if section_name in legacy_map:
            return '0', legacy_map[section_name]
        elif section_name.startswith('ResultData') and section_name != 'ResultData':
            return '0', 'result'
        
        return '0', 'unknown'

    def extract_axis_info(self, dut_data, debug=False):
        """Extract X and Y axis information with conditional logic on AxisTestCondition"""
        debug and print("Extracting axis information")
        
        try:
            # Get axis test conditions
            x_condition = dut_data['axis1'].get('AxisTestCondition', '')
            y_condition = dut_data['axis2'].get('AxisTestCondition', '')
            
            debug and print(f"Original - X condition: {x_condition}, Y condition: {y_condition}")
            
            # Determine if swap is needed (Timing vs Level logic)
            axes_swapped = False
            if x_condition.startswith("Timing") and y_condition.startswith("Level"):
                debug and print("Swapping X and Y axes")
                dut_data['axis1'], dut_data['axis2'] = dut_data['axis2'], dut_data['axis1']
                axes_swapped = True
            else:
                debug and print("Axes already in correct order (Level/Timing), no swap needed")
                # No swap needed - leave everything as is
            
            # Extract axis parameters efficiently
            axis_info = {'x': {}, 'y': {}}
            
            for axis_key, axis_name in [('x', 'axis1'), ('y', 'axis2')]:
                axis_data = dut_data[axis_name]
                start = float(axis_data.get('AxisStart', 0))
                stop = float(axis_data.get('AxisStop', 100))
                steps = int(axis_data.get('AxisSteps', 51))
                
                axis_info[axis_key] = {
                    'name': axis_data.get('AxisName', axis_key.upper()),
                    'var': axis_data.get('AxisVarName', ''),
                    'start': start,
                    'stop': stop,
                    'steps': steps,
                    'default': float(axis_data.get('AxisValue', start)),
                    'values': np.linspace(start, stop, steps)
                }
            
            debug and print(f"Axis info extracted: X={axis_info['x']['steps']} steps, Y={axis_info['y']['steps']} steps")
            debug and print(f"Axes swapped: {axes_swapped}")
            return axis_info, axes_swapped
            
        except Exception as e:
            debug and print(f"Error extracting axis info: {e}")
            return None, False

    def create_aggregated_matrix(self, data, debug=False):
        """Create aggregated matrix from all DUTs with pass percentages - optimized version"""
        debug and print("Creating aggregated matrix")
        
        # Get grid dimensions from first DUT
        first_dut_data = next(iter(data['duts'].values()))
        axis_info, axes_swapped = self.extract_axis_info(first_dut_data, debug)
        
        if not axis_info:
            debug and print("Invalid axes, cannot create matrix")
            return None, None, None
        
        x_steps, y_steps = axis_info['x']['steps'], axis_info['y']['steps']
        debug and print(f"Matrix dimensions: {y_steps}x{x_steps}")
        
        # Initialize counters - use more efficient data types
        pass_count = np.zeros((y_steps, x_steps), dtype=np.int16)
        total_count = np.zeros((y_steps, x_steps), dtype=np.int16)
        
        # Process all DUTs efficiently
        mixed_points = 0
        for dut_id, dut_data in data['duts'].items():
            debug and print(f"Processing DUT {dut_id} with {len(dut_data['results'])} results")
            
            for result in dut_data['results']:
                try:
                    # Get original grid coordinates
                    orig_grid_x = int(result.get('ResultDataGridX', -1))
                    orig_grid_y = int(result.get('ResultDataGridY', -1))
                    result_data = result.get('ResultDataResultData', '').strip().lower()
                    
                    if axes_swapped:
                        grid_x = orig_grid_y  # X becomes original Y
                        grid_y = orig_grid_x  # Y becomes original X
                        debug and print(f"Swapped coordinates: ({orig_grid_x},{orig_grid_y}) -> ({grid_x},{grid_y})")
                    else:
                        grid_x = orig_grid_x
                        grid_y = orig_grid_y
                    
                    if 0 <= grid_x < x_steps and 0 <= grid_y < y_steps:
                        total_count[grid_y, grid_x] += 1
                        if result_data == 'pass':
                            pass_count[grid_y, grid_x] += 1
                            
                except (ValueError, KeyError):
                    continue
        
        # Calculate percentage matrix efficiently
        percentage_matrix = np.full((y_steps, x_steps), -1.0, dtype=np.float32)
        
        # Vectorized operation where possible
        valid_mask = total_count > 0
        percentage_matrix[valid_mask] = pass_count[valid_mask] / total_count[valid_mask]
        
        # Count mixed points and create stats matrix
        fail_count = total_count - pass_count
        mixed_mask = (pass_count > 0) & (fail_count > 0)
        mixed_points = np.sum(mixed_mask)
        
        # Create stats matrix more efficiently
        stats_matrix = np.empty((y_steps, x_steps), dtype=object)
        for y in range(y_steps):
            for x in range(x_steps):
                if total_count[y, x] > 0:
                    pct = percentage_matrix[y, x] * 100
                    stats_matrix[y, x] = f"Pass: {pass_count[y, x]}/{total_count[y, x]} ({pct:.1f}%)"
                else:
                    stats_matrix[y, x] = "No Data"
        
        debug and print(f"Mixed points (both pass and fail): {mixed_points}")
        debug and print(f"Total valid points: {np.sum(valid_mask)}")
        
        return percentage_matrix, stats_matrix, axis_info

    def create_shmoo_plot(self, data, percentage_matrix, stats_matrix, axis_info, shm_file, debug=False):
        """Create optimized shmoo plot with grid lines and SPEC highlighting"""
        html_output = shm_file.with_name(f"{shm_file.stem}_shmoo.html")
        debug and print(f"Creating plot: {html_output}")
        
        try:
            # Get plot metadata
            num_duts = len(data['duts'])
            first_dut = next(iter(data['duts'].values()))
            test_name = first_dut['header'].get('ExecutedTestName', 'Shmoo Test')
            executed_date = first_dut['header'].get('ExecutedDate', '')
            testplan_match = re.search(r'\\([^\\]+)\.tpl', first_dut['header'].get('ExecutedPlanFile', ''))
            testplan = testplan_match.group(1) if testplan_match else 'Unknown'
            
            debug and print(f"Plot metadata: {num_duts} DUTs, Test: {test_name}")
            
            # Optimized colorscale
            colorscale = [[0, 'red'], [0.01, '#ff8100'], [0.99, "#fff700"], [1, "#00ff00"]]
            
            # Create masked matrix for missing data
            masked_matrix = np.ma.masked_where(percentage_matrix == -1, percentage_matrix)
            
            # Create heatmap
            fig = go.Figure(data=go.Heatmap(
                z=masked_matrix,
                x=axis_info['x']['values'],
                y=axis_info['y']['values'],
                colorscale=colorscale,
                zmin=0, zmax=1,
                showscale=False,
                hovertemplate=(
                    f"{axis_info['x']['var']}: %{{x}}<br>" +
                    f"{axis_info['y']['var']}: %{{y}}<br>" +
                    "Results: %{customdata}<extra></extra>"
                ),
                customdata=stats_matrix
            ))
            
            # Add failure count annotations for mixed points
            self._add_failure_annotations(fig, stats_matrix, axis_info, debug)
            
            # Add grid lines with SPEC highlighting
            self._add_grid_lines(fig, axis_info, debug)
            
            # Configure layout
            fig.update_layout(
                xaxis=dict(title=axis_info["x"]["var"], showgrid=True, gridwidth=1, gridcolor='lightgray'),
                yaxis=dict(title=axis_info["y"]["var"], showgrid=True, gridwidth=1, gridcolor='lightgray'),
                autosize=True,
                margin=dict(l=80, r=50, t=100, b=80),
                plot_bgcolor='white',
                paper_bgcolor='white'
            )
            
            # Generate and save HTML
            self._generate_html_file(fig, html_output, test_name, num_duts, executed_date, testplan, axis_info, str(shm_file), debug)
            
            debug and print(f"Plot saved successfully: {html_output}")
            return True
            
        except Exception as e:
            debug and print(f"Error creating plot: {e}")
            return False

    def _add_failure_annotations(self, fig, stats_matrix, axis_info, debug=False):
        """Add failure count annotations for mixed points"""
        annotations = []
        y_values, x_values = axis_info['y']['values'], axis_info['x']['values']
        
        for y_idx, y_val in enumerate(y_values):
            for x_idx, x_val in enumerate(x_values):
                stats = stats_matrix[y_idx, x_idx]
                if stats != "No Data":
                    try:
                        # Extract pass/total from stats string
                        pass_info = stats.split("Pass: ")[1].split(" ")[0]
                        pass_count, total_count = map(int, pass_info.split("/"))
                        fail_count = total_count - pass_count
                        
                        # Add annotation only for mixed points
                        if pass_count > 0 and fail_count > 0:
                            annotations.append(dict(
                                x=x_val, y=y_val, text=str(fail_count),
                                showarrow=False, font=dict(color="black", size=14),
                                xanchor="center", yanchor="middle"
                            ))
                    except (ValueError, IndexError):
                        continue
        
        fig.update_layout(annotations=annotations)
        debug and print(f"Added {len(annotations)} failure annotations")

    def _add_grid_lines(self, fig, axis_info, debug=False):
        """Add grid lines with SPEC highlighting"""
        x_values, y_values = axis_info['x']['values'], axis_info['y']['values']
        x_spec, y_spec = axis_info['x']['default'], axis_info['y']['default']
        
        # Find closest indices to SPEC
        x_closest_idx = np.abs(x_values - x_spec).argmin()
        y_closest_idx = np.abs(y_values - y_spec).argmin()
        
        # Calculate spacing
        x_spacing = (x_values[1] - x_values[0]) / 2 if len(x_values) > 1 else 0
        y_spacing = (y_values[1] - y_values[0]) / 2 if len(y_values) > 1 else 0
        
        debug and print(f"SPEC closest indices: X={x_closest_idx}, Y={y_closest_idx}")
        
        # Add vertical lines
        for i in range(len(x_values) + 1):
            x_line = (x_values[0] - x_spacing if i == 0 else
                     x_values[-1] + x_spacing if i == len(x_values) else
                     (x_values[i-1] + x_values[i]) / 2)
            
            is_spec_line = i == x_closest_idx or i == x_closest_idx + 1
            
            fig.add_shape(
                type="line", x0=x_line, x1=x_line,
                y0=y_values[0] - y_spacing, y1=y_values[-1] + y_spacing,
                line=dict(color="#03234B" if is_spec_line else "gray", 
                         width=2 if is_spec_line else 1),
                layer="above"
            )
        
        # Add horizontal lines
        for j in range(len(y_values) + 1):
            y_line = (y_values[0] - y_spacing if j == 0 else
                     y_values[-1] + y_spacing if j == len(y_values) else
                     (y_values[j-1] + y_values[j]) / 2)
            
            is_spec_line = j == y_closest_idx or j == y_closest_idx + 1
            
            fig.add_shape(
                type="line", y0=y_line, y1=y_line,
                x0=x_values[0] - x_spacing, x1=x_values[-1] + x_spacing,
                line=dict(color="#03234B" if is_spec_line else "gray",
                         width=2 if is_spec_line else 1),
                layer="above"
            )

    def _generate_html_file(self, fig, html_output, test_name, num_duts, executed_date, testplan, axis_info, filepath, debug=False):
        """Generate optimized HTML file"""
        debug and print(f"Generating HTML file: {html_output}")
        
        html_template = f"""<!DOCTYPE html>
<html>
<head>
    <title>{os.path.basename(html_output)}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ margin: 0; padding: 20px; font-family: Arial, sans-serif; background-color: #f8f9fa; 
               display: flex; flex-direction: column; align-items: center; min-height: 100vh; }}
        .plot-container {{ width: 95vw; height: 85vh; max-width: 1400px; background-color: white; }}
        .plot-div {{ width: 100%; height: 100%; }}
        .info-box {{ background-color: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                     margin-bottom: 20px; text-align: center; max-width: 1400px; width: 95vw; }}
    </style>
</head>
<body>
    <div class="info-box">
        <h2>Shmoo Plot - {test_name}</h2>
        <p><strong>DUTs:</strong> {num_duts} | <strong>Date:</strong> {executed_date} | <strong>Plan:</strong> {testplan}</p>
        <p><strong>SPEC:</strong> X={axis_info['x']['default']:.3f}, Y={axis_info['y']['default']:.3f}</p>
    </div>
    <div class="plot-container">
        <div id="plotDiv" class="plot-div"></div>
    </div>
    <script>
        var plotData = {fig.to_json()};
        plotData.layout.autosize = true;
        plotData.layout.margin = {{l: 60, r: 40, t: 60, b: 60}};
        Plotly.newPlot('plotDiv', plotData.data, plotData.layout, {{
            displayModeBar: true, displaylogo: false,
            modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d'], responsive: true
        }});
        window.addEventListener('resize', () => Plotly.Plots.resize('plotDiv'));
    </script>
</body>
</html>"""
        
        with open(html_output, 'w', encoding='utf-8') as f:
            f.write(html_template)

    def ensure_shm_folder(self, directory, debug=False):
        """
        Assicura che la cartella 'shm' esista nella directory specificata
        
        Args:
            directory: Cartella di lavoro
            debug: Flag per debug output
            
        Returns:
            Path: Percorso della cartella shm
        """
        directory = Path(directory)
        shm_folder = directory / "shm"
        
        if not shm_folder.exists():
            shm_folder.mkdir()
            debug and print(f"Cartella 'shm' creata: {shm_folder}")
        else:
            debug and print(f"Cartella 'shm' esiste già: {shm_folder}")
        
        return shm_folder

    def move_file_to_shm(self, shm_file, shm_folder, debug=False):
        """
        Sposta un singolo file .shm nella cartella shm
        
        Args:
            shm_file: Percorso del file .shm da spostare
            shm_folder: Cartella di destinazione
            debug: Flag per debug output
            
        Returns:
            bool: True se il file è stato spostato con successo
        """
        try:
            destination = shm_folder / shm_file.name
            shutil.move(str(shm_file), str(destination))
            debug and print(f"File spostato: {shm_file.name} -> shm/{shm_file.name}")
            return True
        except Exception as e:
            print(f"ERRORE nello spostamento di {shm_file.name}: {e}")
            return False

    def process_shmoo_files(self, directory, debug=False):
        """Process all .shm files in specified directory - one at a time approach with progress tracking"""
        debug and print(f"Processing directory: {directory}")
        
        directory = Path(directory)
        if not directory.exists():
            print(f"ERROR: Directory {directory} does not exist")
            return
        
        # Get all .shm files
        shm_files = list(directory.glob("*.shm"))
        
        if not shm_files:
            print(f"No .shm files found in {directory}")
            return
        
        debug and print(f"Found {len(shm_files)} .shm files in {directory}")
        
        # Assicura che la cartella 'shm' esista
        shm_folder = self.ensure_shm_folder(directory, debug)
        
        processed_count = 0
        start_time = time.time()
        
        # Processa ogni file uno alla volta con progress tracking
        for idx, shm_file in enumerate(shm_files):
            # Update progress
            self._show_progress(idx, len(shm_files), start_time, "Processing files")
            
            debug and print(f"\n[{idx + 1}/{len(shm_files)}] Processando {shm_file.name}...")
            
            # 1. Prima parse il file dalla posizione originale
            debug and print(f"  Parsing file: {shm_file}")
            data = self.parse_shmoo_file(shm_file, debug)
            if not data or not data['duts']:
                print(f"  No DUTs found in {shm_file.name}")
                # Sposta comunque il file anche se il parsing fallisce
                self.move_file_to_shm(shm_file, shm_folder, debug)
                continue
            
            num_duts = len(data['duts'])
            debug and print(f"  Found {num_duts} DUTs: {list(data['duts'].keys())}")
            
            # 2. Crea il plot aggregato
            debug and print("  Creando plot aggregato...")
            matrix_result = self.create_aggregated_matrix(data, debug)
            
            if matrix_result[0] is not None:
                percentage_matrix, stats_matrix, axis_info = matrix_result
                
                success = self.create_shmoo_plot(data, percentage_matrix, stats_matrix, axis_info, shm_file, debug)
                
                if success:
                    html_file = shm_file.with_name(f"{shm_file.stem}_shmoo.html")
                    debug and print(f"  HTML plot saved: {html_file.name}")
                    debug and print(f"  SPEC X ({axis_info['x']['var']}): {axis_info['x']['default']}")
                    debug and print(f"  SPEC Y ({axis_info['y']['var']}): {axis_info['y']['default']}")
                    
                    # 4. Solo ora sposta il file .shm nella cartella 'shm'
                    if self.move_file_to_shm(shm_file, shm_folder, debug):
                        debug and print(f" File spostato in: shm/{shm_file.name}")
                        processed_count += 1
                    else:
                        print(f" HTML creato ma errore nello spostamento del file")
                else:
                    print(f"  ERROR: Failed to create plot for {shm_file.name}")
                    # Sposta comunque il file anche se la creazione del plot fallisce
                    self.move_file_to_shm(shm_file, shm_folder, debug)
            else:
                print(f"  ERROR: Failed to create matrix for {shm_file.name}")
                # Sposta comunque il file anche se la creazione della matrice fallisce
                self.move_file_to_shm(shm_file, shm_folder, debug)
        
        # Final progress update
        self._show_progress(len(shm_files), len(shm_files), start_time, "Processing files")


def main():
    """Main function with debug argument support"""
    parser = argparse.ArgumentParser(description='Optimized Shmoo Data Visualizer - Aggregated Plots Only')
    parser.add_argument('directory', nargs='?', default='.', 
                       help='Directory containing .shm files (default: current directory)')
    parser.add_argument('--debug', action='store_true', default=False,
                       help='Enable debug output (default: False)')
    
    args = parser.parse_args()
    
    print("=== Shmoo Data Visualizer ===")
    print(f"Directory: {os.path.abspath(args.directory)}")
    if args.debug:
        print("Debug mode activated")
    
    try:
        visualizer = ShmooVisualizer(debug=args.debug)
        visualizer.process_shmoo_files(args.directory, args.debug)
        print("\nProcessing completed successfully!")
    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()