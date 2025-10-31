"""Parallel processing utilities"""

import multiprocessing
from typing import List, Callable, Any, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed


def parallel_process_files(
    files: List[Any],
    processor_func: Callable,
    num_workers: int = 2
) -> List[Tuple[bool, Any, str]]:
    """
    Process files in parallel using multiprocessing.

    Args:
        files: List of files to process
        processor_func: Function to process each file
        num_workers: Number of parallel workers

    Returns:
        List of (success, result, error_message) tuples
    """
    if not files or num_workers < 1:
        return []

    if len(files) == 1:
        # Single file - process directly
        try:
            result = processor_func(files[0])
            return [(True, result, "")]
        except Exception as e:
            return [(False, files[0], str(e))]

    # Multiple files - use multiprocessing
    with multiprocessing.Pool(processes=num_workers) as pool:
        results = pool.map(processor_func, files)

    return results


def process_with_pool(
    items: List[Any],
    worker_func: Callable,
    max_workers: int = 4
) -> List[Any]:
    """
    Process items with ProcessPoolExecutor.

    Args:
        items: Items to process
        worker_func: Worker function
        max_workers: Maximum workers

    Returns:
        List of results
    """
    results = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker_func, item): item for item in items}
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Error processing item: {e}")
    return results
