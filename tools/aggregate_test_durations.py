#!/usr/bin/env python3
"""Script to aggregate pytest test durations by different levels.

- Individual tests
- Test modules (files)
- Test directories

Usage:
    python tools/aggregate_test_durations.py [pytest_output_file]
"""

import argparse
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional


class TestDurationAggregator:
    def __init__(self, filenames: list[str]):
        self.filenames = filenames if isinstance(filenames, list) else [filenames]
        self.test_times = []
        self.module_times = defaultdict(list)
        self.directory_times = defaultdict(list)
        self.tests_analyzed = 0

    def parse_durations(self):
        """Parse test durations from pytest output files."""
        # Pattern to match test timing lines (setup, teardown, call)
        pattern = r"^(\d+\.\d+)s (setup|teardown|call)\s+tests/([^:]+\.py)::([^\s]+)"

        for filename in self.filenames:
            try:
                with open(filename) as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()

                        # Check for duration lines
                        match = re.match(pattern, line)
                        if match:
                            time_str, phase, file_path, test_name = match.groups()
                            time_seconds = float(time_str)

                            # Count 'call' phases as actual tests
                            if phase == "call":
                                self.tests_analyzed += 1

                            # Store individual test with phase information
                            self.test_times.append(
                                {
                                    "time": time_seconds,
                                    "phase": phase,
                                    "file": file_path,
                                    "test": test_name,
                                    "line": line_num,
                                }
                            )

                            # Aggregate by module
                            self.module_times[file_path].append(time_seconds)

                            # Aggregate by directory - only count in the immediate parent directory
                            dir_path = str(Path(file_path).parent)
                            self.directory_times[dir_path].append(time_seconds)

            except FileNotFoundError:
                print(f"Error: File '{filename}' not found.")
                sys.exit(1)
            except Exception as e:
                print(f"Error reading file '{filename}': {e}")
                sys.exit(1)

    def calculate_stats(self, times: list[float]) -> dict[str, float]:
        """Calculate statistics for a list of times."""
        if not times:
            return {"total": 0, "avg": 0, "max": 0, "min": 0, "count": 0}

        return {
            "total": sum(times),
            "avg": sum(times) / len(times),
            "max": max(times),
            "min": min(times),
            "count": len(times),
        }

    def get_phase_breakdown(self, key: str) -> dict[str, float]:
        """Get phase breakdown (setup, call, teardown) for a given key."""
        phase_times = {"setup": 0.0, "call": 0.0, "teardown": 0.0}

        for test in self.test_times:
            if test["file"] == key:  # For modules
                phase_times[test["phase"]] += test["time"]
            elif str(Path(test["file"]).parent) == key:  # For directories
                phase_times[test["phase"]] += test["time"]

        return phase_times

    def print_results(
        self, title: str, data: dict[str, list[float]], sort_key: str = "total", limit: Optional[int] = None
    ):
        """Print aggregated results in a formatted table with phase breakdown."""
        print(f"\n{'='*80}")
        print(f"{title}")
        print(f"{'='*80}")

        # Calculate stats for each item with phase breakdown
        results = []
        for key, times in data.items():
            stats = self.calculate_stats(times)
            stats["key"] = key

            # Get phase breakdown for this key
            phase_breakdown = self.get_phase_breakdown(key)
            stats.update(phase_breakdown)

            results.append(stats)

        # Sort by specified key (descending)
        results.sort(key=lambda x: x[sort_key], reverse=True)

        # Apply limit if specified
        if limit:
            results = results[:limit]

        # Prepare table data
        table_data = []
        for result in results:
            # Create range string for min-max
            range_str = (
                f"{result['min']:.2f}-{result['max']:.2f}" if result["max"] > result["min"] else f"{result['max']:.2f}"
            )
            table_data.append(
                [
                    result["key"],
                    f"{result['total']:.1f}",
                    f"{result['setup']:.1f}",
                    f"{result['call']:.1f}",
                    f"{result['teardown']:.1f}",
                    f"{result['avg']:.2f}",
                    range_str,
                    result["count"],
                ]
            )

        # Add totals row
        all_total_time = sum(sum(times) for times in data.values())
        all_total_tests = sum(len(times) for times in data.values())
        all_setup = sum(result["setup"] for result in results)
        all_call = sum(result["call"] for result in results)
        all_teardown = sum(result["teardown"] for result in results)

        # Calculate overall stats for totals row
        all_avg = all_total_time / all_total_tests if all_total_tests > 0 else 0
        all_max = max(max(times) for times in data.values()) if data else 0
        all_min = min(min(times) for times in data.values()) if data else 0

        # Create range string for totals
        total_range = f"{all_min:.2f}-{all_max:.2f}" if all_max > all_min else f"{all_max:.2f}"
        table_data.append(
            [
                "TOTAL",
                f"{all_total_time:.1f}",
                f"{all_setup:.1f}",
                f"{all_call:.1f}",
                f"{all_teardown:.1f}",
                f"{all_avg:.2f}",
                total_range,
                all_total_tests,
            ]
        )

        # Print table
        print(f"{'Name':<50} {'Total':<7} {'Setup':<7} {'Call':<6} {'Tear':<6} {'Avg':<6} {'Range':<12} {'Cnt':<5}")
        print("-" * 105)
        for row in table_data:
            print(f"{row[0]:<50} {row[1]:<7} {row[2]:<7} {row[3]:<6} {row[4]:<6} {row[5]:<6} {row[6]:<12} {row[7]:<5}")

    def print_summary(self):
        """Print overall summary statistics."""
        print(f"\n{'='*80}")
        print("OVERALL SUMMARY")
        print(f"{'='*80}")

        total_tests = len(self.test_times)
        total_time = sum(test["time"] for test in self.test_times)
        avg_time = total_time / total_tests if total_tests > 0 else 0
        max_time = max(test["time"] for test in self.test_times) if self.test_times else 0
        min_time = min(test["time"] for test in self.test_times) if self.test_times else 0

        print(f"Tests Analyzed: {self.tests_analyzed}")
        print(f"Total Time: {total_time:.2f}s")
        print(f"Average Time: {avg_time:.2f}s")
        print(f"Max Time: {max_time:.2f}s")
        print(f"Min Time: {min_time:.2f}s")

        # Phase breakdown
        phase_times = defaultdict(list)
        for test in self.test_times:
            phase_times[test["phase"]].append(test["time"])

        print("\nBy Phase:")
        phase_table_data = []
        for phase, times in phase_times.items():
            stats = self.calculate_stats(times)
            phase_table_data.append(
                [
                    phase.capitalize(),
                    stats["count"],
                    f"{stats['total']:.2f}",
                    f"{stats['avg']:.2f}",
                    f"{stats['max']:.2f}",
                    f"{stats['min']:.2f}",
                ]
            )

        print(f"{'Phase':<10} {'Count':<6} {'Total (s)':<10} {'Avg (s)':<8} {'Max (s)':<8} {'Min (s)':<8}")
        print("-" * 60)
        for row in phase_table_data:
            print(f"{row[0]:<10} {row[1]:<6} {row[2]:<10} {row[3]:<8} {row[4]:<8} {row[5]:<8}")

    def print_slowest_tests(self):
        """Print the slowest individual tests."""
        print(f"\n{'='*80}")
        print("SLOWEST INDIVIDUAL TESTS")
        print(f"{'='*80}")

        # Sort tests by time (descending)
        sorted_tests = sorted(self.test_times, key=lambda x: x["time"], reverse=True)

        # Take top 50
        top_tests = sorted_tests[:50]

        # Print header
        print(f"{'Time':<8} {'Phase':<8} {'Test':<60}")
        print("-" * 80)

        # Print tests
        for test in top_tests:
            test_name = f"{test['file']}::{test['test']}"
            print(f"{test['time']:<8.2f} {test['phase']:<8} {test_name:<60}")

    def run_analysis(self):
        """Run the complete analysis."""
        start_time = time.time()

        print(f"Analyzing test durations from: {', '.join(self.filenames)}")
        print("Note: All times are in seconds")
        print()
        self.parse_durations()

        if not self.test_times:
            print("No test durations found in the files.")
            return

        # Print summary
        self.print_summary()

        # Print slowest individual tests
        self.print_slowest_tests()

        # Print module-level aggregation
        self.print_results("SLOWEST TEST MODULES (FILES)", self.module_times, sort_key="total", limit=None)

        # Print directory-level aggregation
        self.print_results("SLOWEST TEST DIRECTORIES", self.directory_times, sort_key="total", limit=None)

        # Print report generation time
        end_time = time.time()
        report_time = end_time - start_time
        print(f"\nReport generated in {report_time:.3f} seconds")


def main():
    parser = argparse.ArgumentParser(description="Aggregate pytest test durations by different levels")
    parser.add_argument("files", nargs="+", help="Pytest output file(s) to analyze")

    args = parser.parse_args()

    # Check if all files exist
    for file_path in args.files:
        if not Path(file_path).exists():
            print(f"Error: File '{file_path}' does not exist.")
            sys.exit(1)

    # Run analysis with all files
    aggregator = TestDurationAggregator(args.files)
    aggregator.run_analysis()


if __name__ == "__main__":
    main()
