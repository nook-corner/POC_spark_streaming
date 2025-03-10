import pytest
from tabulate import tabulate
import json
import os

def run_tests():
    result = pytest.main(["-q", "--tb=short", "--disable-warnings", "--json-report", "--json-report-file=report.json", "--cache-clear"])

    report_file = "report.json"
    
    if not os.path.exists(report_file):
        print("Error: JSON test report file not found.")
        return

    try:
        with open(report_file, "r") as f:
            report = json.load(f)
    except json.JSONDecodeError:
        print("Error: Failed to parse JSON report file.")
        return

    if "tests" not in report:
        print("Error: No test results found in JSON report.")
        return
        
    num_messages = "N/A"
    for test in report["tests"]:
        if "test_kafka_record_count" in test["nodeid"]:
            num_messages = test.get("call", {}).get("num_messages", "N/A")

    results = []
    for test in report["tests"]:
        test_name = test["nodeid"].split("::")[-1]
        outcome = "PASS" if test["outcome"] == "passed" else "FAIL"
        
        results.append([test_name, outcome, num_messages])

    print("\nTest Results:")
    print(tabulate(results, headers=["Test Case", "Result", "Messages Read"], tablefmt="grid"))

if __name__ == "__main__":
    run_tests()
