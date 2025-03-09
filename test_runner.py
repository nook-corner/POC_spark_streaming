import pytest
from tabulate import tabulate

def run_tests():
    result = pytest.main(["-q", "--tb=short", "--disable-warnings", "--json-report", "--json-report-file=report.json"])
    
    # อ่าน JSON Report
    import json
    with open("report.json") as f:
        report = json.load(f)
    
    # แปลงผลลัพธ์เป็นตาราง
    results = []
    for test in report["tests"]:
        results.append([test["nodeid"].split("::")[-1], "PASS" if test["outcome"] == "passed" else "FAIL"])
    
    print(tabulate(results, headers=["Test Case", "Result"], tablefmt="grid"))

if __name__ == "__main__":
    run_tests()
