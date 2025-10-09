#!/usr/bin/env python3
"""
Wrapper script to fetch Gmail data using Claude's Gmail tools
and pass it to the processing script.
"""

import json
import subprocess
import sys

def main():
    print("🔍 Searching Gmail for VemCount emails...")
    print("Note: This script expects to be run in an environment where")
    print("Claude's Gmail tools have already fetched the data.")
    print()
    print("For manual operation, you should:")
    print("1. Use Claude to search Gmail: search_gmail_messages(q='vemcount')")
    print("2. Use Claude to read each thread with attachments")
    print("3. Save the results to /tmp/gmail_search_results.json")
    print("4. Run python3 process_traffic_gmail_api.py")
    print()
    print("For GitHub Actions, this will be automated in the workflow.")

if __name__ == "__main__":
    main()
