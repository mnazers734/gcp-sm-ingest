#!/usr/bin/env python3
"""
Simple test script to verify the container works
"""

import os
import sys

print("=== SIMPLE TEST STARTING ===")
print(f"Python version: {sys.version}")
print(f"Working directory: {os.getcwd()}")
print(f"Python path: {sys.path}")

# Test basic imports
try:
    import json
    print("✓ json imported successfully")
except Exception as e:
    print(f"❌ json import failed: {e}")

try:
    import logging
    print("✓ logging imported successfully")
except Exception as e:
    print(f"❌ logging import failed: {e}")

try:
    from google.cloud import storage
    print("✓ google.cloud.storage imported successfully")
except Exception as e:
    print(f"❌ google.cloud.storage import failed: {e}")

print("=== SIMPLE TEST COMPLETED ===")
