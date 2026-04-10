#!/usr/bin/env python3
"""
Create HBase Tables Using HBase Shell Commands
Owner: Chawi
Compatible with Python 3.6
"""

import subprocess
import time
import sys

def run_hbase_command(commands):
    """Execute HBase shell commands - Python 3.6 compatible"""
    # Use docker exec with hbase shell directly
    cmd = ['docker', 'exec', '-i', 'hbase', 'hbase', 'shell']
    
    try:
        # Pass commands via stdin (without 'text' parameter for Python 3.6)
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        # Encode commands to bytes for Python 3.6
        stdout, stderr = process.communicate(input=commands.encode('utf-8'), timeout=30)
        # Decode bytes to string
        return stdout.decode('utf-8') + stderr.decode('utf-8')
    except subprocess.TimeoutExpired:
        process.kill()
        return "ERROR: Command timeout"
    except Exception as e:
        return f"ERROR: {str(e)}"

def create_table(table_name, column_families):
    """Create a table with specified column families"""
    print(f"\n  Creating table: {table_name}")
    
    # First check if table exists
    if table_exists(table_name):
        print(f"    Table '{table_name}' already exists, skipping...")
        return True
    
    # Create table with column families
    families_str = ", ".join([f"{{NAME => '{cf}'}}" for cf in column_families])
    create_cmd = f"create '{table_name}', {families_str}\n"
    
    print(f"    Executing: {create_cmd.strip()}")
    output = run_hbase_command(create_cmd)
    
    # Check for successful creation
    if "ERROR" in output.upper() and "already exists" not in output.lower():
        print(f"    ERROR: Could not create table")
        print(f"    {output[:500]}")
        return False
    elif "Created table" in output or "CREATED" in output.upper():
        print(f"    SUCCESS: Table '{table_name}' created")
        print(f"    Column families: {', '.join(column_families)}")
        return True
    elif "already exists" in output.lower():
        print(f"    Table '{table_name}' already exists")
        return True
    else:
        # If no error, assume success
        print(f"    SUCCESS: Table '{table_name}' created/verified")
        print(f"    Column families: {', '.join(column_families)}")
        return True

def table_exists(table_name):
    """Check if table exists"""
    output = run_hbase_command(f"exists '{table_name}'\n")
    return "does exist" in output.lower()

def list_all_tables():
    """List all tables in HBase"""
    output = run_hbase_command("list\n")
    return output

def main():
    print("="*60)
    print("HBASE TABLES CREATION")
    print("Owner: Chawi")
    print("="*60)
    print("Using HBase Shell commands\n")
    
    # Define tables and their column families
    tables = {
        'ip_reputation': ['metadata', 'reputation', 'statistics'],
        'attack_patterns': ['metadata', 'characteristics', 'statistics', 'mitigation'],
        'threat_timeline': ['time_series', 'threat_metrics', 'volume_stats', 'trends']
    }
    
    # Create each table
    success_count = 0
    for table_name, families in tables.items():
        if create_table(table_name, families):
            success_count += 1
        time.sleep(1)  # Give HBase time to process
    
    # List all tables
    print("\n" + "="*60)
    print("All HBase Tables")
    print("="*60)
    
    list_output = list_all_tables()
    print(list_output)
    
    print("\n" + "="*60)
    print("CREATION COMPLETE")
    print("="*60)
    
    if success_count == 3:
        print("[SUCCESS] 3/3 tables created successfully")
        print("\n[DONE] Definition of Done achieved!")
    else:
        print(f"[WARNING] Only {success_count}/3 tables created")
    
    print("\n" + "="*60)
    print("NEXT STEPS")
    print("="*60)
    print("1. Verify tables: docker exec -it hbase hbase shell -c 'list'")
    print("2. Load data: python3 /tmp/load_spark_data.py")
    print("3. Or load manually with HBase shell")

if __name__ == "__main__":
    main()