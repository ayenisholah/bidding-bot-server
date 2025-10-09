#!/usr/bin/env python3
import json
import re
import sys

def clean_tasks_file(input_file, output_file):
    """Remove 'traits' and '_id' fields from all items in the MongoDB insert file"""
    
    print(f"Processing {input_file}...")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Parse the file to extract the array content
    # Look for the pattern: db.tasks.insertMany([...])
    match = re.search(r'db\.tasks\.insertMany\(\[\s*(.*?)\s*\]\)', content, re.DOTALL)
    
    if not match:
        print("Error: Could not find insertMany array in the file")
        return False
    
    array_content = '[' + match.group(1) + ']'
    
    try:
        # Parse the JSON array
        tasks = json.loads(array_content)
        print(f"Found {len(tasks)} tasks")
        
        # Clean each task
        cleaned_tasks = []
        for i, task in enumerate(tasks):
            cleaned_task = {}
            for key, value in task.items():
                # Skip 'traits' and '_id' fields
                if key not in ['traits', '_id']:
                    cleaned_task[key] = value
            cleaned_tasks.append(cleaned_task)
            
            if (i + 1) % 1000 == 0:
                print(f"Processed {i + 1} tasks...")
        
        # Write the cleaned data
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('db.tasks.insertMany(')
            json.dump(cleaned_tasks, f, indent=2, ensure_ascii=False)
            f.write(')')
        
        print(f"Successfully cleaned file. Output saved to: {output_file}")
        print(f"Removed 'traits' and '_id' fields from {len(cleaned_tasks)} tasks")
        return True
        
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return False
    except Exception as e:
        print(f"Error processing file: {e}")
        return False

if __name__ == "__main__":
    input_file = "/Users/titan/projects/nfttools/bidding-bot/server/tasks (52).json"
    output_file = "/Users/titan/projects/nfttools/bidding-bot/server/tasks_cleaned.json"
    
    success = clean_tasks_file(input_file, output_file)
    if success:
        print("\nCleaning completed successfully!")
        
        # Show file sizes
        import os
        original_size = os.path.getsize(input_file) / (1024 * 1024)  # MB
        cleaned_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
        print(f"Original file size: {original_size:.2f} MB")
        print(f"Cleaned file size: {cleaned_size:.2f} MB")
        print(f"Size reduction: {((original_size - cleaned_size) / original_size * 100):.1f}%")
    else:
        print("Cleaning failed!")
        sys.exit(1)