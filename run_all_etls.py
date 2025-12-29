import subprocess
import os
import sys

def run_script(script_path):
    print(f"\n>>> Starting ETL: {os.path.basename(script_path)} <<<")
    try:
        # Run with current executable to ensure environment is same
        result = subprocess.run([sys.executable, script_path], check=True)
        print(f">>> Successfully completed {os.path.basename(script_path)} <<<")
    except subprocess.CalledProcessError as e:
        print(f">>> ERROR in {os.path.basename(script_path)}: {e} <<<")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    etl_dir = os.path.join(BASE_DIR, 'etl')
    
    scripts = [
        os.path.join(etl_dir, 'etl_health.py'),
        os.path.join(etl_dir, 'etl_education.py')
    ]
    
    for script in scripts:
        if os.path.exists(script):
            run_script(script)
        else:
            print(f"Skipping missing script: {script}")

    print("\n--- All ETL Processes Finished ---")
