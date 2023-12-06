import subprocess
import os

def run_script_in_terminal(script_path):
    terminal_command = os.getenv('TERMINAL', 'x-terminal-emulator')  # Default to x-terminal-emulator if TERMINAL is not set
    command = f'{terminal_command} -e python3 {script_path}'
    subprocess.run(command, shell=True)

# Example: Run multiple scripts in separate terminals
scripts_to_run = ['/home/azureuser/prakash/pdf_extraction_pipeline/pdf_pipeline/tableBank_consumer.py','/home/azureuser/prakash/pdf_extraction_pipeline/pdf_pipeline/nougat_consumer.py']

for script in scripts_to_run:
    script_path = os.path.abspath(script)
    run_script_in_terminal(script_path)
