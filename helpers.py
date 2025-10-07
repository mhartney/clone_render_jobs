import subprocess

from datetime import datetime

def run_command(cmd: list):
    process = execute_subprocess(cmd)
    return process.stderr


def execute_subprocess(cmd: str):
    process = subprocess.run(
        cmd, capture_output=True,                        
        text=True, check=True)
    return process


def time_stamp():
    timestamp = datetime.now().strftime("%d%s")
    return timestamp
