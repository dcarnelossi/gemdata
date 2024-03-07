from pathlib import Path

def load_reqs(file: str = 'requirements.txt'):
    with open(Path(__file__).parent / file, encoding="utf8", errors='ignore') as f:
        return "\n".join(f.readlines())