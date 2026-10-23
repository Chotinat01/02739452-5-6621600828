"""Package only files selected by git and reject accidental local credentials."""
import os
import subprocess
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

root = Path(__file__).resolve().parents[1]
git = os.environ.get('GIT_EXE', 'git')
files = subprocess.check_output([git, 'ls-files', '-z'], cwd=root).decode('utf-8').split('\0')
files = [f for f in files if f]
assert files, 'Stage the intended project files before packaging'
assert '.env' not in files, '.env must never be packaged'
env_file = root / '.env'
secrets = []
if env_file.exists():
    for line in env_file.read_text(encoding='utf-8-sig').splitlines():
        if '=' not in line:
            continue
        name, value = line.split('=', 1)
        if any(word in name for word in ('PASSWORD', 'SECRET', 'FERNET', 'API_KEY')) and len(value) >= 16:
            secrets.append(value.encode())
for name in files:
    data = (root / name).read_bytes()
    assert not any(secret in data for secret in secrets), f'Local credential found in {name}; packaging blocked'
target = root.parent / '02739452-5-6621600828_submission.zip'
with ZipFile(target, 'w', ZIP_DEFLATED) as archive:
    for name in files:
        archive.write(root / name, '02739452-5-6621600828/' + name)
print(f'Packaged {len(files)} reviewed files; no local credentials found: {target}')
