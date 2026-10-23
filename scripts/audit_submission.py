"""Read-only submission checks; never print credential values or Git blob contents."""
import io
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from pypdf import PdfReader

root = Path(__file__).resolve().parents[1]
git = os.environ.get('GIT_EXE', 'git')


def command(*args):
    return subprocess.check_output([git, *args], cwd=root)


required = ['dags/recipe_pipeline.py', 'web/app.py', 'docker-compose.yml', 'requirements.txt',
            'README.md', '.env.example', 'output/pdf/5_6621600828_summary.pdf']
tracked = set(command('ls-files', '-z').decode().strip('\0').split('\0'))
checks = {f'required:{name}': name in tracked and (root / name).is_file() for name in required}
historical_paths = command('log', '--all', '--name-only', '--format=').decode().splitlines()
checks['no_dotenv_committed_in_history'] = not any(Path(p).name == '.env' for p in historical_paths)
secrets = []
for line in (root / '.env').read_text(encoding='utf-8-sig').splitlines():
    if '=' in line:
        key, value = line.split('=', 1)
        if any(word in key for word in ('PASSWORD', 'SECRET', 'FERNET', 'API_KEY')) and len(value) >= 16:
            secrets.append(value.encode())
objects = list(dict.fromkeys(line.split()[0] for line in command('rev-list', '--objects', '--all').splitlines()))
batch = subprocess.run([git, 'cat-file', '--batch'], cwd=root,
                       input=b'\n'.join(objects) + b'\n', stdout=subprocess.PIPE, check=True)
stream = io.BytesIO(batch.stdout)
secret_free, blobs_checked = True, 0
for _ in objects:
    _, kind, size = stream.readline().split()
    content = stream.read(int(size))
    assert stream.read(1) == b'\n'
    if kind == b'blob':
        blobs_checked += 1
        secret_free &= not any(secret in content for secret in secrets)
checks['current_local_credentials_absent_from_all_git_blobs'] = secret_free

pdf = PdfReader(root / 'output/pdf/5_6621600828_summary.pdf')
text = '\n'.join(page.extract_text() for page in pdf.pages)
checks['pdf_at_most_3_pages'] = len(pdf.pages) <= 3
checks['pdf_has_repo_url'] = 'github.com/Chotinat01/02739452-5-6621600828' in text
checks['pdf_graph_grid_and_dashboard_images'] = sum(len(page.images) for page in pdf.pages) >= 3
checks['pdf_graph_and_grid_labels'] = 'Graph view' in text and 'Grid view' in text
checks['pdf_two_real_problems'] = 'PermissionError' in text and 'ChunkedEncodingError' in text
checks['no_selenium_dependency'] = 'selenium' not in (root / 'requirements.txt').read_text().casefold()
verification = json.loads((root / 'evidence/verification.json').read_text())
checks['scheduled_three_source_verification_passed'] = verification.get('passed', False)
checks['restored_all_three_sources'] = verification['runs'][-1]['quality']['all_three_sources_available']
result = {'checked_at_utc': datetime.now(timezone.utc).isoformat(),
          'audited_commit': command('rev-parse', 'HEAD').decode().strip(),
          'commit_time': command('show', '-s', '--format=%cI', 'HEAD').decode().strip(),
          'checks': checks, 'git_blobs_checked': blobs_checked, 'pdf_pages': len(pdf.pages),
          'all_static_checks_passed': all(checks.values())}
output = root / 'evidence/submission_audit.json'
output.write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps(result, indent=2))
assert result['all_static_checks_passed'], 'Submission audit found a failed check'
