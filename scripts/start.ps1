$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
& "$PSScriptRoot/setup.ps1"
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose failed' }
Write-Host 'Airflow: http://localhost:8080 | Dashboard: http://localhost:8501'
