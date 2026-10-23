$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (Test-Path -LiteralPath '.env') { Write-Host '.env already exists; credentials preserved.'; exit 0 }
function New-Secret([int]$Length = 32) {
    $bytes = New-Object byte[] $Length
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    return ([BitConverter]::ToString($bytes)).Replace('-','').ToLower()
}
$dbPassword = New-Secret
$readerPassword = New-Secret
$adminPassword = New-Secret 12
$fernetBytes = New-Object byte[] 32
[System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($fernetBytes)
$fernetKey = [Convert]::ToBase64String($fernetBytes).Replace('+','-').Replace('/','_')
$secretKey = New-Secret
@"
POSTGRES_USER=recipe_admin
POSTGRES_PASSWORD=$dbPassword
POSTGRES_DB=airflow
PIPELINE_DATABASE_URL=postgresql://recipe_admin:${dbPassword}@postgres:5432/airflow
DASHBOARD_DATABASE_URL=postgresql://recipe_reader:${readerPassword}@postgres:5432/airflow
DASHBOARD_PASSWORD=$readerPassword
AIRFLOW_ADMIN_USER=student
AIRFLOW_ADMIN_PASSWORD=$adminPassword
AIRFLOW_FERNET_KEY=$fernetKey
AIRFLOW_SECRET_KEY=$secretKey
SPOONACULAR_API_KEY=
AIRFLOW_PORT=8080
DASHBOARD_PORT=8501
"@ | Set-Content -LiteralPath '.env' -Encoding utf8
Write-Host 'Created .env with unique local credentials. Read Airflow login from .env locally.'
