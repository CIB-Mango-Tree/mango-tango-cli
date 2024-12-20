# Check if running in PowerShell
if ($PSVersionTable -eq $null) {
    Write-Host "Please run this script in PowerShell."
    exit 1
}

# Define the virtual environment and requirements file paths
$repo_root = (Get-Location).Path
$venv_path = Join-Path $repo_root "venv"
$requirements_file = Join-Path $repo_root "requirements-dev.txt"

# Activate the virtual environment
$activate_script = Join-Path $venv_path "Scripts\Activate.ps1"
if (-Not (Test-Path $activate_script)) {
    Write-Host "Virtual environment not found. Please ensure it exists at: $venv_path"
    exit 1
}

Write-Host "Activating virtual environment..."
. $activate_script

# Install dependencies
if (-Not (Test-Path $requirements_file)) {
    Write-Host "requirements-dev.txt not found at: $requirements_file"
    exit 1
}

Write-Host "Installing dependencies from requirements-dev.txt..."
pip install -r $requirements_file

Write-Host "Bootstrap process complete."
