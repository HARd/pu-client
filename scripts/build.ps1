$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $RootDir

$PythonBin = if (Get-Command python -ErrorAction SilentlyContinue) { "python" } elseif (Get-Command py -ErrorAction SilentlyContinue) { "py" } else { $null }

if (-not $PythonBin) {
  Write-Host "Python not found in PATH."
  exit 1
}

if ($PythonBin -eq "py") { & py -m PyInstaller --version } else { & python -m PyInstaller --version }
if (-not $?) {
  Write-Host "PyInstaller module not found for $PythonBin. Install build deps first:"
  if ($PythonBin -eq "py") {
    Write-Host "  py -m pip install -r requirements-build.txt"
  } else {
    Write-Host "  python -m pip install -r requirements-build.txt"
  }
  exit 1
}

if ($PythonBin -eq "py") { & py -c "import requests; import PySide6" } else { & python -c "import requests; import PySide6" }
if (-not $?) {
  Write-Host "Runtime dependencies are missing for $PythonBin. Install app deps first:"
  if ($PythonBin -eq "py") {
    Write-Host "  py -m pip install -r requirements.txt"
  } else {
    Write-Host "  python -m pip install -r requirements.txt"
  }
  exit 1
}

if (Test-Path build) { Remove-Item build -Recurse -Force }
if (Test-Path dist) { Remove-Item dist -Recurse -Force }

if ($PythonBin -eq "py") { & py scripts/prepare_icons.py } else { & python scripts/prepare_icons.py }
if (-not $?) {
  Write-Host "Failed to prepare app icons."
  exit 1
}

$IconPath = Join-Path $RootDir "build\\icons\\app-icon.ico"
$PyArgs = @(
  "-m", "PyInstaller",
  "--name", "playua-desktop-client",
  "--onefile",
  "--windowed",
  "--noconfirm",
  "--add-data", "assets/icon.png;assets"
)
if (Test-Path $IconPath) {
  $PyArgs += @("--icon", $IconPath)
}
$PyArgs += "app/main.py"

if ($PythonBin -eq "py") { & py @PyArgs } else { & python @PyArgs }

Write-Host ""
Write-Host "Build finished. Output in: $RootDir/dist"
