# PowerShell Script: Clear Cache and Restart Django Server
# Save as: restart_django.ps1

Write-Host "=== SMARTSHEET PRO - DJANGO RESTART SCRIPT ===" -ForegroundColor Green

# Step 1: Stop any running Django processes
Write-Host "Step 1: Stopping Django processes..." -ForegroundColor Yellow
Get-Process | Where-Object {$_.ProcessName -like "*python*" -and $_.CommandLine -like "*runserver*"} | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# Step 2: Clear Python cache
Write-Host "Step 2: Clearing Python cache..." -ForegroundColor Yellow
Set-Location "D:\himangi\smartsheet-pro\backend"

# Remove __pycache__ directories
Get-ChildItem -Path . -Recurse -Directory -Name "__pycache__" | ForEach-Object {
    $path = Join-Path -Path $PWD -ChildPath $_
    Write-Host "Removing: $path"
    Remove-Item -Path $path -Recurse -Force -ErrorAction SilentlyContinue
}

# Remove .pyc files
Get-ChildItem -Path . -Recurse -File -Name "*.pyc" | ForEach-Object {
    $path = Join-Path -Path $PWD -ChildPath $_
    Write-Host "Removing: $path"
    Remove-Item -Path $path -Force -ErrorAction SilentlyContinue
}

Write-Host "Cache cleared successfully!" -ForegroundColor Green

# Step 3: Verify files are in place
Write-Host "Step 3: Verifying files..." -ForegroundColor Yellow
$files = @(
    "reports\views.py",
    "reports\urls.py",
    "config\urls.py"
)

foreach ($file in $files) {
    if (Test-Path $file) {
        Write-Host "✅ $file exists" -ForegroundColor Green
    } else {
        Write-Host "❌ $file missing" -ForegroundColor Red
    }
}

# Step 4: Test imports
Write-Host "Step 4: Testing imports..." -ForegroundColor Yellow
python -c "
import sys
sys.path.append('.')
import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

try:
    from reports.views import ReportViewSet
    print('✅ ReportViewSet imported successfully')
except Exception as e:
    print(f'❌ Import error: {e}')

try:
    from reports.urls import router
    print('✅ Router imported successfully')
    print(f'   Registered URLs: {len(router.urls)}')
except Exception as e:
    print(f'❌ Router error: {e}')
"

# Step 5: Run URL diagnostic
Write-Host "Step 5: Running URL diagnostic..." -ForegroundColor Yellow
python debug_urls.py

# Step 6: Start Django server
Write-Host "Step 6: Starting Django server..." -ForegroundColor Yellow
Write-Host "Server will start in 3 seconds..." -ForegroundColor Cyan
Start-Sleep -Seconds 3

# Activate virtual environment and start server
& "D:\himangi\smartsheet-pro\backend\venv\Scripts\Activate.ps1"
python manage.py runserver 8000

Write-Host "=== SCRIPT COMPLETED ===" -ForegroundColor Green