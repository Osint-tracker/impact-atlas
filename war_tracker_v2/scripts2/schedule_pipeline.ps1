# schedule_pipeline.ps1
# Automates the scheduling of the Impact Atlas OSINT Pipeline (8-hour cycle)

$projectRoot = "c:\Users\lucag\.vscode\cli\osint-tracker"
$orchestrator = "$projectRoot\war_tracker_v2\scripts2\auto_pipeline.py"
$venvPython = "$projectRoot\.venv\Scripts\python.exe"

# 1. Select the best Python interpreter
if (Test-Path $venvPython) {
    $pythonExe = $venvPython
    Write-Host "[*] Using VENV Python: $pythonExe"
} else {
    $pythonExe = "python.exe"
    Write-Host "[!] VENV not found or non-standard. Using system Python."
}

# 2. Define the Action
$action = New-ScheduledTaskAction -Execute $pythonExe `
    -Argument "`"$orchestrator`"" `
    -WorkingDirectory "$projectRoot\war_tracker_v2\scripts2\"

# 3. Define the Trigger (Every 8 hours starting NOW)
# Using a 1-minute delay for the first run to avoid overlap with current session if needed
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Hours 8)

# 4. Register the Task
# Note: We run it as the current user to preserve environment variables and PATH access.
Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "ImpactAtlas_Pipeline_8h" -Description "Runs the OSINT Download -> Embedding -> Event Creation pipeline every 8 hours." -Force

Write-Host "`n✅ Pipeline scheduled successfully!" -ForegroundColor Green
Write-Host "Task Name: ImpactAtlas_Pipeline_8h"
Write-Host "Frequency: Every 8 hours"
Write-Host "First Run: $((Get-Date).AddMinutes(1).ToString('yyyy-MM-dd HH:mm:ss'))"
