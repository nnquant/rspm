$ErrorActionPreference = "Stop"

$Addr = if ($env:RSPM_SMOKE_ADDR) { $env:RSPM_SMOKE_ADDR } else { "127.0.0.1:27792" }
$Root = if ($env:RSPM_SMOKE_ROOT) { $env:RSPM_SMOKE_ROOT } else { "$env:TEMP\rspm-smoke" }
$LogDir = Join-Path $Root "logs"
$StateDir = Join-Path $Root "state"
$SocketPath = Join-Path $Root "run\rspmd.sock"
$ConfigPath = Join-Path $Root "tasks.rspm.toml"
$PythonCommand = $env:RSPM_SMOKE_PYTHON

function Invoke-Rspm {
  cargo run -p rspm -- `
    --addr $Addr `
    --log-dir $LogDir `
    --state-dir $StateDir `
    --socket-path $SocketPath `
    @args
  if ($LASTEXITCODE -ne 0) {
    throw "rspm command failed exit_code=[$LASTEXITCODE] args=[$args]"
  }
}

function Assert-Contains {
  param(
    [string] $Haystack,
    [string] $Needle,
    [string] $Label
  )
  if (-not $Haystack.Contains($Needle)) {
    throw "missing expected output [$Needle] while checking [$Label]"
  }
}

if (-not $PythonCommand) {
  $ResolvedPython = Get-Command python3 -ErrorAction SilentlyContinue
  if (-not $ResolvedPython) {
    $ResolvedPython = Get-Command python -ErrorAction SilentlyContinue
  }
  if (-not $ResolvedPython) {
    throw "python3 or python is required for examples/tasks.rspm.toml"
  }
  $PythonCommand = $ResolvedPython.Source
}

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
New-Item -ItemType Directory -Force -Path $StateDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $SocketPath) | Out-Null

$PythonToml = $PythonCommand.Replace("\", "\\").Replace('"', '\"')
(Get-Content examples/tasks.rspm.toml) |
  ForEach-Object {
    if ($_ -eq 'cmd = "python3"') {
      "cmd = `"$PythonToml`""
    } else {
      $_
    }
  } |
  Set-Content -Path $ConfigPath -Encoding utf8

try {
  $ApplyOutput = Invoke-Rspm apply -f $ConfigPath | Out-String
  Write-Output $ApplyOutput
  Assert-Contains $ApplyOutput "applied [rspm-simulated-tasks] tasks=4" "apply"
  Assert-Contains $ApplyOutput "task_id=2 market_feed" "apply"

  $DoctorOutput = Invoke-Rspm doctor --config $ConfigPath --log-dir $LogDir | Out-String
  Write-Output $DoctorOutput
  Assert-Contains $DoctorOutput "daemon: ok" "doctor"
  Assert-Contains $DoctorOutput "platform:" "doctor"
  Assert-Contains $DoctorOutput "default_addr:" "doctor"
  Assert-Contains $DoctorOutput "tasks: 4" "doctor"

  $ServiceStatusOutput = Invoke-Rspm service status --dry-run | Out-String
  Write-Output $ServiceStatusOutput
  Assert-Contains $ServiceStatusOutput "status command:" "service status dry-run"

  $ServiceStartOutput = Invoke-Rspm service start --dry-run | Out-String
  Write-Output $ServiceStartOutput
  Assert-Contains $ServiceStartOutput "start command:" "service start dry-run"

  $ServiceRestartOutput = Invoke-Rspm service restart --dry-run | Out-String
  Write-Output $ServiceRestartOutput
  Assert-Contains $ServiceRestartOutput "restart command:" "service restart dry-run"

  $LsOutput = Invoke-Rspm ls | Out-String
  Write-Output $LsOutput
  Assert-Contains $LsOutput "TASK_ID" "ls header"
  Assert-Contains $LsOutput "START_TIME" "ls header"
  Assert-Contains $LsOutput "STOP_TIME" "ls header"
  Assert-Contains $LsOutput "market_feed" "ls task"

  $StartOutput = Invoke-Rspm start 1 3 | Out-String
  Write-Output $StartOutput
  Assert-Contains $StartOutput "task_id=1 long_watcher" "start"
  Assert-Contains $StartOutput "task_id=3 oneshot_message" "start"
  Assert-Contains $StartOutput "TASK_ID" "post-start table"

  $LogOutput = Invoke-Rspm log all --no-follow --lines 20 --merge | Out-String
  Write-Output $LogOutput
  Assert-Contains $LogOutput "long_watcher |" "aggregate log prefix"
  Assert-Contains $LogOutput "oneshot_message |" "aggregate log prefix"

  $StopOutput = Invoke-Rspm stop all | Out-String
  Write-Output $StopOutput
  Assert-Contains $StopOutput "task_id=1 long_watcher stopped" "stop"
  Assert-Contains $StopOutput "task_id=2 market_feed stopped" "stop"
  Assert-Contains $StopOutput "TASK_ID" "post-stop table"
} finally {
  try {
    Invoke-Rspm daemon stop | Out-Null
  } catch {
    Write-Warning "failed to stop smoke daemon: $_"
  }
}
