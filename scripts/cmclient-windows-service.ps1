[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("install", "uninstall", "start", "stop", "restart", "status", "logs", "render")]
    [string]$Command,
    [string]$HostPath = (Join-Path $PSScriptRoot "..\bin\cmclient-service-host.exe"),
    [string]$Lines = "200",
    [switch]$NoStart
)

$ErrorActionPreference = "Stop"
$ServiceName = "CMClientAgent"
$DisplayName = "CMClient Agent"

function Assert-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "WINDOWS_SERVICE_PRIVILEGE_REQUIRED"
    }
}

function Test-IsWindowsAbsolutePath([string]$Path) {
    if ([string]::IsNullOrWhiteSpace($Path)) {
        return $false
    }

    # Device namespaces are not regular drive or UNC file paths and can bypass
    # normal Win32 path handling.
    if ($Path.StartsWith('\\?\') -or $Path.StartsWith('\\.\')) {
        return $false
    }

    if ($Path -match '^[A-Za-z]:[\\/]') {
        return $true
    }

    return $Path -match '^\\\\[^\\/:*?"<>|]+\\[^\\/:*?"<>|]+(?:\\|$)'
}

function Assert-SafeAbsolutePath([string]$Path) {
    if (-not (Test-IsWindowsAbsolutePath $Path) -or $Path.Contains('"') -or $Path.Contains("`n") -or $Path.Contains("`r")) {
        throw "WINDOWS_SERVICE_PATH_INVALID"
    }
}

function Invoke-Sc([string[]]$Arguments) {
    & sc.exe @Arguments | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "WINDOWS_SERVICE_SCM_FAILED"
    }
}

$LineCount = 0
if (-not [int]::TryParse($Lines, [ref]$LineCount) -or $LineCount -lt 1 -or $LineCount -gt 10000) {
    throw "WINDOWS_SERVICE_LOG_LINES_INVALID"
}

function Show-ServiceLogs {
    $programData = [Environment]::GetFolderPath([Environment+SpecialFolder]::CommonApplicationData)
    $logDirectory = Join-Path $programData "CMClient\logs"
    $logNames = @("service-host.jsonl", "agent.jsonl", "gateway.jsonl")
    $availableLogs = @()

    foreach ($logName in $logNames) {
        $legacyLog = Join-Path $logDirectory $logName
        $selectedLog = $null
        if (Test-Path -LiteralPath $legacyLog) {
            $item = Get-Item -LiteralPath $legacyLog -Force
            if ($item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
                throw "WINDOWS_SERVICE_LOG_FILE_INVALID"
            }
            $selectedLog = $item.FullName
        }

        $datedPattern = ('^{0}\.\d{{4}}-\d{{2}}-\d{{2}}$' -f [regex]::Escape($logName))
        $datedLogs = @(
            Get-ChildItem -LiteralPath $logDirectory -Force -ErrorAction SilentlyContinue |
                Where-Object { $_.Name -match $datedPattern } |
                Sort-Object Name -Descending
        )
        foreach ($item in $datedLogs) {
            if ($item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
                throw "WINDOWS_SERVICE_LOG_FILE_INVALID"
            }
            $stamp = $item.Name.Substring($logName.Length + 1)
            $parsedDate = [DateTime]::MinValue
            if (-not [DateTime]::TryParseExact($stamp, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::None, [ref]$parsedDate)) {
                throw "WINDOWS_SERVICE_LOG_FILE_INVALID"
            }
        }
        if ($datedLogs.Count -gt 0) {
            $selectedLog = $datedLogs[0].FullName
        }
        if ($null -ne $selectedLog) {
            $availableLogs += $selectedLog
        }
    }

    if ($availableLogs.Count -eq 0) {
        throw "WINDOWS_SERVICE_LOGS_UNAVAILABLE"
    }

    foreach ($logFile in $availableLogs) {
        Get-Content -LiteralPath $logFile -Tail $LineCount
    }
}

Assert-SafeAbsolutePath $HostPath
$BinaryPath = ('"{0}" --service' -f $HostPath)

if ($Command -eq "render") {
    [pscustomobject]@{
        serviceName = $ServiceName
        displayName = $DisplayName
        binaryPath = $BinaryPath
        account = "NT AUTHORITY\LocalService"
        storesCredentials = $false
    } | ConvertTo-Json -Compress
    exit 0
}

Assert-Administrator

switch ($Command) {
    "install" {
        if (-not (Test-Path -LiteralPath $HostPath -PathType Leaf)) {
            throw "WINDOWS_SERVICE_HOST_MISSING"
        }
        $existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
        if ($null -eq $existing) {
            Invoke-Sc @("create", $ServiceName, "binPath=", $BinaryPath, "start=", "auto", "obj=", "NT AUTHORITY\LocalService")
        }
        else {
            Invoke-Sc @("config", $ServiceName, "binPath=", $BinaryPath, "start=", "auto", "obj=", "NT AUTHORITY\LocalService")
        }
        Invoke-Sc @("description", $ServiceName, "CMClient Agent service host")
        Invoke-Sc @("failure", $ServiceName, "reset=", "86400", "actions=", "restart/5000/restart/5000/none/0")
        if (-not $NoStart) {
            Start-Service -Name $ServiceName
        }
    }
    "uninstall" {
        $existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
        if ($null -ne $existing) {
            if ($existing.Status -ne "Stopped") {
                Stop-Service -Name $ServiceName -Force
            }
            Invoke-Sc @("delete", $ServiceName)
        }
    }
    "start" { Start-Service -Name $ServiceName }
    "stop" { Stop-Service -Name $ServiceName }
    "restart" { Restart-Service -Name $ServiceName -Force }
    "status" { Get-Service -Name $ServiceName | Select-Object Name, DisplayName, Status, StartType | Format-Table -AutoSize }
    "logs" { Show-ServiceLogs }
}
