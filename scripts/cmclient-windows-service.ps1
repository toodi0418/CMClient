[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("install", "uninstall", "start", "stop", "restart", "status", "render")]
    [string]$Command,
    [string]$HostPath = (Join-Path $PSScriptRoot "..\bin\cmclient-service-host.exe"),
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
}
