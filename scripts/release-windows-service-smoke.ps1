[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Bundle,
    [Parameter(Mandatory = $true)]
    [string]$Version,
    [Parameter(Mandatory = $true)]
    [string]$Commit,
    [Parameter(Mandatory = $true)]
    [string]$NodePath
)

$ErrorActionPreference = "Stop"
$ServiceName = "CMClientAgent"
$SentinelValue = "must survive final service lifecycle"

function Invoke-ServiceManager([string]$Manager, [string]$Command, [string]$HostPath, [switch]$NoStart) {
    $arguments = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $Manager,
        $Command,
        "-HostPath", $HostPath,
        "-ServiceName", $ServiceName
    )
    if ($NoStart) {
        $arguments += "-NoStart"
    }
    & powershell.exe @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "RELEASE_SERVICE_MANAGER_FAILED"
    }
}

function Wait-ServiceState([string]$Expected, [int]$Attempts) {
    for ($attempt = 0; $attempt -lt $Attempts; $attempt++) {
        $service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
        if ($null -ne $service -and $service.Status.ToString() -eq $Expected) {
            return $true
        }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

function Get-AgentStatus([string]$CliPath) {
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $statusJson = & $CliPath --json status 2>$null
        if ($LASTEXITCODE -eq 0) {
            try {
                $status = $statusJson | ConvertFrom-Json
                if ($status.agent -eq "running" -and $status.agent_version -eq $Version) {
                    return $status
                }
            }
            catch {}
        }
        Start-Sleep -Milliseconds 500
    }
    throw "RELEASE_SERVICE_AGENT_HEALTH_FAILED"
}

function Get-ExpectedChannel([string]$ReleaseVersion) {
    if ($ReleaseVersion -match '-dev\.') {
        return "dev"
    }
    if ($ReleaseVersion.Contains("-")) {
        return "beta"
    }
    return "stable"
}

if ($Version -notmatch '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$') {
    throw "RELEASE_SERVICE_VERSION_INVALID"
}
if ($Commit -notmatch '^[a-f0-9]{40}$') {
    throw "RELEASE_SERVICE_COMMIT_INVALID"
}

$Bundle = (Resolve-Path -LiteralPath $Bundle).Path
$NodePath = (Resolve-Path -LiteralPath $NodePath).Path
$bundlePrefix = $Bundle.TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
if ($NodePath.StartsWith($bundlePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw "RELEASE_SERVICE_NODE_MUST_BE_EXTERNAL"
}
if ([IO.Path]::GetFileName($NodePath) -ne "node.exe") {
    throw "RELEASE_SERVICE_NODE_INVALID"
}
if (Get-ChildItem -LiteralPath $Bundle -File -Recurse -Filter "node.exe") {
    throw "RELEASE_SERVICE_BUNDLED_NODE_FORBIDDEN"
}

$manager = Join-Path $Bundle "scripts/cmclient-windows-service.ps1"
$hostPath = Join-Path $Bundle "bin/cmclient-service-host.exe"
$agentPath = Join-Path $Bundle "bin/cmclient-agent.exe"
$cliPath = Join-Path $Bundle "bin/cmclient.exe"
$requiredPaths = @(
    $manager,
    $hostPath,
    $agentPath,
    $cliPath,
    (Join-Path $Bundle "bin/cmclient-migrate.exe"),
    (Join-Path $Bundle "gateway/dist/main.js"),
    (Join-Path $Bundle "gateway/node_modules"),
    (Join-Path $Bundle "web/index.html"),
    (Join-Path $Bundle "proto/meshtastic/mesh.proto")
)
foreach ($path in $requiredPaths) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "RELEASE_SERVICE_COMPOSITION_INCOMPLETE"
    }
}

$nodeVersionText = & $NodePath --version
$nodeVersionMatch = [regex]::Match([string]$nodeVersionText, '^v([0-9]+)\.([0-9]+)\.([0-9]+)$')
if ($LASTEXITCODE -ne 0 -or -not $nodeVersionMatch.Success) {
    throw "RELEASE_SERVICE_NODE_INVALID"
}
$nodeMajor = [int]$nodeVersionMatch.Groups[1].Value
$nodeMinor = [int]$nodeVersionMatch.Groups[2].Value
if (-not (($nodeMajor -eq 22 -and $nodeMinor -ge 18) -or ($nodeMajor -eq 24 -and $nodeMinor -ge 11) -or $nodeMajor -gt 24)) {
    throw "RELEASE_SERVICE_NODE_VERSION_UNSUPPORTED"
}

if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    throw "RELEASE_SERVICE_NAME_IN_USE"
}

$stateRoot = Join-Path $env:ProgramData "CMClient\data"
$runIdentity = if ([string]::IsNullOrWhiteSpace($env:GITHUB_RUN_ID)) { [Guid]::NewGuid().ToString("N") } else { $env:GITHUB_RUN_ID }
$statePath = Join-Path $stateRoot "retained-release-smoke-$runIdentity"
New-Item -ItemType Directory -Force $stateRoot | Out-Null
[IO.File]::WriteAllText($statePath, $SentinelValue)
if (-not (Test-Path -LiteralPath $statePath -PathType Leaf) -or [IO.File]::ReadAllText($statePath) -ne $SentinelValue) {
    throw "RELEASE_SERVICE_STATE_FIXTURE_INVALID"
}

try {
    Invoke-ServiceManager $manager "install" $hostPath -NoStart

    $serviceRegistry = "HKLM:\SYSTEM\CurrentControlSet\Services\$ServiceName"
    $nodeDirectory = Split-Path -Parent $NodePath
    $machinePath = [Environment]::GetEnvironmentVariable("Path", [EnvironmentVariableTarget]::Machine)
    $servicePath = if ([string]::IsNullOrWhiteSpace($machinePath)) { $nodeDirectory } else { "$nodeDirectory;$machinePath" }
    New-ItemProperty -Path $serviceRegistry -Name "Environment" -PropertyType MultiString -Value @("PATH=$servicePath") -Force | Out-Null

    Start-Service -Name $ServiceName
    if (-not (Wait-ServiceState "Running" 60)) {
        throw "RELEASE_SERVICE_START_FAILED"
    }

    $status = Get-AgentStatus $cliPath
    $agent = Get-CimInstance Win32_Process -Filter "Name = 'cmclient-agent.exe'" |
        Where-Object { $_.ExecutablePath -eq $agentPath } |
        Select-Object -First 1
    if ($null -eq $agent) {
        throw "RELEASE_SERVICE_ADJACENT_AGENT_MISSING"
    }

    $startJson = & $cliPath --json start 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "RELEASE_SERVICE_GATEWAY_START_FAILED"
    }
    $startStatus = $startJson | ConvertFrom-Json
    if ($startStatus.agent_version -ne $Version) {
        throw "RELEASE_SERVICE_GATEWAY_START_FAILED"
    }

    $expectedChannel = Get-ExpectedChannel $Version
    $versionProjection = $null
    for ($attempt = 0; $attempt -lt 120 -and $null -eq $versionProjection; $attempt++) {
        $statusJson = & $cliPath --json status 2>$null
        if ($LASTEXITCODE -eq 0) {
            try {
                $status = $statusJson | ConvertFrom-Json
            }
            catch {
                $status = $null
            }
        }
        else {
            $status = $null
        }
        if (-not [string]::IsNullOrWhiteSpace($status.management_web_url)) {
            $baseUrl = $status.management_web_url.TrimEnd('/')
            try {
                $health = Invoke-RestMethod -Method Get -Uri "$baseUrl/api/v1/system/health" -TimeoutSec 2
                $candidate = Invoke-RestMethod -Method Get -Uri "$baseUrl/api/v1/system/version" -TimeoutSec 2
                if ($health.status -eq "ok" -and $candidate.version -eq $Version -and $candidate.commit -eq $Commit -and $candidate.channel -eq $expectedChannel) {
                    $versionProjection = $candidate
                }
            }
            catch {}
        }
        if ($null -eq $versionProjection) {
            Start-Sleep -Milliseconds 500
        }
    }
    if ($null -eq $versionProjection) {
        throw "RELEASE_SERVICE_GATEWAY_IDENTITY_FAILED"
    }

    $gatewayNode = Get-CimInstance Win32_Process -Filter "Name = 'node.exe'" |
        Where-Object { $_.ParentProcessId -eq $agent.ProcessId -and $_.ExecutablePath -eq $NodePath } |
        Select-Object -First 1
    if ($null -eq $gatewayNode) {
        throw "RELEASE_SERVICE_EXTERNAL_NODE_NOT_USED"
    }

    Invoke-ServiceManager $manager "uninstall" $hostPath
    for ($attempt = 0; $attempt -lt 40 -and (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue); $attempt++) {
        Start-Sleep -Milliseconds 250
    }
    if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
        throw "RELEASE_SERVICE_REMOVE_FAILED"
    }
    if (-not (Test-Path -LiteralPath $statePath -PathType Leaf) -or [IO.File]::ReadAllText($statePath) -ne $SentinelValue) {
        throw "RELEASE_SERVICE_STATE_NOT_RETAINED"
    }
}
finally {
    if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
        try {
            Invoke-ServiceManager $manager "uninstall" $hostPath
        }
        catch {}
    }
    Remove-Item -LiteralPath $statePath -Force -ErrorAction SilentlyContinue
}
