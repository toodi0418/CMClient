[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Stage,
    [Parameter(Mandatory = $true)]
    [string]$Target,
    [Parameter(Mandatory = $true)]
    [string]$Version
)

$ErrorActionPreference = "Stop"
if ($Target -ne "windows-x86_64") {
    throw "NATIVE_DESKTOP_SMOKE_TARGET_UNSUPPORTED"
}

$Stage = (Resolve-Path -LiteralPath $Stage).Path
& node scripts/desktop-native-bundles.mjs verify-stage --target $Target --version $Version --input $Stage
if ($LASTEXITCODE -ne 0) {
    throw "NATIVE_DESKTOP_STAGE_INVALID"
}

$temporary = Join-Path $env:TEMP "cmclient-native-desktop-$([Guid]::NewGuid().ToString('N'))"
New-Item -ItemType Directory -Force $temporary | Out-Null

function Find-BundledRuntime([string]$Root) {
    # Locate the contract manifest rather than relying on provider-specific
    # directory filtering after an administrative MSI extraction.
    $manifest = Get-ChildItem -LiteralPath $Root -File -Recurse -Force -Filter "build-manifest.json" -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($null -eq $manifest) {
        $hints = Get-ChildItem -LiteralPath $Root -Recurse -Force -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -match '(?i)cmclient-runtime|build-manifest' } |
            Select-Object -First 20
        $hints | ForEach-Object { Write-Host "resource hint: $($_.FullName)" }
        throw "NATIVE_DESKTOP_RUNTIME_MISSING"
    }
    $runtime = $manifest.Directory
    if ($runtime.Name -ne "cmclient-runtime") {
        throw "NATIVE_DESKTOP_RUNTIME_PATH_INVALID"
    }
    return $runtime
}

function Assert-BundledRuntime([string]$Root) {
    $runtime = Find-BundledRuntime $Root
    & node scripts/desktop-native-bundles.mjs verify-runtime --target $Target --version $Version --input $runtime.FullName
    if ($LASTEXITCODE -ne 0) {
        throw "NATIVE_DESKTOP_RUNTIME_INVALID"
    }
}

function Assert-NativeAppLaunch([string]$Root) {
    $candidates = Get-ChildItem -LiteralPath $Root -File -Recurse -Filter "*.exe" |
        Where-Object {
            $_.FullName -notmatch '[\\/]cmclient-runtime[\\/]' -and
            $_.FullName -notmatch '[\\/]\$PLUGINSDIR[\\/]' -and
            $_.Name -notmatch '(?i)uninstall|setup'
        }
    $application = $candidates |
        Sort-Object @{ Expression = { if ($_.Name -match '(?i)^cmclient(?:-desktop)?\.exe$') { 0 } else { 1 } } }, FullName |
        Select-Object -First 1
    if ($null -eq $application) {
        throw "NATIVE_DESKTOP_APP_BINARY_MISSING"
    }
    $process = Start-Process -FilePath $application.FullName -PassThru
    try {
        Start-Sleep -Seconds 2
        if ($process.HasExited) {
            throw "NATIVE_DESKTOP_APP_EXITED"
        }
    }
    finally {
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
    }
}

function Invoke-MsiAdministrativeExtract([string]$Package, [string]$Destination, [string]$LogPath) {
    $arguments = @(
        "/a"
        ('"{0}"' -f $Package)
        "/qn"
        ('TARGETDIR="{0}"' -f $Destination)
        "/L*v"
        ('"{0}"' -f $LogPath)
    )
    $process = Start-Process `
        -FilePath (Join-Path $env:SystemRoot "System32\msiexec.exe") `
        -ArgumentList $arguments `
        -Wait `
        -PassThru `
        -WindowStyle Hidden
    if ($process.ExitCode -ne 0) {
        Write-Host "msiexec exit code: $($process.ExitCode)"
        Get-Content -LiteralPath $LogPath -Tail 80 -ErrorAction SilentlyContinue
        throw "NATIVE_DESKTOP_MSI_EXTRACT_FAILED"
    }
}

try {
    $msi = Join-Path $Stage "cmclient-desktop-$Target-$Version.msi"
    $msiRoot = Join-Path $temporary "msi"
    $msiLog = Join-Path $temporary "msiexec.log"
    New-Item -ItemType Directory -Force $msiRoot | Out-Null
    Invoke-MsiAdministrativeExtract $msi $msiRoot $msiLog
    Assert-BundledRuntime $msiRoot
    Assert-NativeAppLaunch $msiRoot

    $nsis = Join-Path $Stage "cmclient-desktop-$Target-$Version.setup.exe"
    $nsisRoot = Join-Path $temporary "nsis"
    New-Item -ItemType Directory -Force $nsisRoot | Out-Null
    & 7z.exe x -y "-o$nsisRoot" $nsis | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "NATIVE_DESKTOP_NSIS_EXTRACT_FAILED"
    }
    Assert-BundledRuntime $nsisRoot
    Assert-NativeAppLaunch $nsisRoot
}
finally {
    Remove-Item -LiteralPath $temporary -Recurse -Force -ErrorAction SilentlyContinue
}
