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

function Assert-BundledRuntime([string]$Root) {
    $runtime = Get-ChildItem -LiteralPath $Root -Directory -Recurse -Filter "cmclient-runtime" |
        Select-Object -First 1
    if ($null -eq $runtime) {
        throw "NATIVE_DESKTOP_RUNTIME_MISSING"
    }
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

try {
    $msi = Join-Path $Stage "cmclient-desktop-$Target-$Version.msi"
    $msiRoot = Join-Path $temporary "msi"
    New-Item -ItemType Directory -Force $msiRoot | Out-Null
    & msiexec.exe /a $msi /qn "TARGETDIR=$msiRoot"
    if ($LASTEXITCODE -ne 0) {
        throw "NATIVE_DESKTOP_MSI_EXTRACT_FAILED"
    }
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
