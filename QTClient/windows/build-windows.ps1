param(
    [ValidateSet('Debug', 'Release', 'RelWithDebInfo')]
    [string]$Configuration = 'Release',
    [string]$QtRoot = $env:QT_ROOT,
    [string]$VcpkgRoot = $env:VCPKG_ROOT,
    [string]$BuildDirectory = 'build-windows'
)

$ErrorActionPreference = 'Stop'
$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
if ([string]::IsNullOrWhiteSpace($QtRoot)) { throw 'Set -QtRoot, e.g. C:\Qt\6.8.3\msvc2022_64.' }
if ([string]::IsNullOrWhiteSpace($VcpkgRoot)) { throw 'Set -VcpkgRoot to the vcpkg checkout containing OpenSSL.' }
$QtRoot = (Resolve-Path $QtRoot).Path
$VcpkgRoot = (Resolve-Path $VcpkgRoot).Path
$QtCmake = Join-Path $QtRoot 'lib\cmake\Qt6'
$DeployQt = Join-Path $QtRoot 'bin\windeployqt.exe'
$Toolchain = Join-Path $VcpkgRoot 'scripts\buildsystems\vcpkg.cmake'
foreach ($required in @($QtCmake, $DeployQt, $Toolchain)) {
    if (-not (Test-Path $required)) { throw "Required path does not exist: $required" }
}

$BuildPath = Join-Path $RepositoryRoot $BuildDirectory
cmake -S $RepositoryRoot -B $BuildPath -G 'Visual Studio 17 2022' -A x64 `
    "-DCMAKE_TOOLCHAIN_FILE=$Toolchain" "-DCMAKE_PREFIX_PATH=$QtRoot" `
    -DMINIKV_DESKTOP_CLIENT_ONLY=ON -DMINIKV_BUILD_QT_CLIENT=ON
if ($LASTEXITCODE -ne 0) { throw 'CMake configure failed.' }
cmake --build $BuildPath --config $Configuration --target minidriver_qt_client
if ($LASTEXITCODE -ne 0) { throw 'Qt client build failed.' }

$ExeCandidates = @(
    (Join-Path $BuildPath "QTClient\$Configuration\minidriver_qt_client.exe"),
    (Join-Path $BuildPath "bin\$Configuration\minidriver_qt_client.exe"),
    (Join-Path $BuildPath "QTClient\minidriver_qt_client.exe"),
    (Join-Path $BuildPath "bin\minidriver_qt_client.exe")
)
$Exe = $ExeCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if ($null -eq $Exe) { throw "Built executable was not found below $BuildPath" }
$Distribution = Join-Path $RepositoryRoot 'dist\MiniDriverQtClient'
New-Item -ItemType Directory -Force -Path $Distribution | Out-Null
Copy-Item -Force $Exe $Distribution
$DeployMode = if ($Configuration -eq 'Debug') { '--debug' } else { '--release' }
& $DeployQt $DeployMode --compiler-runtime --no-translations (Join-Path $Distribution 'minidriver_qt_client.exe')
if ($LASTEXITCODE -ne 0) { throw 'windeployqt failed.' }

# QtSql is linked by the client, but the SQLite driver is a runtime plugin.
# Fail packaging early if windeployqt did not copy it.
$SqliteDriver = Join-Path $Distribution 'sqldrivers\qsqlite.dll'
if (-not (Test-Path $SqliteDriver)) { throw "Qt SQLite driver was not deployed: $SqliteDriver" }

# Qt's deployer does not own the OpenSSL runtime used by the SDK.
$VcpkgBin = Join-Path $VcpkgRoot 'installed\x64-windows\bin'
Get-ChildItem -Path $VcpkgBin -Filter 'libcrypto*.dll' | Copy-Item -Destination $Distribution -Force
Get-ChildItem -Path $VcpkgBin -Filter 'libssl*.dll' | Copy-Item -Destination $Distribution -Force
Write-Host "Windows package ready: $Distribution"
