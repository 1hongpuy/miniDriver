<#
Collect Windows-host evidence for a bounded MiniDriver storage probe window.

Run this on the Windows Hyper-V host while run-storage-sync-probe.sh is
running. It is read-only: it lists VM/VHD mapping and writes Performance
Counter samples to an output directory. It does not start, stop, migrate, or
modify any virtual machine.

Example (PowerShell as Administrator):
  .\windows-host-storage-capture.ps1 -DurationSeconds 90 -OutputDirectory C:\Temp\minidriver-storage-20260918
#>
[CmdletBinding()]
param(
    [ValidateRange(10, 3600)]
    [int]$DurationSeconds = 90,
    [string]$OutputDirectory = (Join-Path $env:TEMP ("minidriver-storage-" + (Get-Date -Format 'yyyyMMddTHHmmss')))
)

$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null

$started = Get-Date -Format o
@{
    started_at = $started
    computer_name = $env:COMPUTERNAME
    duration_seconds = $DurationSeconds
} | ConvertTo-Json | Set-Content -Encoding UTF8 (Join-Path $OutputDirectory 'manifest.json')

# VM/VHD/VHDX mapping, including virtual disk type. This is the missing link
# between the Linux guest block device and the Windows physical disk.
Get-VM | Select-Object Name, State, Generation, Path, ConfigurationLocation, SnapshotFileLocation |
    Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $OutputDirectory 'vms.csv')

Get-VM | ForEach-Object {
    $vm = $_
    Get-VMHardDiskDrive -VMName $vm.Name | ForEach-Object {
        $drive = $_
        $vhd = Get-VHD -Path $drive.Path
        [PSCustomObject]@{
            VMName = $vm.Name
            ControllerType = $drive.ControllerType
            ControllerNumber = $drive.ControllerNumber
            ControllerLocation = $drive.ControllerLocation
            VhdPath = $drive.Path
            VhdType = $vhd.VhdType
            VhdFormat = $vhd.VhdFormat
            VhdSizeBytes = $vhd.Size
            VhdFileSizeBytes = $vhd.FileSize
            ParentPath = $vhd.ParentPath
            FragmentationPercentage = $vhd.FragmentationPercentage
        }
    }
} | Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $OutputDirectory 'vm-vhd-mapping.csv')

Get-VMSnapshot | Select-Object VMName, Name, CreationTime, SnapshotType |
    Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $OutputDirectory 'vm-snapshots.csv')

$counters = @(
    '\PhysicalDisk(*)\Avg. Disk sec/Write',
    '\PhysicalDisk(*)\Avg. Disk sec/Read',
    '\PhysicalDisk(*)\Current Disk Queue Length',
    '\PhysicalDisk(*)\Disk Writes/sec',
    '\PhysicalDisk(*)\Disk Write Bytes/sec',
    '\Processor(_Total)\% Processor Time',
    '\Memory\Available MBytes',
    '\Memory\Pages/sec'
)

# Capture once so the BLG and CSV represent the same bounded probe window.
$samples = @(Get-Counter -Counter $counters -SampleInterval 1 -MaxSamples $DurationSeconds)
$samples | Export-Counter -Path (Join-Path $OutputDirectory 'performance-counter.blg') -FileFormat BLG
$samples | Export-Counter -Path (Join-Path $OutputDirectory 'performance-counter.csv') -FileFormat CSV

Write-Host "Windows host storage evidence: $OutputDirectory"
