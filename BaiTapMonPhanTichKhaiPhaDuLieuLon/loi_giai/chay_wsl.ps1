param(
    [ValidateSet('all', 'mapreduce', 'sql', 'ml', 'aml')]
    [string]$Only = 'all',
    [string]$Distro = 'Ubuntu',
    [switch]$SkipTests
)
$ErrorActionPreference = 'Stop'
$labsRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$labsLinuxRoot = & wsl.exe -d $Distro -- wslpath -u $labsRoot.Replace('\', '/')
if ($LASTEXITCODE -ne 0) { throw 'Could not convert the project path to WSL.' }
$labsLinuxRoot = $labsLinuxRoot.Trim()
$labsCommand = "source loi_giai/kich_hoat_moi_truong.sh && python loi_giai/chay_bai_tap.py --only $Only"
if ($SkipTests) { $labsCommand += ' --skip-tests' }
& wsl.exe -d $Distro --cd $labsLinuxRoot -- bash -c $labsCommand
exit $LASTEXITCODE
