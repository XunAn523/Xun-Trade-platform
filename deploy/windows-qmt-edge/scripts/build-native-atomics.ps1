[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$SourceRoot,
    [Parameter(Mandatory = $true)]
    [string]$OutputDirectory
)

$ErrorActionPreference = 'Stop'

if (-not [Environment]::Is64BitOperatingSystem) {
    throw 'TradeCore QMT atomics require 64-bit Windows.'
}

$compiler = Get-Command cl.exe -ErrorAction SilentlyContinue
if ($null -eq $compiler) {
    throw 'cl.exe was not found. Run this script from an x64 Visual Studio Developer PowerShell.'
}

$source = Join-Path $SourceRoot 'deploy\windows-qmt-edge\native\tradecore_qmt_atomic.c'
if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
    throw 'The TradeCore QMT atomic C source is missing.'
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$output = Join-Path $OutputDirectory 'tradecore_qmt_atomic.dll'

# The C source owns WIN32_LEAN_AND_MEAN so the compiler invocation must not
# define it again: under /W4 /WX that otherwise becomes C4005/C2220 and turns
# an otherwise valid audited build into a false deployment failure.
& $compiler.Source /nologo /LD /O2 /W4 /WX $source "/Fe:$output"
if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $output -PathType Leaf)) {
    throw 'Building the TradeCore QMT atomic DLL failed.'
}

Get-FileHash -Algorithm SHA256 -LiteralPath $output | Select-Object Algorithm, Hash, Path
