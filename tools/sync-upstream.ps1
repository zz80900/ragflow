<#
.SYNOPSIS
Sync this private RAGFlow repository with the official upstream repository.

.DESCRIPTION
Fetches the official RAGFlow upstream branch and merges it into the current
working branch. The script refuses to run with uncommitted changes so local
development work is not overwritten accidentally.

.EXAMPLE
pwsh tools/sync-upstream.ps1

.EXAMPLE
pwsh tools/sync-upstream.ps1 -Push

.EXAMPLE
pwsh tools/sync-upstream.ps1 -DryRun
#>

[CmdletBinding()]
param(
    [string]$UpstreamName = "upstream",
    [string]$UpstreamUrl = "https://github.com/infiniflow/ragflow.git",
    [string]$UpstreamBranch = "main",
    [string]$OriginName = "origin",
    [switch]$Push,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Git {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    Write-Host "git $($Arguments -join ' ')" -ForegroundColor Cyan
    if (-not $DryRun) {
        & git @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "Git command failed: git $($Arguments -join ' ')"
        }
    }
}

$repoRoot = & git rev-parse --show-toplevel 2>$null
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($repoRoot)) {
    throw "This script must be run inside a Git repository."
}

Set-Location -LiteralPath $repoRoot.Trim()

$status = & git status --porcelain
if (-not [string]::IsNullOrWhiteSpace(($status -join ""))) {
    Write-Host "Working tree is not clean. Commit or stash local changes before syncing:" -ForegroundColor Yellow
    $status | ForEach-Object { Write-Host $_ }
    exit 1
}

$currentBranch = (& git branch --show-current).Trim()
if ([string]::IsNullOrWhiteSpace($currentBranch)) {
    throw "Detached HEAD is not supported. Switch to a branch before syncing."
}

$remoteNames = & git remote
if ($remoteNames -notcontains $UpstreamName) {
    Invoke-Git @("remote", "add", $UpstreamName, $UpstreamUrl)
} else {
    $currentUpstreamUrl = (& git remote get-url $UpstreamName).Trim()
    if ($currentUpstreamUrl -ne $UpstreamUrl) {
        Write-Host "Updating $UpstreamName URL from $currentUpstreamUrl to $UpstreamUrl" -ForegroundColor Yellow
        Invoke-Git @("remote", "set-url", $UpstreamName, $UpstreamUrl)
    }
}

Invoke-Git @("fetch", $UpstreamName, $UpstreamBranch, "--prune")

$upstreamRef = "$UpstreamName/$UpstreamBranch"
Invoke-Git @("merge", "--no-ff", $upstreamRef)

if ($Push) {
    Invoke-Git @("push", $OriginName, "${currentBranch}:${currentBranch}")
} else {
    Write-Host "Sync complete locally. Review and run 'git push $OriginName $currentBranch' when ready." -ForegroundColor Green
}
