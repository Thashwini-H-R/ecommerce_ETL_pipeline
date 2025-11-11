param(
    [Parameter(Mandatory=$true)]
    [string]$Version
)

# Simple PowerShell release helper: creates annotated tag and pushes it.
Write-Host "Creating tag v$Version and pushing to origin"
git tag -a "v$Version" -m "chore(release): v$Version"
git push origin --tags
Write-Host "Pushed tags. Update CHANGELOG.md and create a GitHub Release if desired."
