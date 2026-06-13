# PowerShell build script for Poop on Windows

$MINOR = git rev-list --count HEAD 2>$null
if ($LASTEXITCODE -ne 0) { $MINOR = "0" }
$HASH = git rev-parse --short HEAD 2>$null
if ($LASTEXITCODE -ne 0) { $HASH = "no-git" }
$GIT_STR = "$MINOR-$HASH"

Write-Host "🔨 Embedding Git version : $GIT_STR"

go build -ldflags "-X 'main.GitVersion=$GIT_STR'" -o poop.exe
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Compilation successful"
} else {
    Write-Host "❌ Compilation in error"
    exit 1
}
