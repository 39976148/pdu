# 在项目目录执行：初始化仓库并提交 PDU 相关文件
# 执行前请先在 GitHub 新建空仓库，并把下面的 YOUR_USERNAME 和 YOUR_REPO 换成你的
Set-Location $PSScriptRoot

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Host "未找到 git，请确认已安装 Git 并已重新打开终端。" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path .git)) {
    git init
    Write-Host "已执行 git init" -ForegroundColor Green
}

git add pdu_monitor.py pdu_monitor_with_group.py pdu_outlet_switch_test.py pdu3_current_test.py
git status
$count = (git status --short | Measure-Object -Line).Lines
if ($count -gt 0) {
    git commit -m "Add PDU monitor and outlet control tools"
    Write-Host "已提交 PDU 相关文件" -ForegroundColor Green
} else {
    Write-Host "没有需要提交的更改（可能已提交过）" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "下一步：在 GitHub 新建空仓库后，执行：" -ForegroundColor Cyan
Write-Host "  git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git"
Write-Host "  git branch -M main"
Write-Host "  git push -u origin main"
