param(
    [string]$Docx = "",
    [string]$Pdf = ""
)

$ErrorActionPreference = "Stop"

if (-not $Docx) {
    $Docx = Join-Path $PSScriptRoot "CS324_A2_Report.docx"
}
if (-not $Pdf) {
    $Pdf = Join-Path $PSScriptRoot "CS324_A2_Report.pdf"
}

$Docx = (Resolve-Path $Docx).Path
$Pdf = [System.IO.Path]::GetFullPath($Pdf)

$word = New-Object -ComObject Word.Application
$word.Visible = $false
$word.DisplayAlerts = 0
$doc = $null
try {
    $doc = $word.Documents.Open($Docx, $false, $true)
    $doc.ExportAsFixedFormat($Pdf, 17)
    $pages = $doc.ComputeStatistics(2)
    Write-Host "Wrote $Pdf ($pages pages)"
}
finally {
    if ($null -ne $doc) {
        $doc.Close(0)
    }
    $word.Quit()
}
