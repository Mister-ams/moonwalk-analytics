# =====================================================================
# EXTRACT POWER QUERY M-CODE FROM EXCEL
# =====================================================================
# Opens Excel workbook and extracts M-code from all Power Queries
# Saves each query to separate file for review
# AUTO-DETECTS latest Lime_Reporting*.xlsx file
# =====================================================================

param(
    [string]$WorkbookFolder = "C:\Users\MRAL-\OneDrive\Documents\3. Projects\2. Investment Theses\01. Penrose Alpha\Admin\Moonwalk Data",
    [string]$OutputFolder = "C:\Users\MRAL-\OneDrive\Documents\3. Projects\2. Investment Theses\01. Penrose Alpha\Admin\Moonwalk Data\PowerQuery_MCode_Export"
)

Write-Host ""
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 69) -ForegroundColor Cyan
Write-Host "POWER QUERY M-CODE EXTRACTOR" -ForegroundColor Cyan
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 69) -ForegroundColor Cyan
Write-Host ""

# =====================================================================
# AUTO-DETECT LATEST WORKBOOK
# =====================================================================

Write-Host "Searching for latest Lime_Reporting file..." -ForegroundColor Yellow

$excelFiles = Get-ChildItem -Path $WorkbookFolder -Filter "Lime_Reporting*.xlsx" | 
    Where-Object { $_.Name -notlike "*~*" } |  # Exclude temp files
    Sort-Object LastWriteTime -Descending

if ($excelFiles.Count -eq 0) {
    Write-Host "  [ERROR] No Lime_Reporting*.xlsx files found in:" -ForegroundColor Red
    Write-Host "  $WorkbookFolder" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Please check the folder path" -ForegroundColor Yellow
    Write-Host ""
    pause
    exit
}

$WorkbookPath = $excelFiles[0].FullName
$WorkbookName = $excelFiles[0].Name

Write-Host "  [OK] Found: $WorkbookName" -ForegroundColor Green
Write-Host "       Last modified: $($excelFiles[0].LastWriteTime)" -ForegroundColor Gray
Write-Host ""

# =====================================================================
# VALIDATE INPUTS
# =====================================================================

Write-Host "Validating inputs..." -ForegroundColor Yellow

if (-not (Test-Path $WorkbookPath)) {
    Write-Host "  [ERROR] Workbook not found: $WorkbookPath" -ForegroundColor Red
    Write-Host ""
    Write-Host "  This shouldn't happen - auto-detection failed" -ForegroundColor Yellow
    Write-Host ""
    pause
    exit
}

Write-Host "  [OK] Workbook verified: $WorkbookName" -ForegroundColor Green

# Create output folder
if (-not (Test-Path $OutputFolder)) {
    New-Item -ItemType Directory -Path $OutputFolder -Force | Out-Null
    Write-Host "  [OK] Created output folder: $OutputFolder" -ForegroundColor Green
} else {
    Write-Host "  [OK] Using output folder: $OutputFolder" -ForegroundColor Green
}

# =====================================================================
# OPEN EXCEL
# =====================================================================

Write-Host ""
Write-Host "Opening Excel..." -ForegroundColor Yellow

try {
    $Excel = New-Object -ComObject Excel.Application
    $Excel.Visible = $false
    $Excel.DisplayAlerts = $false
    Write-Host "  [OK] Excel application started" -ForegroundColor Green
}
catch {
    Write-Host "  [ERROR] Failed to start Excel: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Make sure Excel is installed" -ForegroundColor Yellow
    pause
    exit
}

# =====================================================================
# OPEN WORKBOOK
# =====================================================================

Write-Host ""
Write-Host "Opening workbook..." -ForegroundColor Yellow

try {
    $Workbook = $Excel.Workbooks.Open($WorkbookPath, $null, $true)  # ReadOnly = $true
    Write-Host "  [OK] Workbook opened" -ForegroundColor Green
}
catch {
    Write-Host "  [ERROR] Failed to open workbook: $_" -ForegroundColor Red
    $Excel.Quit()
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($Excel) | Out-Null
    pause
    exit
}

# =====================================================================
# EXTRACT QUERIES
# =====================================================================

Write-Host ""
Write-Host "Extracting Power Queries..." -ForegroundColor Yellow
Write-Host ""

try {
    # Access the Queries collection
    $Queries = $Workbook.Queries
    
    if ($Queries.Count -eq 0) {
        Write-Host "  [WARNING] No Power Queries found in workbook" -ForegroundColor Yellow
        Write-Host "  This workbook may not have any queries, or they may be in PowerPivot only" -ForegroundColor Yellow
    }
    else {
        Write-Host "  Found $($Queries.Count) Power Queries" -ForegroundColor Green
        Write-Host ""
        
        $QueryList = @()
        
        foreach ($Query in $Queries) {
            $QueryName = $Query.Name
            $QueryFormula = $Query.Formula
            
            Write-Host "  Processing: $QueryName" -ForegroundColor Cyan
            
            # Clean up query name for filename (remove invalid characters)
            $SafeName = $QueryName -replace '[\\/:*?"<>|]', '_'
            
            # Save to file
            $OutputPath = Join-Path $OutputFolder "$SafeName.txt"
            
            # Create header with metadata
            $Header = @"
// =====================================================================
// POWER QUERY: $QueryName
// =====================================================================
// Extracted: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
// Workbook: $WorkbookName
// =====================================================================

"@
            
            $Content = $Header + $QueryFormula
            
            $Content | Out-File -FilePath $OutputPath -Encoding UTF8
            
            Write-Host "    [OK] Saved to: $SafeName.txt" -ForegroundColor Green
            
            # Add to list for summary
            $QueryList += [PSCustomObject]@{
                Name = $QueryName
                Length = $QueryFormula.Length
                Lines = ($QueryFormula -split "`n").Count
                File = "$SafeName.txt"
            }
        }
        
        # Save summary
        Write-Host ""
        Write-Host "  Creating summary..." -ForegroundColor Yellow
        
        $SummaryPath = Join-Path $OutputFolder "_QUERY_SUMMARY.txt"
        
        $Summary = @"
=====================================================================
POWER QUERY EXTRACTION SUMMARY
=====================================================================
Workbook: $WorkbookName
Extracted: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
Total Queries: $($Queries.Count)
=====================================================================

QUERIES:

"@
        
        foreach ($Q in $QueryList) {
            $Summary += "`n$($Q.Name)"
            $Summary += "`n  File: $($Q.File)"
            $Summary += "`n  Size: $($Q.Length) characters, $($Q.Lines) lines"
            $Summary += "`n"
        }
        
        $Summary | Out-File -FilePath $SummaryPath -Encoding UTF8
        
        Write-Host "    [OK] Saved summary to: _QUERY_SUMMARY.txt" -ForegroundColor Green
    }
}
catch {
    Write-Host "  [ERROR] Failed to extract queries: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "  This workbook may use PowerPivot queries instead of standard Power Queries" -ForegroundColor Yellow
    Write-Host "  Attempting alternative extraction method..." -ForegroundColor Yellow
}

# =====================================================================
# ALTERNATIVE: EXTRACT FROM POWERPIVOT (if regular queries failed)
# =====================================================================

if ($Queries.Count -eq 0) {
    Write-Host ""
    Write-Host "Attempting PowerPivot extraction..." -ForegroundColor Yellow
    Write-Host ""
    
    try {
        # Access PowerPivot data model
        $DataModel = $Workbook.Model
        
        if ($null -ne $DataModel) {
            Write-Host "  [INFO] PowerPivot data model found" -ForegroundColor Cyan
            Write-Host "  [INFO] PowerPivot queries cannot be extracted via COM automation" -ForegroundColor Yellow
            Write-Host ""
            Write-Host "  MANUAL EXTRACTION REQUIRED:" -ForegroundColor Yellow
            Write-Host "    1. Open the workbook in Excel" -ForegroundColor White
            Write-Host "    2. Data → Queries & Connections" -ForegroundColor White
            Write-Host "    3. Right-click each query → View Code" -ForegroundColor White
            Write-Host "    4. Copy M-code to text file" -ForegroundColor White
            Write-Host ""
            Write-Host "  OR use the M-code files you already have in your project" -ForegroundColor Cyan
            Write-Host ""
        }
        else {
            Write-Host "  [INFO] No PowerPivot data model found" -ForegroundColor Yellow
        }
    }
    catch {
        Write-Host "  [INFO] PowerPivot not accessible: $_" -ForegroundColor Gray
    }
}

# =====================================================================
# CLOSE WORKBOOK & EXCEL
# =====================================================================

Write-Host ""
Write-Host "Cleaning up..." -ForegroundColor Yellow

try {
    $Workbook.Close($false)  # Don't save changes
    $Excel.Quit()
    
    # Release COM objects
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($Workbook) | Out-Null
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($Excel) | Out-Null
    [System.GC]::Collect()
    [System.GC]::WaitForPendingFinalizers()
    
    Write-Host "  [OK] Excel closed" -ForegroundColor Green
}
catch {
    Write-Host "  [WARNING] Error closing Excel: $_" -ForegroundColor Yellow
}

# =====================================================================
# FINAL SUMMARY
# =====================================================================

Write-Host ""
Write-Host "=" -NoNewline -ForegroundColor Green
Write-Host ("=" * 69) -ForegroundColor Green
Write-Host "EXTRACTION COMPLETE" -ForegroundColor Green
Write-Host "=" -NoNewline -ForegroundColor Green
Write-Host ("=" * 69) -ForegroundColor Green
Write-Host ""

if ($Queries.Count -gt 0) {
    Write-Host "Extracted $($Queries.Count) queries from $WorkbookName to:" -ForegroundColor Green
    Write-Host "  $OutputFolder" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "  1. Review extracted M-code files" -ForegroundColor White
    Write-Host "  2. Compare with current queries in workbook" -ForegroundColor White
    Write-Host "  3. Update documentation if needed" -ForegroundColor White
}
else {
    Write-Host "No standard Power Queries found in $WorkbookName" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "This workbook likely uses PowerPivot queries" -ForegroundColor Yellow
    Write-Host "Manual extraction required - see instructions above" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "Press any key to exit..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
