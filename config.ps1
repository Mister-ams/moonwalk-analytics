# =====================================================================
# Shared path configuration — dot-source from other PS scripts
# Override any path via the corresponding MOONWALK_* env var.
# =====================================================================

$MoonwalkConfig = @{
    PythonScriptFolder = $(if ($env:MOONWALK_SCRIPTS)      { $env:MOONWALK_SCRIPTS }      else { $PSScriptRoot })
    DataFolder         = $(if ($env:MOONWALK_DATA)          { $env:MOONWALK_DATA }          else { Split-Path $PSScriptRoot -Parent })
    LocalStagingFolder = $(if ($env:MOONWALK_STAGING)       { $env:MOONWALK_STAGING }       else { 'C:\Users\MRAL-\Downloads\Lime Reporting' })
    OneDriveDataFolder = $(if ($env:MOONWALK_ONEDRIVE_DATA) { $env:MOONWALK_ONEDRIVE_DATA } else { Join-Path (Split-Path $PSScriptRoot -Parent) 'Sales Data' })
    DownloadsPath      = $(if ($env:MOONWALK_DOWNLOADS)     { $env:MOONWALK_DOWNLOADS }     else { 'C:\Users\MRAL-\Downloads' })
    RequiredCsvs       = @(
        'All_Customers_Python.csv'
        'All_Sales_Python.csv'
        'All_Items_Python.csv'
        'Customer_Quality_Monthly_Python.csv'
        'DimPeriod_Python.csv'
    )

    # Named constants (replaces magic numbers)
    XlCalculationManual    = -4135
    XlCalculationAutomatic = -4105
    DashboardPort          = 8504
    ExcelTimeoutSeconds    = 300
    DuckDbTimeoutSeconds   = 120
}
