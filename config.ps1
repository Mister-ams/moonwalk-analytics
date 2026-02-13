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
}
