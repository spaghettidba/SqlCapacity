#
# Script.ps1
#



Import-Module (Join-Path $PSScriptRoot 'SQLCM.psm1') -Force

$SQLCMServer = "GSVSQL33\SQL21"
$SQLCMDatabase = "SQLCM"


$srv = Get-Servers -Server $SQLCMServer -Database $SQLCMDatabase
$counter_definitions = Get-CounterDefinitions -Server $SQLCMServer -Database $SQLCMDatabase

$srv | ForEach-Object {
    $previous_snapshot = Get-LastPersistedPerformanceCounterSet -Server $SQLCMServer -Database $SQLCMDatabase -Target $_.server_name
    $current_snapshot = Get-PerformanceCounters -Server $srv.server_name -Counter_definitions $counter_definitions

    $interval_id = New-Interval -Server $SQLCMServer -Database $SQLCMDatabase -Target $_.server_name

    $current_snapshot | ForEach-Object {
        $cntr_name = $_.counter_name 
        $previous_value = $previous_snapshot | Where-Object {$_.counter_name = $cntr_name}

        if($previous_value) {
            $_.cntr_value -= $previous_value.max_counter_value
        }
    }

    # non funziona perché devo selezionare le colonne corrette
    Write-PerformanceCounters -Server $SQLCMServer -Database $SQLCMDatabase -Data $current_snapshot 
}
