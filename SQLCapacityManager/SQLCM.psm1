#
# SQLCM.psm1
#

Import-Module sqlserver -DisableNameChecking


function Get-Servers
{
    param(
        [Parameter(Mandatory=$True,Position=1)]
        [string]$Server,
        [Parameter(Mandatory=$True,Position=2)]
        [string]$Database
    )
    Process {
        Invoke-SqlCmd -ServerInstance $Server -Database $Database -Query "SELECT server_id, server_name FROM Servers"
    }
}


function Get-CounterDefinitions
{
    param(
        [Parameter(Mandatory=$True,Position=1)]
        [string]$Server,
        [Parameter(Mandatory=$True,Position=2)]
        [string]$Database
    )
    Process {
        Invoke-SqlCmd -ServerInstance $Server -Database $Database -Query "SELECT counter_name, instance_name, cumulative FROM CounterDefinitions"
    }
}


function Get-LastPersistedPerformanceCounterSet
{
    param(
        [Parameter(Mandatory=$True,Position=1)]
        [string]$Server,
        [Parameter(Mandatory=$True,Position=2)]
        [string]$Database,
        [Parameter(Mandatory=$True,Position=3)]
        [string]$Target
    )
    Process {

        $sql = "
        SELECT [interval_id]
                ,[counter_name]
                ,[min_counter_value]
                ,[max_counter_value]
                ,[avg_counter_value]
        FROM [PerformanceCounters]
        WHERE interval_id = (
            SELECT TOP(1) interval_id
            FROM Intervals 
            WHERE server_id = (
                SELECT server_id 
                FROM Servers
                WHERE server_name = '$Target')
            ORDER BY interval_id DESC
        )
        "

        Invoke-SqlCmd -ServerInstance $Server -Database $Database -Query $sql
    }
}


function Get-PerformanceCounters 
{
    param(
        [Parameter(Mandatory=$True,Position=1)]
        [string]$Server,
        [Parameter(Mandatory=$True,Position=2)]
        [object[]]$Counter_definitions
    )
    Process {

        $sql = "
            SELECT RTRIM(counter_name) AS counter_name,
                RTRIM(instance_name) AS instance_name,
                cntr_value, 
                cumulative
            FROM sys.dm_os_performance_counters AS p
            INNER JOIN (
                VALUES "


        $sql += ( `
                $Counter_definitions | 
                Select-Object -Property @{Name = 'SQLLine'; Expression = {"('" + $_.counter_name + "','" + $_.instance_name +"'," + [int]$_.cumulative +")" }} |
                Select-Object -ExpandProperty SQLLine `
            ) -join "`r`n,"

        $sql += "
                ) AS v(name, instance, cumulative)
                    ON p.counter_name = v.name 
                    AND p.instance_name = v.instance
            ORDER BY 1,2,3
        "

        Invoke-SqlCmd -ServerInstance $Server -Query $sql
    }
}



function Write-PerformanceCounters 
{
    param(
        [Parameter(Mandatory=$True,Position=1)]
        [string]$Server,
        [Parameter(Mandatory=$True,Position=2)]
        [string]$Database,
        [Parameter(Mandatory=$True,Position=3)]
        [object[]]$data
    )
    Process {

        $conn = New-Object -TypeName System.Data.SqlClient.SqlConnection -ArgumentList "Data Source=$Server; Integrated Security=true; Initial Catalog=$Database;"
        $conn.Open()

        $dt = ConvertTo-DataTable $data

        $bcp = New-Object -TypeName System.Data.SqlClient.SqlBulkCopy -ArgumentList $conn
        $bcp.DestinationTableName = "PerformanceCounters"
        $bcp.WriteToServer($dt)
        
    }
}



function ConvertTo-DataTable
{
    <#
    .Synopsis
        Creates a DataTable from an object
    .Description
        Creates a DataTable from an object, containing all properties (except built-in properties from a database)
    .Example
        Get-ChildItem| Select Name, LastWriteTime | ConvertTo-DataTable
    .Link
        Select-DataTable
    .Link
        Import-DataTable
    .Link
        Export-Datatable
    #> 
    [OutputType([Data.DataTable])]
    param(
    # The input objects
    [Parameter(Position=0, Mandatory=$true, ValueFromPipeline = $true)]
    [PSObject[]]
    $InputObject
    ) 
 
    begin { 
        
        $outputDataTable = new-object Data.datatable   
          
        $knownColumns = @{}
        
        
    } 

    process {         
               
        foreach ($In in $InputObject) { 
            $DataRow = $outputDataTable.NewRow()   
            $isDataRow = $in.psobject.TypeNames -like "*.DataRow*" -as [bool]

            $simpleTypes = ('System.Boolean', 'System.Byte[]', 'System.Byte', 'System.Char', 'System.Datetime', 'System.Decimal', 'System.Double', 'System.Guid', 'System.Int16', 'System.Int32', 'System.Int64', 'System.Single', 'System.UInt16', 'System.UInt32', 'System.UInt64')

            $SimpletypeLookup = @{}
            foreach ($s in $simpleTypes) {
                $SimpletypeLookup[$s] = $s
            }            
            
            
            foreach($property in $In.PsObject.properties) {   
                if ($isDataRow -and 
                    'RowError', 'RowState', 'Table', 'ItemArray', 'HasErrors' -contains $property.Name) {
                    continue     
                }
                $propName = $property.Name
                $propValue = $property.Value
                $IsSimpleType = $SimpletypeLookup.ContainsKey($property.TypeNameOfValue)

                if (-not $outputDataTable.Columns.Contains($propName)) {   
                    $outputDataTable.Columns.Add((
                        New-Object Data.DataColumn -Property @{
                            ColumnName = $propName
                            DataType = if ($issimpleType) {
                                $property.TypeNameOfValue
                            } else {
                                'System.Object'
                            }
                        }
                    ))
                }                   
                
                $DataRow.Item($propName) = if ($isSimpleType -and $propValue) {
                    $propValue
                } elseif ($propValue) {
                    [PSObject]$propValue
                } else {
                    [DBNull]::Value
                }
                
            }   
            $outputDataTable.Rows.Add($DataRow)   
        } 
        
    }  
      
    end 
    { 
        ,$outputDataTable

    } 
 
}



function New-Interval {
    param(
        [Parameter(Mandatory=$True,Position=1)]
        [string]$Server,
        [Parameter(Mandatory=$True,Position=2)]
        [string]$Database,
        [Parameter(Mandatory=$True,Position=3)]
        [string]$Target
    )
    Process {
        $sql = "
            INSERT INTO Intervals (
                interval_id,
                server_id,
                end_time,
                duration_minutes
            )
            OUTPUT INSERTED.interval_id
            SELECT
                interval_id = ISNULL(MAX(interval_id),0) + 1,
                server_id = (SELECT server_id FROM Servers WHERE server_name = '$Target'),
                end_time = GETDATE(),
                duration_minutes = DATEDIFF(minute, ISNULL((SELECT end_time FROM Intervals WHERE server_id = (SELECT server_id FROM Servers WHERE server_name = '$Target')),GETDATE()), GETDATE())
            FROM Intervals
        "
        Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $sql
    }
}