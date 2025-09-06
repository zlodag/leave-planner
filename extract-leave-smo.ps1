# SMO-Only Radiologist Leave Data Extraction Script

param(
    [Parameter(Mandatory=$false)]
    [string]$OutputPath = ".\smo_leave_data.json",
    
    [Parameter(Mandatory=$false)]
    [int]$MonthsAhead = 6,
    
    [Parameter(Mandatory=$false)]
    [switch]$ExcludePending
)

# Configuration
$ServerName = "MSCHCPSCHSQLP1"
$DatabaseName = "PhySch"
$ConnectionString = "Server=$ServerName;Database=$DatabaseName;Integrated Security=true;"

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$Timestamp] [$Level] $Message"
}

function Export-SMOLeaveData {
    Write-Log "Starting SMO leave data extraction..."
    Write-Log "Parameters: OutputPath=$OutputPath, MonthsAhead=$MonthsAhead"
    
    try {
        # Calculate date range
        $StartDate = Get-Date
        $EndDate = $StartDate.AddMonths($MonthsAhead)
        $StartDateInt = [int]($StartDate.ToString("yyyyMMdd"))
        $EndDateInt = [int]($EndDate.ToString("yyyyMMdd"))
        
        Write-Log "Extracting leave data from $(Get-Date $StartDate -Format 'yyyy-MM-dd') to $(Get-Date $EndDate -Format 'yyyy-MM-dd')"
        
        # Build status filter
        $StatusFilter = if ($ExcludePending) { "and Request.Status <> 1" } else { "" }
                
        # SQL Query - Extract leave data for SMOs only
        $Query = @"
            select
                Employee.EmployeeID,
                FirstName,
                LastName,
                ShiftName,
                datetime2fromparts(
                    StartDate/10000,
                    StartDate/100%100,
                    StartDate%100,
                    Request.StartTime%2400/100,
                    Request.StartTime%100,
                    0, 0, 0
                ) as start,
                datetime2fromparts(
                    EndDate/10000,
                    EndDate/100%100,
                    EndDate%100,
                    Request.EndTime%2400/100,
                    Request.EndTime%100,
                    0, 0, 0
                ) as 'end',
                case Status when 1 then 'Pending'
                    when 2 then 'Approved'
                    when 4 then 'Denied'
                    when 8 then 'Waitlisted'
                end as status
                from Request
            join Employee on Employee.EmployeeID = Request.EmployeeID
            join Profile on Profile.ProfileID = Request.ProfileID
            join Shift on Shift.ShiftID = Request.ShiftID and Shift.ProfileID = Profile.ProfileID
            join Assignment on Assignment.AssignID = Shift.AssignID and Assignment.ProfileID = Profile.ProfileID
            where Profile.Abbr = 'SMO' -- also 'Fellows', 'RMO'
            -- and Employee.Abbr = 'xyz' -- for a single SMO
            and Assignment.Abbr in ('Leave am', 'Leave pm')
            and Request.Status <> 4 -- status is not 'denied'
            $StatusFilter
            and Request.IsAssignTo = 1 -- request is 'assign to' not 'block'
            -- and Request.StartDate >= year(CURRENT_TIMESTAMP) * 10000 + month(CURRENT_TIMESTAMP) * 100 + day(CURRENT_TIMESTAMP) -- from today
            -- and Request.StartDate >= year(CURRENT_TIMESTAMP) * 10000 -- from start of this year
            and Request.StartDate <= $EndDateInt
            and Request.EndDate >= $StartDateInt
            order by Request.EmployeeId, Request.StartDate, Request.StartTime
"@

        Write-Log "Connecting to database and identifying SMOs..."
        
        # Execute query
        $Connection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
        $Connection.Open()
        
        $Command = New-Object System.Data.SqlClient.SqlCommand($Query, $Connection)
        $Command.CommandTimeout = 60
        
        $Reader = $Command.ExecuteReader()
        
        $LeaveRecords = @()
        $RecordCount = 0
        $SMOEmployees = @{}
        
        while ($Reader.Read()) {

            $LeaveRecord = @{
                employee_id = $Reader["EmployeeID"]
                shift_name = $Reader["ShiftName"]
                start = $Reader["start"]
                end = $Reader["end"]
                full_name = "$($Reader['LastName']), $($Reader['FirstName'])"
                status = $Reader["status"]
            }
            $LeaveRecords += $LeaveRecord

            # Track SMO employees
            if (-not $SMOEmployees.ContainsKey($LeaveRecord["employee_id"])) {
                $SMOEmployees[$LeaveRecord["employee_id"]] = @{
                    full_name = $LeaveRecord["full_name"]
                }
            }

            $RecordCount++
            if ($RecordCount % 100 -eq 0) {
                Write-Log "Processed $RecordCount leave records..."
            }
        }
        
        $Reader.Close()
        $Connection.Close()
        
        Write-Log "Extracted $RecordCount leave records from $($SMOEmployees.Count) SMO radiologists"
        
        # Group by employee for summary
        $StaffSummary = $LeaveRecords | Group-Object employee_id | ForEach-Object {
           
            @{
                employee_id = [int]$_.Name
                full_name = $_.Group[0].full_name
                total_leave_days = $_.Group.Count * .5
                leave_shifts = $_.Group.Count
                approved_shifts = ($_.Group | Where-Object { $_.status -eq 'Approved' }).Count
                pending_shifts = ($_.Group | Where-Object { $_.status -eq 'Pending' }).Count
                leave_types = ($_.Group | Select-Object shift_name -Unique | ForEach-Object { $_.shift_name }) -join ", "
            }
        }
        
        # Create final output object
        $OutputData = @{
            metadata = @{
                extraction_date = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
                date_range = @{
                    start = Get-Date $StartDate -Format "yyyy-MM-dd"
                    end = Get-Date $EndDate -Format "yyyy-MM-dd"
                }
                filter_criteria = @{
                    exclude_pending = $ExcludePending.ToBool()
                }
                total_leave_shifts = $RecordCount
                unique_smo_staff = $SMOEmployees.Count
                database_info = @{
                    server = $ServerName
                    database = $DatabaseName
                }
            }
            smo_staff_summary = $StaffSummary
            leave_shifts = $LeaveRecords
        }
        
        # Export to JSON
        $JsonOutput = $OutputData | ConvertTo-Json -Depth 10
        $JsonOutput | Out-File -FilePath $OutputPath -Encoding UTF8
        
        Write-Log "Successfully exported SMO leave data to: $OutputPath"
        
        # Display summary statistics
        Write-Host "`n=== SMO LEAVE EXTRACTION SUMMARY ===" -ForegroundColor Green
        Write-Host "Total leave shifts: $RecordCount" -ForegroundColor Cyan
        Write-Host "SMO radiologists: $($SMOEmployees.Count)" -ForegroundColor Cyan
        Write-Host "Date range: $(Get-Date $StartDate -Format 'yyyy-MM-dd') to $(Get-Date $EndDate -Format 'yyyy-MM-dd')" -ForegroundColor Cyan
        Write-Host "Output file: $OutputPath" -ForegroundColor Cyan
        
        if ($StaffSummary.Count -gt 0) {
            Write-Host "`n=== SMO RADIOLOGISTS IDENTIFIED ===" -ForegroundColor Yellow
            $StaffSummary | Sort-Object full_name | ForEach-Object {
                Write-Host "$($_.full_name) - $($_.leave_shifts) shifts" -ForegroundColor White
            }
            
            Write-Host "`n=== TOP SMOs BY LEAVE DAYS ===" -ForegroundColor Yellow
            $StaffSummary | Sort-Object total_leave_days -Descending | Select-Object -First 10 | ForEach-Object {
                Write-Host "$($_.full_name) - $($_.total_leave_days) days ($($_.leave_shifts) shifts)" -ForegroundColor White
            }
        }
        
        return $OutputData
        
    }
    catch {
        Write-Log "Error during extraction: $($_.Exception.Message)" "ERROR"
        throw
    }
    finally {
        if ($Connection -and $Connection.State -eq 'Open') {
            $Connection.Close()
        }
    }
}

# Execute main function
try {
    Write-Host "Starting SMO-only leave extraction..." -ForegroundColor Green
    Export-SMOLeaveData | Out-Null
    
    Write-Host "`nSMO leave data extraction completed successfully!" -ForegroundColor Green
    Write-Host "Data saved to: $OutputPath" -ForegroundColor Cyan
    Write-Host "`nThis data contains only consultant radiologists (SMOs)" -ForegroundColor Yellow
    Write-Host "Perfect for your 2-radiologist weekly roster system!" -ForegroundColor Yellow
    
    exit 0
}
catch {
    Write-Host "`nSMO leave data extraction failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}