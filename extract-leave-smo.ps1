# SMO-Only Radiologist Leave Data Extraction Script
# Version 5.0 - Focuses on consultant/attending radiologists (SMOs)

param(
    [Parameter(Mandatory=$false)]
    [string]$OutputPath = ".\smo_leave_data.json",
    
    [Parameter(Mandatory=$false)]
    [int]$MonthsAhead = 6,
    
    [Parameter(Mandatory=$false)]
    [switch]$IncludePending = $true,
    
    [Parameter(Mandatory=$false)]
    [int]$MinSMOShifts = 5  # Minimum SMO shifts to be considered an SMO
)

# Configuration
$ServerName = "MSCHCPSCHSQLP1"
$DatabaseName = "PhySch"
$ConnectionString = "Server=$ServerName;Database=$DatabaseName;Integrated Security=true;"

# Leave-related shift patterns to extract
$LeaveShiftPatterns = @(
    "Annual Leave%",
    "Annual leave%", 
    "Leave Pending%",
    "Sick%",
    "CME Leave%",
    "CME Travel%",
    "Study Leave%",
    "Parental Leave%",
    "Sabbatical%",
    "Bereavement%",
    "Day in Lieu%",
    "LWOP%",
    "Peak Leave%"
)

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$Timestamp] [$Level] $Message"
}

function Convert-IntToTime {
    param([int]$TimeInt)
    try {
        if ($TimeInt -eq 0) { return "00:00" }
        $timeStr = $TimeInt.ToString().PadLeft(4, '0')
        return "$($timeStr.Substring(0,2)):$($timeStr.Substring(2,2))"
    }
    catch {
        return "00:00"
    }
}

function Convert-IntToDate {
    param([int]$DateInt)
    try {
        $dateStr = $DateInt.ToString().PadLeft(8, '0')
        return [DateTime]::ParseExact($dateStr, "yyyyMMdd", $null)
    }
    catch {
        Write-Log "Error converting date: $DateInt" "ERROR"
        return $null
    }
}

function Export-SMOLeaveData {
    Write-Log "Starting SMO leave data extraction..."
    Write-Log "Parameters: OutputPath=$OutputPath, MonthsAhead=$MonthsAhead, MinSMOShifts=$MinSMOShifts"
    
    try {
        # Calculate date range
        $StartDate = Get-Date
        $EndDate = $StartDate.AddMonths($MonthsAhead)
        $StartDateInt = [int]($StartDate.ToString("yyyyMMdd"))
        $EndDateInt = [int]($EndDate.ToString("yyyyMMdd"))
        
        Write-Log "Extracting leave data from $(Get-Date $StartDate -Format 'yyyy-MM-dd') to $(Get-Date $EndDate -Format 'yyyy-MM-dd')"
        
        # Build status filter
        $StatusFilter = if ($IncludePending) { "r.Status IN (1,2)" } else { "r.Status = 2" }
        
        # Build leave shift pattern filter
        $ShiftPatternFilter = ($LeaveShiftPatterns | ForEach-Object { "s.ShiftName LIKE '$_'" }) -join " OR "
        
        # SQL Query - Extract leave data for SMOs only
        $Query = @"
        WITH SMOEmployees AS (
            -- Identify SMOs based on their shift assignments
            SELECT 
                e.EmployeeID,
                e.FirstName,
                e.LastName,
                COUNT(CASE WHEN s.ShiftName LIKE '%SMO%' THEN 1 END) as SMO_Shifts,
                COUNT(*) as Total_Shifts
            FROM Employee e
            JOIN Request r ON e.EmployeeID = r.EmployeeID
            JOIN Shift s ON r.ShiftID = s.ShiftID
            WHERE r.IsAssignTo = 1 
                AND r.Status = 2
                AND r.StartDate >= 20250101
            GROUP BY e.EmployeeID, e.FirstName, e.LastName
            HAVING COUNT(CASE WHEN s.ShiftName LIKE '%SMO%' THEN 1 END) >= $MinSMOShifts
        ),
        SMOLeaveData AS (
            -- Get leave data for identified SMOs
            SELECT 
                r.EmployeeID,
                smo.FirstName,
                smo.LastName,
                smo.SMO_Shifts,
                r.StartDate,
                r.EndDate,
                r.StartTime,
                r.EndTime,
                s.ShiftName,
                s.IsOffTime,
                r.Status,
                r.Note,
                CASE r.Status 
                    WHEN 1 THEN 'Pending'
                    WHEN 2 THEN 'Approved'
                    WHEN 4 THEN 'Cancelled'
                    ELSE 'Other'
                END AS StatusText
            FROM Request r
            JOIN SMOEmployees smo ON r.EmployeeID = smo.EmployeeID
            JOIN Shift s ON r.ShiftID = s.ShiftID
            WHERE 
                $StatusFilter
                AND r.IsAssignTo = 1
                AND ($ShiftPatternFilter)
                AND (
                    (r.StartDate >= $StartDateInt AND r.StartDate <= $EndDateInt)
                    OR (r.EndDate >= $StartDateInt AND r.EndDate <= $EndDateInt)
                    OR (r.StartDate <= $StartDateInt AND r.EndDate >= $EndDateInt)
                )
        )
        SELECT 
            EmployeeID,
            FirstName,
            LastName,
            SMO_Shifts,
            StartDate,
            EndDate,
            StartTime,
            EndTime,
            ShiftName,
            IsOffTime,
            Status,
            StatusText,
            Note
        FROM SMOLeaveData
        ORDER BY LastName, FirstName, StartDate, StartTime
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
            $StartDateConverted = Convert-IntToDate $Reader["StartDate"]
            $EndDateConverted = Convert-IntToDate $Reader["EndDate"]
            
            if ($null -eq $StartDateConverted -or $null -eq $EndDateConverted) {
                continue
            }
            
            # Extract and clean field values
            $employeeID = $Reader["EmployeeID"]
            $firstName = if ([DBNull]::Value.Equals($Reader["FirstName"])) { "" } else { $Reader["FirstName"].ToString().Trim() }
            $lastName = if ([DBNull]::Value.Equals($Reader["LastName"])) { "" } else { $Reader["LastName"].ToString().Trim() }
            $smoShifts = $Reader["SMO_Shifts"]
            $shiftName = if ([DBNull]::Value.Equals($Reader["ShiftName"])) { "" } else { $Reader["ShiftName"].ToString().Trim() }
            $note = if ([DBNull]::Value.Equals($Reader["Note"])) { "" } else { $Reader["Note"].ToString().Trim() }
            
            # Track SMO employees
            if (-not $SMOEmployees.ContainsKey($employeeID)) {
                $SMOEmployees[$employeeID] = @{
                    full_name = "$lastName, $firstName"
                    smo_shifts = $smoShifts
                }
            }
            
            $LeaveRecord = @{
                employee_id = $employeeID
                first_name = $firstName
                last_name = $lastName
                full_name = "$lastName, $firstName"
                smo_shifts = $smoShifts
                start_date = Get-Date $StartDateConverted -Format "yyyy-MM-dd"
                end_date = Get-Date $EndDateConverted -Format "yyyy-MM-dd"
                start_time = Convert-IntToTime $(if ([DBNull]::Value.Equals($Reader["StartTime"])) { 0 } else { $Reader["StartTime"] })
                end_time = Convert-IntToTime $(if ([DBNull]::Value.Equals($Reader["EndTime"])) { 0 } else { $Reader["EndTime"] })
                shift_name = $shiftName
                is_off_time = $Reader["IsOffTime"]
                status = $Reader["Status"]
                status_text = $Reader["StatusText"]
                note = $note
                extracted_at = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            }
            
            $LeaveRecords += $LeaveRecord
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
            $staffLeave = $_.Group
            $totalDays = 0
            
            # Calculate total leave days
            $leaveByDate = $staffLeave | Group-Object start_date
            foreach ($dateGroup in $leaveByDate) {
                $shiftsOnDate = $dateGroup.Group
                $hasAM = ($shiftsOnDate | Where-Object { $_.shift_name -like "*am*" }).Count -gt 0
                $hasPM = ($shiftsOnDate | Where-Object { $_.shift_name -like "*pm*" }).Count -gt 0
                
                if ($hasAM -and $hasPM) {
                    $totalDays += 1
                } elseif ($hasAM -or $hasPM) {
                    $totalDays += 0.5
                }
            }
            
            @{
                employee_id = $_.Name
                full_name = $_.Group[0].full_name
                smo_shifts = $_.Group[0].smo_shifts
                total_leave_days = $totalDays
                leave_shifts = $_.Group.Count
                approved_shifts = ($_.Group | Where-Object { $_.status -eq 2 }).Count
                pending_shifts = ($_.Group | Where-Object { $_.status -eq 1 }).Count
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
                    role_type = "SMO (Senior Medical Officer/Consultant)"
                    min_smo_shifts = $MinSMOShifts
                    include_pending = $IncludePending
                }
                total_leave_shifts = $RecordCount
                unique_smo_staff = $SMOEmployees.Count
                leave_shift_patterns = $LeaveShiftPatterns
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
            $SMOEmployees.Values | Sort-Object { $_.full_name } | ForEach-Object {
                Write-Host "$($_.full_name) - $($_.smo_shifts) SMO shifts" -ForegroundColor White
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
    Write-Host "`nThis data contains only consultant/attending radiologists (SMOs)" -ForegroundColor Yellow
    Write-Host "Perfect for your 2-radiologist weekly roster system!" -ForegroundColor Yellow
    
    exit 0
}
catch {
    Write-Host "`nSMO leave data extraction failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}