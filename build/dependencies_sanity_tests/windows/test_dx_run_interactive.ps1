Import-Module Await

Start-AwaitSession

Send-AwaitCommand "dx run $Env:APPLET"
Start-Sleep 1
Wait-AwaitResponse "inp1:"
Send-AwaitCommand "$Env:INP1_VAL"
Start-Sleep 1
Wait-AwaitResponse "inp2:"
Send-AwaitCommand "{TAB}{TAB}" -NoNewLine
Start-Sleep 1
Wait-AwaitResponse "$Env:INP2_VAL"
Send-AwaitCommand "$Env:INP2_VAL"
Start-Sleep 1
Wait-AwaitResponse "Confirm running the executable with this input [Y/n]:"
Send-AwaitCommand "y"
Start-Sleep 1
$output = Wait-AwaitResponse "Watch launched job now? [Y/n]"
$matcher = select-string "job-[a-zA-Z0-9]{24}" -inputobject $output
$jobId = $matcher.Matches.groups[0]
Send-AwaitCommand "n"

Start-Sleep 5

dx terminate $jobId