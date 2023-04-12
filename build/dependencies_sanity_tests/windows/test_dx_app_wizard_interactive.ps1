Import-Module Await

Start-AwaitSession

Send-AwaitCommand "dx-app-wizard"
Start-Sleep 1
Wait-AwaitResponse "App Name:"
Send-AwaitCommand "test_applet"
Start-Sleep 1
Wait-AwaitResponse "Title []:"
Send-AwaitCommand "title"
Start-Sleep 1
Wait-AwaitResponse "Summary []:"
Send-AwaitCommand "summary"
Start-Sleep 1
Wait-AwaitResponse "Version [0.0.1]:"
Send-AwaitCommand "0.0.1"
Start-Sleep 1
Wait-AwaitResponse "1st input name (<ENTER> to finish):"
Send-AwaitCommand "inp1"
Start-Sleep 1
Wait-AwaitResponse "Label (optional human-readable name) []:"
Send-AwaitCommand "{ENTER}"
Start-Sleep 1
Wait-AwaitResponse "Choose a class (<TAB> twice for choices):"
Send-AwaitCommand "{TAB}" -NoNewLine
Start-Sleep 1
Wait-AwaitResponse "int           string"
Send-AwaitCommand "float"
Start-Sleep 1
Wait-AwaitResponse "This is an optional parameter [y/n]:"
Send-AwaitCommand "n"
Start-Sleep 1
Wait-AwaitResponse "2nd input name (<ENTER> to finish):"
Send-AwaitCommand "{ENTER}" -NoNewLine
Start-Sleep 1
Wait-AwaitResponse "1st output name (<ENTER> to finish):"
Send-AwaitCommand "out1"
Start-Sleep 1
Wait-AwaitResponse "Label (optional human-readable name) []:"
Send-AwaitCommand " "
Start-Sleep 1
Wait-AwaitResponse "Choose a class (<TAB> twice for choices):"
Send-AwaitCommand "{TAB}" -NoNewLine
Start-Sleep 1
Wait-AwaitResponse "int           string"
Send-AwaitCommand "float"
Start-Sleep 1
Wait-AwaitResponse "2nd output name (<ENTER> to finish):"
Send-AwaitCommand "{ENTER}" -NoNewLine
Start-Sleep 1
Wait-AwaitResponse "Timeout policy [48h]:"
Send-AwaitCommand "1h"
Start-Sleep 1
Wait-AwaitResponse "Programming language:"
Send-AwaitCommand "bash"
Start-Sleep 1
Wait-AwaitResponse "Will this app need access to the Internet? [y/N]:"
Send-AwaitCommand "n"
Start-Sleep 1
Wait-AwaitResponse "Will this app need access to the parent project? [y/N]:"
Send-AwaitCommand "n"
Start-Sleep 1
Wait-AwaitResponse "Choose an instance type for your app [mem1_ssd1_v2_x4]:"
Send-AwaitCommand "{ENTER}" -NoNewLine
