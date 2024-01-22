# Install choco
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Enable script execution
#Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy Unrestricted

# Install enhanced PowerShell Await plugin
$awaitModuleDir = "$env:UserProfile\Documents\PowerShell\Modules\Await"
$destination = "await.zip"
Invoke-RestMethod -Uri https://github.com/wormsik/await/archive/refs/heads/master.zip -OutFile $destination
New-Item -Type Directory -Force $awaitModuleDir
Expand-Archive -Path $destination -DestinationPath $awaitModuleDir
Rename-Item "$awaitModuleDir\await-master" "0.9"
Remove-Item "$destination"

# Install Windows terminal
choco install --yes --no-progress microsoft-windows-terminal

# Install Pythons
choco install --yes --no-progress python38
choco install --yes --no-progress python39
choco install --yes --no-progress python310
choco install --yes --no-progress python311
choco install --yes --no-progress python312
#choco install --yes miniconda3

# Install pytest
python3.11 -m pip install pytest

# Refresh env vars
refreshenv
