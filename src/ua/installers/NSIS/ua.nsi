# DNAnexus Upload Agent Windows NSIS installer configuration

!define MUI_ICON ua.ico
!define MUI_UNICON ua.ico
!define MUI_WELCOMEFINISHPAGE_BITMAP dx-logo.bmp
!define MUI_WELCOMEFINISHPAGE_BITMAP_NOSTRETCH
!define MUI_LICENSEPAGE_TEXT_TOP "Please review the license for ${COMPANYNAME} ${APPNAME}"
!define MUI_FINISHPAGE_TITLE "Installation Complete"
!define MUI_FINISHPAGE_TEXT "To use the Upload Agent, open the Command Line Prompt and type 'ua', or right-click on any file or directory and choose 'Upload to DNAnexus'."
!define MUI_FINISHPAGE_LINK "Click here to open the Upload Agent Guide"
!define MUI_FINISHPAGE_LINK_LOCATION https://wiki.dnanexus.com/Upload-Agent
!include MUI2.nsh
 
!define APPNAME "Upload Agent"
!define COMPANYNAME "DNAnexus"
!define DESCRIPTION "Uploads files to the DNAnexus Platform"
# These three must be integers
!define VERSIONMAJOR $%VERSIONMAJOR%
!define VERSIONMINOR $%VERSIONMINOR%
!define VERSIONBUILD $%VERSIONBUILD%
# These will be displayed by the "Click here for support information" link in "Add/Remove Programs"
!define HELPURL "https://wiki.dnanexus.com/Upload-Agent" # "Support Information" link
!define UPDATEURL "https://wiki.dnanexus.com/Downloads#Upload-Agent" # "Product Updates" link
!define ABOUTURL "https://dnanexus.com/" # "Publisher" link
# This is the size (in kB) of all the files copied into "Program Files"
!define INSTALLSIZE 7233

InstallDir "$APPDATA\${COMPANYNAME}\${APPNAME}"

# This will be in the installer/uninstaller's title bar
Name "${COMPANYNAME} ${APPNAME}"
outFile "${COMPANYNAME} ${APPNAME} ${VERSIONMAJOR}.${VERSIONMINOR}.${VERSIONBUILD} Installer.exe"

!include LogicLib.nsh
!include EnvVarUpdate.nsh

!insertmacro MUI_PAGE_LICENSE "..\..\..\..\COPYING"
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_LANGUAGE English

section "install"
	# Files for the install directory - to build the installer, these should be in the same directory as the install script (this file)
	setOutPath $INSTDIR
	# Files added here should be removed by the uninstaller (see section "uninstall")
	file ..\..\dist\*
	file "ua.ico"
	# Add any other files for the install directory (license files, app data, etc) here

	# Uninstaller - See function un.onInit and section "uninstall" for configuration
	writeUninstaller "$INSTDIR\uninstall.exe"

	# Start Menu
	createDirectory "$SMPROGRAMS\${COMPANYNAME}"
	createShortCut "$SMPROGRAMS\${COMPANYNAME}\${APPNAME}.lnk" "$INSTDIR\ua.exe" "" "$INSTDIR\ua.ico"
 
	# Registry information for add/remove programs
	WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "DisplayName" "${COMPANYNAME} - ${APPNAME} - ${DESCRIPTION}"
	WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "UninstallString" "$\"$INSTDIR\uninstall.exe$\""
	WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "QuietUninstallString" "$\"$INSTDIR\uninstall.exe$\" /S"
	WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "InstallLocation" "$\"$INSTDIR$\""
	WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "DisplayIcon" "$\"$INSTDIR\ua.ico$\""
	WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "Publisher" "$\"${COMPANYNAME}$\""
	WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "HelpLink" "$\"${HELPURL}$\""
	WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "URLUpdateInfo" "$\"${UPDATEURL}$\""
	WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "URLInfoAbout" "$\"${ABOUTURL}$\""
	WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "DisplayVersion" "$\"${VERSIONMAJOR}.${VERSIONMINOR}.${VERSIONBUILD}$\""
	WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "VersionMajor" ${VERSIONMAJOR}
	WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "VersionMinor" ${VERSIONMINOR}
	# There is no option for modifying or repairing the install
	WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "NoModify" 1
	WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "NoRepair" 1
	# Set the INSTALLSIZE constant (!defined at the top of this script) so Add/Remove Programs can accurately report the size
	WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}" "EstimatedSize" ${INSTALLSIZE}

	WriteRegStr HKCR "exefile\shell\${NameStr}\command" "" "$INSTDIR\ua.exe $\"%1$\""

	${EnvVarUpdate} $0 "PATH" "P" "HKCU" "$INSTDIR"
sectionEnd

section "uninstall"
 
	# Remove Start Menu launcher
	delete "$SMPROGRAMS\${COMPANYNAME}\${APPNAME}.lnk"
	# Try to remove the Start Menu folder - this will only happen if it is empty
	rmDir "$SMPROGRAMS\${COMPANYNAME}"
 
	# Remove files
	delete $INSTDIR\ua.exe
	delete $INSTDIR\*.dll
	rmdir $INSTDIR

	${un.EnvVarUpdate} $0 "PATH" "R" "HKCU" "$INSTDIR"

	# Remove uninstaller information from the registry
	DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${COMPANYNAME} ${APPNAME}"
sectionEnd

!define NameStr "UPX" ; The string that the context menu will display
