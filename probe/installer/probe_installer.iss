; Inno Setup Script for Otter Probe
; Build with Inno Setup Compiler (https://jrsoftware.org/isinfo.php)
;
; Prerequisites:
; 1. Build executables first:
;    pyinstaller probe_tray.spec
;    pyinstaller probe_auth.spec
; 2. Executables will be in dist/ folder
; 3. Run this script with Inno Setup Compiler

#define MyAppName "Otter Probe"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "Otter"
#define MyAppURL "https://otter.com"
#define MyAppExeName "OtterProbe.exe"
#define MyAppAuthExeName "OtterProbeAuth.exe"

[Setup]
; NOTE: AppId uniquely identifies this application
AppId={{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}/support
AppUpdatesURL={#MyAppURL}/downloads
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
; Output settings
OutputDir=dist\installer
OutputBaseFilename=OtterProbeSetup
; Installer settings
Compression=lzma
SolidCompression=yes
WizardStyle=modern
; Privilege settings - install for current user only
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
; Uninstaller
UninstallDisplayIcon={app}\{#MyAppExeName}
; Architecture
ArchitecturesInstallIn64BitMode=x64compatible

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "autostart"; Description: "Start automatically when Windows starts"; GroupDescription: "Additional options:"; Flags: checkedonce
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; Main tray application
Source: "..\dist\OtterProbe.exe"; DestDir: "{app}"; Flags: ignoreversion
; Auth wizard
Source: "..\dist\OtterProbeAuth.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
; Start Menu
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Authorize Device"; Filename: "{app}\{#MyAppAuthExeName}"
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"
; Desktop icon (optional)
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Registry]
; Add to Windows autostart (if task selected)
Root: HKCU; Subkey: "Software\Microsoft\Windows\CurrentVersion\Run"; ValueType: string; ValueName: "OtterProbe"; ValueData: """{app}\{#MyAppExeName}"""; Flags: uninsdeletevalue; Tasks: autostart

[Run]
; Run auth wizard after installation (wait for it to complete)
Filename: "{app}\{#MyAppAuthExeName}"; Description: "Authorize device with Otter"; Flags: nowait postinstall skipifsilent
; Start tray app after installation
Filename: "{app}\{#MyAppExeName}"; Description: "Start Otter Probe"; Flags: nowait postinstall skipifsilent runasoriginaluser

[UninstallRun]
; Stop the running tray app before uninstall
Filename: "taskkill"; Parameters: "/F /IM OtterProbe.exe"; Flags: runhidden; RunOnceId: "StopOtterProbe"

[UninstallDelete]
; Clean up logs and credentials on uninstall (optional - commented out to preserve)
; Type: filesandordirs; Name: "{userappdata}\.otter"

[Code]
// Custom code for installation
procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
  begin
    // Any post-install steps can go here
    Log('Installation completed successfully');
  end;
end;

// Check if app is running and offer to close it
function PrepareToInstall(var NeedsRestart: Boolean): String;
var
  ResultCode: Integer;
begin
  Result := '';
  // Try to close running instance
  Exec('taskkill', '/F /IM OtterProbe.exe', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

// Custom uninstall procedure
procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usPostUninstall then
  begin
    // Clean up registry key if exists
    RegDeleteValue(HKEY_CURRENT_USER, 'Software\Microsoft\Windows\CurrentVersion\Run', 'OtterProbe');
  end;
end;

[Messages]
WelcomeLabel2=This will install [name/ver] on your computer.%n%nOtter Probe syncs your point-of-sale data automatically with Otter cloud services.%n%nClick Next to continue.
FinishedLabel=Setup has completed installing [name] on your computer.%n%nThe authorization wizard will open in your browser. Please approve the device to complete setup.
