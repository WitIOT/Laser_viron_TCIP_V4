; installer.iss — Inno Setup script สำหรับ Laser Control (Rev13)
; Build: "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" installer.iss
;
; ติดตั้งแบบ per-user (%LOCALAPPDATA%\Programs\LaserControl) โดยเจตนา
; เพื่อให้ระบบ auto-update copy ทับไฟล์ได้โดยไม่ต้องสิทธิ์ administrator
;
; หมายเหตุ: AppVersion อ่านค่าเดียวกับ version.py (แก้ที่นั่นแล้ว rebuild)

#define MyAppName "Laser Control"
#define MyAppExeName "LaserControl.exe"
#define MyAppVersion "13.0.5"
#define MyAppPublisher "NARIT"

[Setup]
AppId={{A7E4F2C1-9B3D-4E56-8F12-LASERCTRL0013}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
; per-user install → ไม่ต้อง admin, อัปเดตทับได้เอง
PrivilegesRequired=lowest
DefaultDirName={localappdata}\Programs\LaserControl
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
OutputDir=installer_output
OutputBaseFilename=LaserControl-Setup-{#MyAppVersion}
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
ArchitecturesInstallIn64BitMode=x64compatible

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Additional icons:"

[Files]
; รวมทั้งโฟลเดอร์ one-dir ที่ PyInstaller สร้าง (dist\LaserControl\*)
Source: "dist\LaserControl\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Launch {#MyAppName}"; Flags: nowait postinstall skipifsilent
