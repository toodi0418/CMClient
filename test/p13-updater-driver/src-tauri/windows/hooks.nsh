!include "WinMessages.nsh"
${StrRep}
${UnStrRep}

!macro NSIS_HOOK_POSTINSTALL
  SetShellVarContext current

  ReadRegStr $R0 HKCU "Environment" "Path"
  StrCpy $R1 ";$R0;"
cmclient_install_path_dedupe:
  StrCpy $R2 "$R1"
  ${StrRep} $R1 "$R1" ";$INSTDIR;" ";"
  StrCmp $R1 $R2 cmclient_install_path_deduped cmclient_install_path_dedupe
cmclient_install_path_deduped:
  StrCpy $R0 "$R1" -1 1
  ${If} $R0 == ""
    StrCpy $R0 "$INSTDIR"
  ${Else}
    StrCpy $R0 "$R0;$INSTDIR"
  ${EndIf}
  WriteRegExpandStr HKCU "Environment" "Path" "$R0"

  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Run" "CMClientP13UpdaterFixture" '"$INSTDIR\${MAINBINARYNAME}.exe"'
  WriteRegStr HKCU "Software\CMClient\P13UpdaterFixture" "InstallDir" "$INSTDIR"
  SendMessage ${HWND_BROADCAST} ${WM_SETTINGCHANGE} 0 "STR:Environment" /TIMEOUT=5000
!macroend

!macro NSIS_HOOK_POSTUNINSTALL
  SetShellVarContext current

  ReadRegStr $R0 HKCU "Environment" "Path"
  StrCpy $R1 ";$R0;"
cmclient_uninstall_path_dedupe:
  StrCpy $R2 "$R1"
  ${UnStrRep} $R1 "$R1" ";$INSTDIR;" ";"
  StrCmp $R1 $R2 cmclient_uninstall_path_deduped cmclient_uninstall_path_dedupe
cmclient_uninstall_path_deduped:
  StrCpy $R0 "$R1" -1 1
  WriteRegExpandStr HKCU "Environment" "Path" "$R0"

  DeleteRegValue HKCU "Software\Microsoft\Windows\CurrentVersion\Run" "CMClientP13UpdaterFixture"
  DeleteRegKey HKCU "Software\CMClient\P13UpdaterFixture"
  SendMessage ${HWND_BROADCAST} ${WM_SETTINGCHANGE} 0 "STR:Environment" /TIMEOUT=5000
!macroend
