Dim fso, shell, scriptDir, scriptPath
Dim exePath

Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
scriptPath = fso.BuildPath(scriptDir, "main.py")
exePath = fso.BuildPath(scriptDir, "wenjian-chuli.exe")

If fso.FileExists(exePath) Then
    shell.Run Chr(34) & exePath & Chr(34), 0, False
    WScript.Quit 0
End If

If TryStart(shell.ExpandEnvironmentStrings("%USERPROFILE%") & "\miniconda3\python.exe", shell.ExpandEnvironmentStrings("%USERPROFILE%") & "\miniconda3\pythonw.exe", scriptPath, scriptDir) Then
    WScript.Quit 0
End If

If TryStart("C:\Python314\python.exe", "C:\Python314\pythonw.exe", scriptPath, scriptDir) Then
    WScript.Quit 0
End If

If TryStart(shell.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\Programs\Python\Python310\python.exe", shell.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\Programs\Python\Python310\pythonw.exe", scriptPath, scriptDir) Then
    WScript.Quit 0
End If

Dim pyPath
pyPath = FindFirstUsablePython()
If pyPath <> "" Then
    shell.Run "cmd /c cd /d " & Chr(34) & scriptDir & Chr(34) & " && " & Chr(34) & pyPath & Chr(34) & " " & Chr(34) & scriptPath & Chr(34), 1, False
    WScript.Quit 0
End If

MsgBox "No packaged EXE or usable Python environment was found. Required modules for source mode: pandas, openpyxl, tkinter, tkinterdnd2.", vbExclamation, "Launcher"

Function TryStart(pyPath, pywPath, scriptPath, scriptDir)
    If Not fso.FileExists(pyPath) Then
        TryStart = False
        Exit Function
    End If
    If Not HasRequiredModules(pyPath) Then
        TryStart = False
        Exit Function
    End If
    If fso.FileExists(pywPath) Then
        shell.Run Chr(34) & pywPath & Chr(34) & " " & Chr(34) & scriptPath & Chr(34), 0, False
    Else
        shell.Run "cmd /c cd /d " & Chr(34) & scriptDir & Chr(34) & " && " & Chr(34) & pyPath & Chr(34) & " " & Chr(34) & scriptPath & Chr(34), 1, False
    End If
    TryStart = True
End Function

Function HasRequiredModules(pyPath)
    Dim execObj
    On Error Resume Next
    Set execObj = shell.Exec(Chr(34) & pyPath & Chr(34) & " -c " & Chr(34) & "import importlib.util, sys; mods=('pandas','openpyxl','tkinter','tkinterdnd2'); sys.exit(0 if all(importlib.util.find_spec(m) for m in mods) else 1)" & Chr(34))
    If Err.Number <> 0 Then
        Err.Clear
        HasRequiredModules = False
        Exit Function
    End If
    Do While execObj.Status = 0
        WScript.Sleep 50
    Loop
    HasRequiredModules = (execObj.ExitCode = 0)
End Function

Function FindFirstUsablePython()
    Dim execObj, line
    On Error Resume Next
    Set execObj = shell.Exec("cmd /c where python.exe")
    If Err.Number <> 0 Then
        Err.Clear
        FindFirstUsablePython = ""
        Exit Function
    End If
    Do While Not execObj.StdOut.AtEndOfStream
        line = Trim(execObj.StdOut.ReadLine())
        If line <> "" Then
            If HasRequiredModules(line) Then
                FindFirstUsablePython = line
                Exit Function
            End If
        End If
    Loop
    FindFirstUsablePython = ""
End Function
