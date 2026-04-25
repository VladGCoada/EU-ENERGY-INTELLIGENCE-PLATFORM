param(  
    [string]$Task  
)  
  
switch ($Task) {  
    "test" { pytest }  
    "lint" { ruff check . }  
    "format" { black . }  
    default { Write-Host "Use: .\scripts.ps1 test|lint|format" }  
}