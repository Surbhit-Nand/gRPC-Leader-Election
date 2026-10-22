# Helper to build and launch cluster nodes with the correct classpath.
# Each application uses its own module's classes and target/dependency jars.
#
# Usage (one terminal per node, project root):
#   .\run.ps1 build                              # package modules + copy runtime jars (skips test execution)
#   .\run.ps1 bootstrap [grpcPort] [--state-file path] # default 50051
#   .\run.ps1 worker [host] [grpcPort]               # default localhost 50051
#   .\run.ps1 client [host] [callbackPort] [grpcPort] # default localhost 1199 50051
#   .\run.ps1 reset-state [stateFile]                # default bootstrap-state.properties
$ErrorActionPreference = "Stop"

function Get-Mvn {
    $mvn = Get-Command mvn -ErrorAction SilentlyContinue
    if ($mvn) { return $mvn.Source }
    $wrapper = Get-ChildItem "$env:USERPROFILE\.m2\wrapper\dists\apache-maven-*\*\*\bin\mvn.cmd" `
        -ErrorAction SilentlyContinue | Sort-Object FullName -Descending | Select-Object -First 1
    if ($wrapper) { return $wrapper.FullName }
    throw "Maven not found: install Maven 3.9+ or put mvn on PATH."
}

function Get-Classpath([string]$module) {
    $classes = Join-Path $PSScriptRoot "$module/target/classes"
    $dependencies = Join-Path $PSScriptRoot "$module/target/dependency"
    if (-not (Test-Path -LiteralPath $classes)) {
        throw "$module/target/classes is missing. Run '.\run.ps1 build' first."
    }
    if (-not (Test-Path -LiteralPath $dependencies)) {
        throw "$module/target/dependency is missing. Run '.\run.ps1 build' first."
    }
    return "$classes;$dependencies/*"
}

$command = if ($args.Count -ge 1) { $args[0] } else { "help" }

switch ($command) {
    "build" {
        & (Get-Mvn) -f (Join-Path $PSScriptRoot "pom.xml") package '-DskipTests' dependency:copy-dependencies '-DincludeScope=runtime'
        if ($LASTEXITCODE -ne 0) { throw "Build failed with exit code $LASTEXITCODE." }
        Write-Host "Build OK. Launch nodes with .\run.ps1 bootstrap|worker|client"
    }
    "bootstrap" {
        # Keep this an array: assigning the output of an `if` expression unwraps a
        # single-element array to a string, and splatting that string passes it
        # to Java one character per argument.
        $bootstrapArgs = @()
        if ($args.Count -ge 2) {
            $bootstrapArgs = $args[1..($args.Count - 1)]
        } else {
            $bootstrapArgs = @("50051")
        }
        & java -cp (Get-Classpath "bootstrap") com.cs324.bootstrap.BootstrapNode @bootstrapArgs
    }
    "reset-state" {
        # Bootstrap resolves its default path against the current directory, so do the same here.
        $stateFile = if ($args.Count -ge 2) { $args[1] } else { "bootstrap-state.properties" }
        $statePath = [System.IO.Path]::GetFullPath($stateFile)
        $bootstrap = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object { $_.CommandLine -like "*com.cs324.bootstrap.BootstrapNode*" }
        if ($bootstrap) {
            $pids = ($bootstrap | ForEach-Object { $_.ProcessId }) -join ", "
            throw "Bootstrap is running (PID $pids). Stop bootstrap and all workers before resetting IDs; " +
                "a live bootstrap keeps its counters in memory and would rewrite the state file."
        }
        $removed = @()
        foreach ($path in @($statePath, "$statePath.tmp")) {
            if (Test-Path -LiteralPath $path) {
                Remove-Item -LiteralPath $path -Force
                $removed += $path
            }
        }
        if ($removed.Count -eq 0) {
            Write-Host "No state file at $statePath; counters already start at 1."
        } else {
            $removed | ForEach-Object { Write-Host "Removed $_" }
            Write-Host "Next bootstrap start assigns Worker 1 and Job 1."
        }
        Write-Host "Start a fresh cluster with: .\run.ps1 bootstrap"
    }
    "worker" {
        $hostArg = if ($args.Count -ge 2) { $args[1] } else { "localhost" }
        $portArg = if ($args.Count -ge 3) { $args[2] } else { "50051" }
        & java -cp (Get-Classpath "worker") com.cs324.worker.WorkerNode $hostArg $portArg
    }
    "client" {
        $hostArg = if ($args.Count -ge 2) { $args[1] } else { "localhost" }
        $callbackArg = if ($args.Count -ge 3) { $args[2] } else { "1199" }
        $grpcArg = if ($args.Count -ge 4) { $args[3] } else { "50051" }
        & java -cp (Get-Classpath "client") com.cs324.client.ClientApp $hostArg $callbackArg $grpcArg
    }
    default {
        Write-Host "Usage: .\run.ps1 build | bootstrap [grpcPort] [--state-file path] | worker [host] [grpcPort] | client [host] [callbackPort] [grpcPort] | reset-state [stateFile]"
    }
}
