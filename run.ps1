# Helper to build and launch cluster nodes with the correct classpath.
# Each application uses its own module's classes and target/dependency jars.
#
# Usage (one terminal per node, project root):
#   .\run.ps1 build                              # package modules + copy runtime jars (skips test execution)
#   .\run.ps1 bootstrap [grpcPort] [--state-file path] # default 50051
#   .\run.ps1 worker [host] [grpcPort]               # default localhost 50051
#   .\run.ps1 client [host] [callbackPort] [grpcPort] # default localhost 1199 50051
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
        $bootstrapArgs = if ($args.Count -ge 2) { $args[1..($args.Count - 1)] } else { @("50051") }
        & java -cp (Get-Classpath "bootstrap") com.cs324.bootstrap.BootstrapNode @bootstrapArgs
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
        Write-Host "Usage: .\run.ps1 build | bootstrap [grpcPort] [--state-file path] | worker [host] [grpcPort] | client [host] [callbackPort] [grpcPort]"
    }
}
