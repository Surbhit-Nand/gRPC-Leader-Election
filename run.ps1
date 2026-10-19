# Helper to build and launch cluster nodes with the correct classpath.
# The project has gRPC runtime dependencies, so every node needs
# "target/classes;target/dependency/*" (Windows separator). This script
# centralises that so a stale "target/classes"-only command cannot bite again.
#
# Usage (one terminal per node, project root):
#   .\run.ps1 build                              # compile + fetch runtime jars into target/dependency
#   .\run.ps1 bootstrap [legacyUnusedPort] [grpcPort] # defaults 1099 50051; gRPC only
#   .\run.ps1 worker [host] [legacyUnusedPort] [grpcPort] # default localhost 1099 50051
#                                               # "grpc" remains a compatible third argument
#   .\run.ps1 client [host] [port] [callbackPort] [grpcPort]  # defaults localhost 1099 1199 50051
$ErrorActionPreference = "Stop"

function Get-Mvn {
    $mvn = Get-Command mvn -ErrorAction SilentlyContinue
    if ($mvn) { return $mvn.Source }
    $wrapper = Get-ChildItem "$env:USERPROFILE\.m2\wrapper\dists\apache-maven-*\*\*\bin\mvn.cmd" `
        -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($wrapper) { return $wrapper.FullName }
    throw "Maven not found: install Maven 3.9+ or put mvn on PATH."
}

function Get-Classpath {
    if (-not (Test-Path -LiteralPath "target/classes")) {
        throw "target/classes is missing. Run '.\run.ps1 build' first."
    }
    if (-not (Test-Path -LiteralPath "target/dependency")) {
        throw "target/dependency is missing. Run '.\run.ps1 build' first."
    }
    return "target/classes;target/dependency/*"
}

$command = if ($args.Count -ge 1) { $args[0] } else { "help" }

switch ($command) {
    "build" {
        & (Get-Mvn) compile dependency:copy-dependencies -DincludeScope=runtime
        if ($LASTEXITCODE -ne 0) { throw "Build failed with exit code $LASTEXITCODE." }
        Write-Host "Build OK. Launch nodes with .\run.ps1 bootstrap|worker|client"
    }
    "bootstrap" {
        $port = if ($args.Count -ge 2) { $args[1] } else { "1099" }
        $nodeArgs = @($port)
        if ($args.Count -ge 3) { $nodeArgs += $args[2] }
        & java -cp (Get-Classpath) com.cs324.bootstrap.BootstrapNode @nodeArgs
    }
    "worker" {
        $hostArg = if ($args.Count -ge 2) { $args[1] } else { "localhost" }
        $portArg = if ($args.Count -ge 3) { $args[2] } else { "1099" }
        # All communication uses gRPC. Third arg optionally selects its bootstrap port.
        $nodeArgs = @($hostArg, $portArg)
        if ($args.Count -ge 4) {
            if ($args[3] -eq "grpc") { $nodeArgs = @("--grpc") + $nodeArgs }
            else { $nodeArgs = @("--grpc-port", $args[3]) + $nodeArgs }
        }
        & java -cp (Get-Classpath) com.cs324.worker.WorkerNode @nodeArgs
    }
    "client" {
        $hostArg = if ($args.Count -ge 2) { $args[1] } else { "localhost" }
        $portArg = if ($args.Count -ge 3) { $args[2] } else { "1099" }
        $callbackArg = if ($args.Count -ge 4) { $args[3] } else { "1199" }
        $grpcArg = if ($args.Count -ge 5) { $args[4] } else { "50051" }
        & java -cp (Get-Classpath) com.cs324.client.ClientApp $hostArg $portArg $callbackArg $grpcArg
    }
    default {
        Write-Host "Usage: .\run.ps1 build | bootstrap [legacyUnusedPort] [grpcPort] | worker [host] [legacyUnusedPort] [grpc|grpcPort] | client [host] [legacyUnusedPort] [callbackPort] [grpcPort]"
    }
}
