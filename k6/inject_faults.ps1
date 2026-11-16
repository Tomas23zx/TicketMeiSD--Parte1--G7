param(
    [string]$PaymentUrl = "http://localhost:8004",
    [int]$FailureRate = 80,
    [int]$FailureDurationSeconds = 120
)

$configureUrl = "$PaymentUrl/payments/configure-faults"

Write-Host "Injecting Payment failures: error_rate=$FailureRate%, latency=0s"
Invoke-RestMethod `
    -Uri $configureUrl `
    -Method Post `
    -ContentType "application/json" `
    -Body (@{
        error_rate = $FailureRate
        latency_seconds = 0
    } | ConvertTo-Json)

Write-Host "Keeping the degradation active for $FailureDurationSeconds seconds..."
Start-Sleep -Seconds $FailureDurationSeconds

Write-Host "Restoring Payment: error_rate=0%, latency=0s"
Invoke-RestMethod `
    -Uri $configureUrl `
    -Method Post `
    -ContentType "application/json" `
    -Body (@{
        error_rate = 0
        latency_seconds = 0
    } | ConvertTo-Json)
