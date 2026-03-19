# register_connector.ps1

# Read .env file
Get-Content .env | ForEach-Object {
    if ($_ -match "^(.*?)=(.*)$") {
        [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2])
    }
}

# Get values
$accountName = [System.Environment]::GetEnvironmentVariable("AZURE_STORAGE_ACCOUNT_NAME")
$accountKey  = [System.Environment]::GetEnvironmentVariable("AZURE_STORAGE_ACCOUNT_KEY")

# Wait for Kafka Connect to be ready
Write-Host "Waiting for Kafka Connect to be ready..."
$maxAttempts = 20
$attempt = 0
$ready = $false

while (-not $ready -and $attempt -lt $maxAttempts) {
    $attempt++
    try {
        Invoke-RestMethod -Uri "http://localhost:8083/" -TimeoutSec 5 | Out-Null
        Write-Host "Kafka Connect is up! (attempt $attempt)"
        $ready = $true
    } catch {
        Write-Host "Attempt $attempt/$maxAttempts - not ready yet, waiting 5s..."
        Start-Sleep -Seconds 5
    }
}

if (-not $ready) {
    Write-Host "ERROR: Kafka Connect did not become ready. Run: docker logs kafka-connect"
    exit 1
}

# Pre-create license topic with RF=1 to avoid Confluent license error
Write-Host "Pre-creating _confluent-command topic..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 `
    --create --if-not-exists `
    --topic _confluent-command `
    --partitions 1 `
    --replication-factor 1

# Delete existing connector if present
Write-Host "Deleting existing connector if present..."
try {
    Invoke-RestMethod -Method Delete -Uri "http://localhost:8083/connectors/adls-sink-connector" | Out-Null
    Write-Host "Old connector deleted."
    Start-Sleep -Seconds 3
} catch {
    Write-Host "No existing connector found (that's fine)."
}

# Build connector config
$config = @{
    name   = "adls-sink-connector"
    config = @{
        "connector.class"                    = "io.confluent.connect.azure.blob.AzureBlobStorageSinkConnector"
        "tasks.max"                          = "3"
        "topics"                             = "ecom.orders,ecom.payments,ecom.customers,ecom.inventory,ecom.clickstream"
        "azblob.account.name"                = $accountName
        "azblob.account.key"                 = $accountKey
        "azblob.container.name"              = "ecommerce-bronze"
        "format.class"                       = "io.confluent.connect.azure.blob.format.json.JsonFormat"
        "flush.size"                         = "10"
        "rotate.interval.ms"                 = "10000"
        "storage.class"                      = "io.confluent.connect.azure.blob.storage.AzureBlobStorage"
        "confluent.topic.bootstrap.servers"  = "kafka:29092"
        "confluent.topic.replication.factor" = "1"
        "topics.dir"                         = "bronze"
        "key.converter"                      = "org.apache.kafka.connect.storage.StringConverter"
        "value.converter"                    = "org.apache.kafka.connect.json.JsonConverter"
        "value.converter.schemas.enable"     = "false"
    }
}

$json = $config | ConvertTo-Json -Depth 5

# Register connector
Write-Host "Registering connector..."
try {
    $response = Invoke-RestMethod `
        -Method Post `
        -Uri "http://localhost:8083/connectors" `
        -ContentType "application/json" `
        -Body $json
    Write-Host "Connector registered successfully!"
    Write-Host ($response | ConvertTo-Json -Depth 5)
} catch {
    Write-Host "ERROR registering connector:"
    Write-Host $_.Exception.Message
    $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
    Write-Host $reader.ReadToEnd()
    exit 1
}

# Check status
Start-Sleep -Seconds 5
Write-Host "`nConnector status:"
Invoke-RestMethod -Uri "http://localhost:8083/connectors/adls-sink-connector/status" | ConvertTo-Json -Depth 5