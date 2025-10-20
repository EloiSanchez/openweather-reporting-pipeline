# TFM Eloi UOC 2025-2026

## Ingestion

### Deploy to Azure Functions

Ensure azure CLI has been logged in:

```bash
az login
```

Upload new code to Azure Functions:

```bash
func azure functionapp publish <APP_NAME> --build=local
```

in my particular case `<APP_NAME>=ingest-openweather-eloi`.

This will deploy an azure function that can be triggered via a GET request to
`https://ingest-openweather-eloi.azurewebsites.net/api/ingest-openweather-eloi` with
the corresponding function code as a parameter for authorization.
