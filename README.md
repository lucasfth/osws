# osws

## Setup database locally

Install postgresql
```
createdb osws_dev

dotnet ef database update --project OSWS.KeyManager --startup-project OSWS.WebApi
```

## Run migrations

```
dotnet ef migrations add <Name>  --project OSWS.KeyManager --startup-project OSWS.WebApi 
```

