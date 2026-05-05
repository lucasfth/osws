# Stage 1: Build
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy project files for dependency restore (maximizes layer cache reuse)
COPY OSWS.sln .
COPY OSWS.Common/OSWS.Common.csproj OSWS.Common/
COPY OSWS.Models/OSWS.Models.csproj OSWS.Models/
COPY OSWS.KeyManager/OSWS.KeyManager.csproj OSWS.KeyManager/
COPY OSWS.Library/OSWS.Library.csproj OSWS.Library/
COPY OSWS.ParquetSolver/OSWS.ParquetSolver.csproj OSWS.ParquetSolver/
COPY OSWS.WebApi/OSWS.WebApi.csproj OSWS.WebApi/

RUN dotnet restore OSWS.WebApi/OSWS.WebApi.csproj

# Copy source and publish
COPY OSWS.Common/ OSWS.Common/
COPY OSWS.Models/ OSWS.Models/
COPY OSWS.KeyManager/ OSWS.KeyManager/
COPY OSWS.Library/ OSWS.Library/
COPY OSWS.ParquetSolver/ OSWS.ParquetSolver/
COPY OSWS.WebApi/ OSWS.WebApi/

RUN dotnet publish OSWS.WebApi/OSWS.WebApi.csproj \
    -c Release \
    -o /app/publish \
    --no-restore

# Stage 2: Runtime
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/publish .

ENV ASPNETCORE_URLS=http://+:5000

EXPOSE 5000

ENTRYPOINT ["dotnet", "OSWS.WebApi.dll"]
