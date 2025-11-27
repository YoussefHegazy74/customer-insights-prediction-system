FROM astrocrpublic.azurecr.io/runtime:3.1-3

# Install Microsoft ODBC Driver 17 for SQL Server (Debian/Ubuntu variant)
USER root

# Install required dependencies
RUN apt-get update && \
    apt-get install -y curl apt-transport-https gnupg

# Add Microsoft repository and public key in the modern, secure way
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-archive-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-archive-keyring.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER astro
