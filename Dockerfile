# Consider building container once, vetting it, only update when required, store on registry in GCP
# Look into FedRAMPED containers or vulnerability scanning
# https://www.fedramp.gov/assets/resources/documents/Vulnerability_Scanning_Requirements_for_Containers.pdf

# Use the Rocker R base image
FROM rocker/r-ver:4.3.1

# Install system dependencies required for R packages
#   libcurl4-openssl-dev: Required by httr, curl, and bigrquery for HTTP requests and API calls
#   libssl-dev: Needed by httr, openssl, and plumber for SSL/TLS encryption support
#   libxml2-dev: Required by xml2, XML, and rvest for XML/HTML parsing
#   zlib1g-dev: Required by gzip, plumber, and httpuv for compression support in data transmission
#   libsodium-dev: Needed by sodium and httpuv for cryptographic operations (e.g., encryption)
#   build-essential: Provides compilers and essential build tools for R packages with C/C++ code, such as httpuv and plumber
#   curl: for internal testing
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    zlib1g-dev \
    libsodium-dev \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install required R packages
RUN install2.r --error \
    dplyr \
    DBI \
    plumber \
    bigrquery \
    glue

# Copy application files from the project root
COPY get_roi_physical_activity_scores.R .
COPY plumber_api.R .

# Define the entrypoint to start the Plumber API
ENTRYPOINT ["R", "-e", "pr <- plumber::plumb('plumber_api.R'); pr$run(host='0.0.0.0', port=as.numeric(Sys.getenv('PORT')))"]
