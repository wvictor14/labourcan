#!/bin/bash

# Set target directory (modify this path as needed)
TARGET_DIR=data/

# Create target directory if it doesn't exist
mkdir -p "$TARGET_DIR"

# Change to target directory
cd "$TARGET_DIR"

# download unadjusted 
# URL=$(curl -s 'https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/14100022/en' | grep -o 'https://[^"]*')
# curl -o 14100022-eng.zip "$URL"
# unzip 14100022-eng.zip
# rm 14100022-eng.zip

# download seasonally adjusted
echo "Downloading to: $(pwd)"
URL=$(curl -s 'https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/14100355/en' | jq -r '.object')
curl -o 14100355-eng.zip "$URL"
unzip -o 14100355-eng.zip
rm 14100355-eng.zip
echo "Download completed in: $TARGET_DIR"