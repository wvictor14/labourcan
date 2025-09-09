# download unadjusted 
cd data

URL=$(curl -s 'https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/14100022/en' | grep -o 'https://[^"]*')
curl -o 14100022-eng.zip "$URL"
unzip 14100022-eng.zip
rm 14100022-eng.zip

# download seasonally adjusted
URL=$(curl -s 'https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/14100355/en' | jq -r '.object')
curl -o 14100355-eng.zip "$URL"
unzip 14100355-eng.zip
rm 14100355-eng.zip