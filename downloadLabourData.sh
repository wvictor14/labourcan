# download labour data
URL=$(curl -s 'https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/14100022/en' | grep -o 'https://[^"]*')
curl -o 14100022-eng.zip "$URL"
unzip 14100022-eng.zip
rm 14100022-eng.zip