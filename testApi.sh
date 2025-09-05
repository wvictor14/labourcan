curl -s "http://api.citybik.es/v2/networks" | jq '.networks[] | select(.location.city | contains("Vancouver"))'

# {
#   "id": "mobibikes",
#   "name": "Mobi",
#   "location": {
#     "latitude": 49.2827,
#     "longitude": -123.1207,
#     "city": "Vancouver",
#     "country": "CA"
#   },
#   "href": "/v2/networks/mobibikes",
#   "company": [
#     "Vanncouver Bike Share Inc.",
#     "CycleHop LLC",
#     "City of Vancouver",
#     "Shaw Communications Inc.",
#     "Fifteen"
#   ],
#   "gbfs_href": "https://gbfs.kappa.fifteen.eu/gbfs/2.2/mobi/en/gbfs.json"
# }