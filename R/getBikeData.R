#' Get bike data for a city
#' @export 
get_bikes <- function(city) {
  # Find network ID
  nets <- jsonlite::fromJSON("http://api.citybik.es/v2/networks")$networks
  id <- nets$id[grepl(city, nets$location$city, ignore.case = TRUE)][1]
  
  if(is.na(id)) stop("City not found")
  
  # Get station data
  url <- paste0("http://api.citybik.es/v2/networks/", id)
  stations <- jsonlite::fromJSON(url)$network$stations
  
  tibble::tibble(
    name = stations$name,
    bikes = stations$free_bikes,
    slots = stations$empty_slots,
    lat = stations$latitude,
    lng = stations$longitude
  )
}