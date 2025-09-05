#!/bin/bash

# create container with new cache
mkdir -p /renv/cache
chmod -R 777 /renv/cache
mkdir -p home/rstudio/.cache
chmod -R 777 home/rstudio/.cache
