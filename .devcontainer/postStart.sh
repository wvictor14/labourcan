#!/bin/bash

# renv
R -q -e 'renv::restore()'
R -q -e 'renv::install()'

uv sync --frozen