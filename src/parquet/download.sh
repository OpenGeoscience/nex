#!/bin/bash

mkdir -p data

URL_LIST=$(cat <<EOF
http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/rcp45/day/atmos/pr/r1i1p1/v1.0/pr_day_BCSD_rcp45_r1i1p1_CSIRO-Mk3-6-0_2006.nc
http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/rcp45/day/atmos/tasmax/r1i1p1/v1.0/tasmax_day_BCSD_rcp45_r1i1p1_CSIRO-Mk3-6-0_2006.nc
http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/rcp45/day/atmos/tasmin/r1i1p1/v1.0/tasmin_day_BCSD_rcp45_r1i1p1_CSIRO-Mk3-6-0_2006.nc
EOF
        )

echo $URL_LIST | xargs -n 1 -P 8 wget -P data/
