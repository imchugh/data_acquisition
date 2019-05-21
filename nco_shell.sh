#!/bin/bash

# Here we subset site data from continental-scale ACCESS files (by coords)
# then concatenate to existing site files (placed into month folders);
# the process is complicated by te fact that the ACCESS runs initialise at time
# zero, so the rainfall for that time does not include the accumulated rainfall 
# since the previous time period - the solution is to keep the rainfall 
# forecast for the 7th hour, and then add that as the zero hour rainfall for 
# the next 6-hour period 

# Initialise vars
SITE="$1"
DATETIME="$2"
LATITUDE="$3"
LONGITUDE="$4"
DELTA=0.165
BASE_DIR=/rdsi/market/access_test 

# Get latitude and longitude ranges
LAT_LO=$(echo "$LATITUDE - $DELTA"|bc)
LAT_HI=$(echo "$LATITUDE + $DELTA"|bc)
LONG_LO=$(echo "$LONGITUDE - $DELTA"|bc)
LONG_HI=$(echo "$LONGITUDE + $DELTA"|bc)

echo Beginning netCDF processing with NCO
echo "Running site" $SITE "; Base datetime:" $DATETIME

# Enter base directory
cd $BASE_DIR

# 1) Cut out SITE subset from continental file
for entry in Continental_files/$DATETIME*.tmp
do
  FNAME=$(basename $entry)
  ncks -O -d lat,$LAT_LO,$LAT_HI -d lon,$LONG_LO,$LONG_HI "$entry" Working_files/"$SITE"_"$DATETIME"_"${FNAME:11:3}"."tmp"
done

# 2) Create concatenated files and do rainfall differencing
cd Working_files
ncrcat -O "$SITE"_"$DATETIME"_00[012345].tmp temp012345.nc
ncrcat -O "$SITE"_"$DATETIME"_00[123456].tmp temp123456.nc
ncdiff -O -v accum_prcp temp123456.nc temp012345.nc precip_inst.nc
ncrename -v accum_prcp,inst_prcp precip_inst.nc

# 3) Concatenate previously determined rainfall for 00 with 01-06;
#    If not available, fill with missing data values
RAIN_FILE="$BASE_DIR"/Precip_forecast_files/"$SITE"_"$DATETIME"_precip.nc
if [ -e $RAIN_FILE ]
then
    echo "Found prior forecast file 006 - appending data record"
    ncrcat -O "$RAIN_FILE" precip_inst.nc new_precip_inst.nc
    rm $RAIN_FILE
else
    echo "Did not find prior forecast file 006 - appending empty record"
    ncks -O -d time,0 -v accum_prcp temp012345.nc dummy1.nc
    ncrename -v accum_prcp,inst_prcp dummy1.nc
    ncap2 -O -s 'inst_prcp(:,:,:)=1.0e32' dummy1.nc dummy2.nc
    ncrcat -O dummy2.nc precip_inst.nc new_precip_inst.nc
fi

# 4) Write new rainfall record to existing file
ncks -O -d time,0,5 new_precip_inst.nc trunc_precip_inst.nc
ncks -A -v inst_prcp trunc_precip_inst.nc temp012345.nc

# 5) Rename file
#NEW_NAME="$SITE"_"$DATETIME".nc
#echo New name $NEW_NAME
#mv temp012345.nc "$SITE"_"$DATETIME".nc

# 5) Cut out the rainfall file to be prepended to future dataset (at hour 00, 06, 12 or 18)
NEW_DATETIME=$(date -d "${DATETIME:0:8} ${DATETIME:8:2} +6 hour" '+%Y%m%d%H')
NEXT_RAIN_FILE="$BASE_DIR"/Precip_forecast_files/"$SITE"_"$NEW_DATETIME"_precip.nc
ncks -O -d time,5 precip_inst.nc forecast_precip.nc
NEW_STRING='days since '$(date -d "${DATETIME:0:8} ${DATETIME:8:2} +6 hour" '+%F %H')':0:0'
ncap2 -O -s 'time=array(0.0,1,$time)' -s "time@units=\"$NEW_STRING\"" forecast_precip.nc "$NEXT_RAIN_FILE"

# 6) Write the data to the existing file if exists, else rename in monthly directory
YEARMONTH="${DATETIME:0:6}"
if [ ! -d ../Monthly_files/$YEARMONTH ]
then
    echo "No monthly directory available... created!"
    mkdir ../Monthly_files/$YEARMONTH
fi

MONTHLY_FILE=../Monthly_files/$YEARMONTH/$SITE.nc
MONTHLY_FILE_OLD=../Monthly_files/$YEARMONTH/$SITE_old.nc
if [ ! -f $MONTHLY_FILE ]
then
    echo "No monthly file written... created"
    mv temp012345.nc $MONTHLY_FILE
else
    mv $MONTHLY_FILE $MONTHLY_FILE_OLD
    ncrcat $MONTHLY_FILE_OLD temp012345.nc $MONTHLY_FILE
fi
# 
# 7) Remove all working files
rm -r *