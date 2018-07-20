#!/bin/bash
export var=$1
export len=${#var}
export file=${var:0:len-4}
export dataprefix="Data/"
export data=${dataprefix}${file##*/}
iconv -f gbk -t UTF-8 $file.csv > ${file}_tmp.csv
dos2unix ${file}_tmp.csv
python strip_blank.py ${file}_tmp.csv > ${file}.csv
rm ${file}_tmp.csv
bean-extract my.config ${file}.csv > ${data}.beancount