#!/bin/bash

if [ -z $2 ]; then
    echo "Usage: ./processing file.config file.csv"
    exit 1
fi

if [ ! -f $1 ]; then
    echo "Config file $1 does not exists."
    exit 2
fi

if [ ! -f $2 ]; then
    echo "Data file $2 does not exists."
    exit 3
fi


export config=$1
export var=$2
export len=${#var}
export filePostfix=${var:len-4:len}


if [ $filePostfix != ".csv" ]; then
    echo "Data file should be csv file."
    exit 4
fi

export fileCoding=$(file $2)
export fileName=${var:0:len-4}
export dataPrefix="Data/"
export data=${dataPrefix}${fileName##*/}


if [[ $fileCoding == *"ISO-8859"* || $fileCoding == *"Non-ISO"* ]]; then
    iconv -f gbk -t UTF-8 $fileName.csv > ${fileName}_tmp.csv
elif [[ $fileCoding == *"UTF-8"* ]]; then
    cp $fileName.csv ${fileName}_tmp.csv
else
    echo "Unkown file coding of data file."
    exit 5
fi


dos2unix ${fileName}_tmp.csv 2> /dev/null
python strip_blank.py ${fileName}_tmp.csv > ${fileName}-utf8.csv
rm ${fileName}_tmp.csv
bean-extract $config ${fileName}-utf8.csv > ${data}.beancount