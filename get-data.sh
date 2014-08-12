#!/bin/bash

base='ftp://ftp.ncbi.nih.gov/pub/taxonomy'

if [[ ! -d data ]]; then
   mkdir data
fi

cd data

for f in 'gi_taxid_prot.zip' 'gi_taxid_nucl.zip' 'taxdmp.zip' 'taxcat.zip'; do
    wget -O $f $base/$f
    unzip $f
    if [[ $? == 0 ]]; then
        rm $f
    fi
done

sed 's/[ \t]*|[ \t]*/|/g' names.dmp |
    awk 'BEGIN{FS="|"; OFS="\t"} {print $1, $2, $4}' > taxid2name.dmp

rm categories.dmp
rm citations.dmp
rm delnodes.dmp
rm division.dmp
rm gc.prt
rm gencode.dmp
rm merged.dmp
rm names.dmp
rm nodes.dmp
rm readme.txt

rename 's/gi_taxid_(.).*/$1gi2taxid.dmp/x' *
