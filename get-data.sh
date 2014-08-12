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
    awk 'BEGIN{FS="|"; OFS="\t"} {print $1, $2, $4}' |
    sed '1i taxid\tname\tclass' > taxid2name.dmp

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

for f in gi_taxid_*.dmp; do
    sed '1i gi\ttaxid' $f > $(sed -r 's/(gi_taxid)_(.).*/\2\1.dmp/' <<< $f | tr '_' '2')
done

rm gi_taxid*
