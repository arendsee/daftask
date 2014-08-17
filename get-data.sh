#!/bin/bash

# This scripts prepares the raw data needed by daftask
# Output:
# 1) pgi2taxid.dmp - map of protein gis to taxids
#    pgi | taxid 
# 2) ngi2taxid.dmp - map of nucleotide gis to taxids
#    ngi | taxid 
# 3) taxid2sciname.dmp - map of taxids to scientific names
#    taxid | sciname

# Base directory for the ncbi taxonomy database
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
    awk 'BEGIN{FS="|"; OFS="\t"} $4 == "scientific name" {print $1, $2}' |
    sed '1i taxid\tsciname' > taxid2sciname.dmp

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
    letter=$(sed -r 's/gi_taxid_(.).*/\1/' <<< $f)
    sed '1i '$letter'gi\ttaxid' $f > ${letter}gi2taxid.dmp
done

rm gi_taxid*
