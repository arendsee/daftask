#!/bin/bash


DAFT_HOME=$PWD
DATABASE=$DAFT_HOME/daftask.db
DATA_DIR=$DAFT_HOME/data
ALIAS_DIR=$DAFT_HOME/aliases

function prepare_data {

    # This function prepares the raw data needed by daftask
    # Output:
    # 1) pgi2taxid.dmp - map of protein gis to taxids
    #    pgi | taxid 
    # 2) ngi2taxid.dmp - map of nucleotide gis to taxids
    #    ngi | taxid 
    # 3) taxid2sciname.dmp - map of taxids to scientific names
    #    taxid | sciname

    # Base directory for the ncbi taxonomy database
    base='ftp://ftp.ncbi.nih.gov/pub/taxonomy'

    if [[ ! -d DATA_DIR ]]; then
       mkdir DATA_DIR
    fi

    cd DATA_DIR

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

    cd ..

}


function make_alias {

    afile="${ALIAS_DIR}/${1}2${2}"
echo '#!/bin/bash

if [[ $(tty) == "not a tty" ]]; then
    in=$(cat)
elif [[ ! -z $@ ]]; then
    in=$@
else
    echo "No input detected" > /dev/stderr
fi

echo $in | DAFT_HOME/daftask.py -d INFIELD OUTFIELD --database DATABASE --data-directory DATA_DIR
' |
    sed "s|DAFT_HOME|$DAFT_HOME|;
         s|INFIELD|$1|;
         s|OUTFIELD|$2|;
         s|DATABASE|$DATABASE|;
         s|DATA_DIR|$DATA_DIR|" > $afile

    chmod 755 $afile

}


function make_aliases {


    if [[ ! -d aliases ]]; then
       mkdir aliases
    fi
    
    for cols in 'pgi taxid' 'ngi taxid' 'taxid sciname'; do
        for from in $cols; do
            for to in $cols; do
                if [[ $from != $to ]]; then
                    make_alias $from $to
                fi
            done
        done
    done

}

make_aliases
