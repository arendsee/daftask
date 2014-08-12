#!/usr/bin/env python3

import os
import sqlite3
import sys

def parse():
    parser = argparse.ArgumentParser(
        description="Database for taxonomic and sequence id conversions"
    )

    parser.add_argument(
        '-u', '--update',
        help='download current data and rebuild database',
        action='store_true',
        default=True
    )

    parser.add_argument(
        '-t', '--fromto',
        help='Input and output identifier classes',
        nargs=2,
        choices=['gi', 'taxid', 'sciname']
    )

    args = parser.parse_args()
    return(args)

class Data:
    wkdir = os.path.dirname(os.path.abspath(__file__))
    datadir = os.path.join(wkdir, 'data')
    dbname = os.path.join(wkdir, 'daftask.db')

def build_database():
    try:
        os.remove(Data.dbname)
    except:
        pass

    con = sqlite3.connect(Data.dbname)
    cur = con.cursor()


    cmds = [
            "DROP TABLE IF EXISTS ngi2taxid",
            "CREATE TABLE ngi2taxid(gi INTEGER, taxid INTEGER)",
            "CREATE INDEX ng2t_gix ON ngi2taxid (gi)",
            "CREATE INDEX ng2t_taxidx ON ngi2taxid (taxid)",

            "DROP TABLE IF EXISTS pgi2taxid",
            "CREATE TABLE pgi2taxid(gi INTEGER, taxid INTEGER)",
            "CREATE INDEX pg2t_gix ON pgi2taxid (gi)",
            "CREATE INDEX pg2t_taxidx ON pgi2taxid (taxid)",

            "DROP TABLE IF EXISTS taxid2name",
            "CREATE TABLE taxid2name(taxid INTEGER, name INTEGER, class TEXT)",
            "CREATE INDEX t2n_taxidx ON taxid2name (taxid)",
            "CREATE INDEX t2n_namex ON taxid2name (name)"
           ]

    serial_execution(cmds, cur)

    def tabular_gen(filename, delimiter="\t"):
        with open(filename, 'r') as f:
            for line in f:
                yield line.rstrip().split("\t")

    for tbl, ncol in {'ngi2taxid':2, 'pgi2taxid':2, 'taxid2name':3}.items():
        print("Adding data to %s" % tbl, file=sys.stderr)
        cmd = "INSERT INTO {} VALUES ({})".format(tbl, ','.join(['?'] * ncol))
        datfile = os.path.join(Data.datadir, '%s.dmp' % tbl)
        execute_many(cmd, tabular_gen(datfile), cur)

    con.commit()
    con.close()

def serial_execution(cmds, cur):
    for cmd in cmds:
        try:
            cur.execute(cmd)
        except sqlite3.OperationalError as e:
            print("Error on command:\n{}".format(cmd))
            print(e, file=sys.stderr)
            sys.exit(1)

def execute_many(cmd, val, cur):
    try:
        cur.executemany(cmd, val)
    # except sqlite3.OperationalError as e:
    except Exception as e:
        print("Error on command:\n{}".format(cmd))
        print(e, file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    import argparse
    args = parse()
    if args.update:
        build_database()
