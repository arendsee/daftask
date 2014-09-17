#!/usr/bin/env python3

import os
import sqlite3
import sys
import traceback
import string
import collections

__version__ = '1.0.0'

def parse():
    parser = argparse.ArgumentParser(
        description="Database for taxonomic and sequence id conversions"
    )

    parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s {}'.format(__version__))

    parser.add_argument(
        'input',
        help='Input list of things to convert',
        nargs='*',
        default=sys.stdin
    )

    parser.add_argument(
        '-d', '--fromto',
        help='Input and output identifier classes',
        nargs=2,
        choices=['pgi', 'ngi', 'taxid', 'sciname']
    )

    parser.add_argument(
        '-b', '--build',
        help='download current data and rebuild database',
        action='store_true',
        default=False
    )

    parser.add_argument(
        '-s', '--single-row',
        help='only print the output entries',
        action='store_true',
        default=False
    )

    parser.add_argument(
        '--show-cmd',
        help='print SQL query to STDERR',
        action='store_true',
        default=False
    )

    parser.add_argument(
        '--database',
        help='location of database'
    )

    parser.add_argument(
        '--data-directory',
        help='location of data diretory'
    )

    args = parser.parse_args()
    return(args)

class Table:
    def __init__(self, name, coldefs, indices=None, datadir=None):
        self.name = name
        self.coldefs = coldefs
        self.colnames = {s.split()[0] for s in self.coldefs}
        self.indices = set(indices)
        self.dmpfile = self._get_dmpfile(datadir)

    def _get_dmpfile(self, datadir=None):
        if not datadir:
            wkdir   = os.path.dirname(os.path.abspath(__file__))
            datadir = os.path.join(wkdir, 'data')
        dmpfile = os.path.join(datadir, self.name + '.dmp')
        return(dmpfile)

class PGi2Taxid(Table):
    def __init__(self, *args, **kwargs):
        super().__init__(
            name='pgi2taxid',
            coldefs=['pgi INTEGER PRIMARY KEY', 'taxid INTEGER'],
            indices={'taxid'},
            **kwargs
        )

class NGi2Taxid(Table):
    def __init__(self, *args, **kwargs):
        super().__init__(
            name='ngi2taxid',
            coldefs=['ngi INTEGER PRIMARY KEY', 'taxid INTEGER'],
            indices={'taxid'},
            **kwargs
        )

class Taxid2Sciname(Table):
    def __init__(self, *args, **kwargs):
        super().__init__(
            name='taxid2sciname',
            coldefs=['taxid INTEGER', 'sciname INTEGER', 'class TEXT'],
            indices={'taxid'},
            **kwargs
        )

class Database:
    def __init__(self, dbname=None, datadir=None):
        if not dbname:
            wkdir = os.path.dirname(os.path.abspath(__file__))
            dbname = os.path.join(wkdir, 'daftask.db')
        self.con = sqlite3.connect(dbname)
        self.cur = self.con.cursor()
        self.tbls = [PGi2Taxid(datadir=datadir),
                     NGi2Taxid(datadir=datadir),
                     Taxid2Sciname(datadir=datadir)]

    def __del__(self):
        self.con.commit()
        self.con.close()

    def build(self):
        if not all([self._has_table(t.name, self.cur) for t in self.tbls]):
            self._initialize(self.cur, self.tbls)

        def rowgen(f):
            for line in f:
                yield line.rstrip().split("\t")

        for tbl in self.tbls:
            with open(tbl.dmpfile, 'r') as f:
                header = f.readline().split('\t')
                cmd = "INSERT INTO {} ({}) VALUES ({})".format(
                            tbl.name,
                            ','.join(header),
                            ','.join(['?'] * len(header))
                        )
                self._execute_many(cmd, rowgen(f))

    def map(self, fromto, input_, show_cmd=False):
        cmd = "SELECT {} FROM {} WHERE {} IN ({})"
        for tbl in self.tbls:
            if not set(fromto) - tbl.colnames:
                cmd = cmd.format(','.join(fromto),
                                 tbl.name,
                                 fromto[0],
                                 ','.join(input_))
                if show_cmd:
                    print(cmd, file=sys.stderr)
                return(self._fetch(cmd))
    def _initialize(self, cur, tbls):
        drop_table   = "DROP TABLE IF EXISTS {table}"
        create_index = "CREATE INDEX {table}_{index} ON {table} ({index})"
        create_table = "CREATE TABLE {table}({columns})"

        cmds = []
        for tbl in tbls:
            cmds.append(drop_table.format(table=tbl.name))
            cmds.append(create_table.format(table=tbl.name,
                                            columns=','.join(tbl.coldefs)))
            for index in tbl.indices:
                cmds.append(create_index.format(table=tbl.name, index=index))

        self._serial_execution(cmds)

    def _has_table(self, table, cur):
        cmd = """SELECT name
                 FROM sqlite_master
                 WHERE type='table'
                 AND name='{}' COLLATE NOCASE""".format(table)
        result = self._fetch(cmd)
        table_exists = True if len(result) > 0 else False
        return(table_exists)

    def _fetch(self, cmd):
        try:
            self.cur.execute(cmd)
            result = self.cur.fetchall()
        except Exception as e:
            self._sql_err(e, cmd)
        return(result)

    def _sql_err(self, e, cmd):
        print("Error on command\n{}".format(cmd), file=sys.stderr)
        print(e, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    def _tbl_is_empty(self, cur, table):
        cmd = "SELECT * from {} LIMIT 1" % table
        result = _fetch(cmd)

    def _serial_execution(self, cmds):
        for cmd in cmds:
           try:
                self.cur.execute(cmd)
           except sqlite3.OperationalError as e:
                print("Error on command:\n{}".format(cmd))
                print(e, file=sys.stderr)
                sys.exit(1)

    def _execute_many(self, cmd, val):
        try:
            self.cur.executemany(cmd, val)
        # except sqlite3.OperationalError as e:
        except Exception as e:
            print("Error on command:\n{}".format(cmd))
            print(e, file=sys.stderr)
            sys.exit(1)



# =================
# Utility Functions
# =================

def to_quoted_keys(d):
    def quote_noninteger(s):
        if(s.isdigit()):
            return(s)
        else:
            return("'%s'" % s)
    keys = [quote_noninteger(k) for k in d.keys()]
    return(keys)

def prepare_output(args):
    in_ = collections.defaultdict(list)
    for line in args.input:
        row = line.strip().split('\t')
        in_[row[-1]].append(row)

    out_ = collections.defaultdict(list)
    for i,o in Database().map(args.fromto, to_quoted_keys(in_), show_cmd=args.show_cmd):
        out_[str(i)].append(str(o))

    for k,v in in_.items():
        key = k.strip("'")
        for in_row in v:
            in_line = '\t'.join(in_row)
            for out_line in out_[key]:
                if args.single_row:
                    yield out_line
                else:
                    yield "{}\t{}".format(in_line, out_line)

if __name__ == '__main__':
    import argparse
    args = parse()

    if args.build:
        Database().build()

    if args.input and args.fromto:
        for line in prepare_output(args):
            print(line)
