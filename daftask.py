#!/usr/bin/env python3

import os
import sqlite3
import sys

def parse():
    parser = argparse.ArgumentParser(
        description="Database for taxonomic and sequence id conversions"
    )

    parser.add_argument(
        'input',
        help='Input list of things to convert',
        nargs='*',
        default=sys.stdin
    )

    parser.add_argument(
        '-u', '--update',
        help='download current data and rebuild database',
        action='store_true',
        default=False
    )

    parser.add_argument(
        '-t', '--fromto',
        help='Input and output identifier classes',
        nargs=2,
        choices=['gi', 'taxid', 'sciname']
    )

    args = parser.parse_args()
    return(args)

class Database:
    def __init__(self):
        wkdir = os.path.dirname(os.path.abspath(__file__))
        self.datadir = os.path.join(wkdir, 'data')
        dbname = os.path.join(wkdir, 'daftask.db')
        self.tbls = ['taxid2name', 'pgi2taxid', 'ngi2taxid']
        self.con = sqlite3.connect(dbname)
        self.cur = self.con.cursor()

        if not all([self.has_table(t, self.cur) for t in self.tbls]):
            self.initialize(self.cur, self.tbls)

    def __del__(self):
        self.con.commit()
        self.con.close()

    def has_table(self, table, cur):
        cmd = """SELECT name
                 FROM sqlite_master
                 WHERE type='table'
                 AND name='{}' COLLATE NOCASE""".format(table)
        result = self.fetch(cmd, cur)
        table_exists = True if len(result) > 0 else False
        return(table_exists)

    def fetch(self, cmd, cur):
        try:
            cur.execute(cmd)
            result = cur.fetchall()
        except Exception as e:
            self._sql_err(e, cmd)
        return(result)

    def _sql_err(e, cmd):
        print("Error on command\n{}".format(cmd), file=sys.stderr)
        print(e, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    def tbl_is_empty(self, cur, table):
        cmd = "SELECT * from {} LIMIT 1" % table
        result = fetch(cmd)

    def initialize(self, cur, tbls):

        columns = {
            'taxid2name' : ('taxid INTEGER',
                            'name INTEGER',
                            'class TEXT'),
            'ngi2taxid'  : ('gi INTEGER PRIMARY KEY',
                            'taxid INTEGER'),
            'pgi2taxid'  : ('gi INTEGER PRIMARY KEY',
                            'taxid INTEGER')
        }

        indices = {
            'taxid2name' : ['taxid', 'name'],
            'ngi2taxid'  : ['taxid'],
            'pgi2taxid'  : ['taxid']
        }


        drop_table   = "DROP TABLE IF EXISTS {table}"
        create_index = "CREATE INDEX {table}_{index} ON {table} ({index})"
        create_table = "CREATE TABLE {table}({columns})"

        cmds = []
        for tbl in tbls:
            try:
                cmds.append(drop_table.format(table=tbl))
                cmds.append(create_table.format(table=tbl,
                                                columns=','.join(columns[tbl])))
                for index in indices[tbl]:
                    cmds.append(create_index.format(table=tbl, index=index))
            except KeyError:
                print("Table %s is not supported" % tbl, file=sys.stderr)
                system.exit(1)

        Database.serial_execution(cmds, cur)

    def update(self):
        def rowgen(f):
            for line in f:
                yield line.rstrip().split("\t")

        for tbl in self.tbls:
            filename = os.path.join(self.datadir, tbl + '.dmp~')
            with open(filename, 'r') as f:
                header = f.readline().split('\t')
                cmd = "INSERT INTO {} ({}) VALUES ({})".format(
                            tbl,
                            ','.join(header),
                            ','.join(['?'] * len(header))
                        )
                Database.execute_many(cmd, rowgen(f), self.cur)

    @classmethod
    def serial_execution(cls, cmds, cur):
        for cmd in cmds:
           try:
                cur.execute(cmd)
           except sqlite3.OperationalError as e:
                print("Error on command:\n{}".format(cmd))
                print(e, file=sys.stderr)
                sys.exit(1)

    @classmethod
    def execute_many(cls, cmd, val, cur):
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

    db = Database()

    if args.update:
        db.update()
