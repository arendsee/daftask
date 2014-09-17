#!/usr/bin/env python3

import unittest
import os
import daftask
import tempfile

def vector2file(x, filename):
    x = ['\t'.join([str(z) for z in y]) for y in x]
    x = '\n'.join([y for y in x])
    with open(filename, 'wb') as f:
        f.write(bytes(x, 'ascii'))

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.datadir = tempfile.mkdtemp()
        self.ngi2taxid = os.path.join(self.datadir, 'ngi2taxid.dmp')
        self.pgi2taxid = os.path.join(self.datadir, 'pgi2taxid.dmp')
        self.taxid2sciname = os.path.join(self.datadir, 'taxid2sciname.dmp')
        self.dbname = os.path.join(self.datadir, 'test.db')

        nt = [['ngi', 'taxid'], [10, 100], [11, 101]]
        pt = [['pgi', 'taxid'], [20, 100], [21, 101]]
        tn = [['taxid', 'sciname'], [100, 'Undead unicorn'], [101, 'Flying monkey']]

        vector2file(nt, self.ngi2taxid)
        vector2file(pt, self.pgi2taxid)
        vector2file(tn, self.taxid2sciname)

        self.db = daftask.Database(datadir=self.datadir, dbname=self.dbname)

    def tearDown(self):
        os.remove(self.ngi2taxid)
        os.remove(self.pgi2taxid)
        os.remove(self.taxid2sciname)
        os.remove(self.dbname)

    def _build_db(self):
        try:
            self.db.build()
        except Exception:
            print("Cannot perform test if db.build() fails", file=sys.stderr)
            sys.exit(1)

    def test_build_succeeds(self):
        try:
            raised = False
            self.db.build()
        except Exception as e:
            print(e)
            raised = True
        self.assertFalse(raised, "Database build failed")

    def test_map(self):
        self._build_db()

        fromto = ['ngi', 'taxid']
        input_ = ['10', '11']
        self.assertEqual(self.db.map(fromto, input_), [(10, 100), (11, 101)])

        fromto = ['pgi', 'taxid']
        input_ = ['20', '21']
        self.assertEqual(self.db.map(fromto, input_), [(20, 100), (21, 101)])

        fromto = ['taxid', 'sciname']
        input_ = ['100', '101']
        self.assertEqual(self.db.map(fromto, input_), [(100, 'Undead unicorn'),
                                                       (101, 'Flying monkey')])


if __name__ == '__main__':
    unittest.main()
