"""Kiểm tra đường dữ liệu thật từ CSV trùng tên cột tới GraphFrame và motif."""
import csv
import os
from pathlib import Path
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]

class AMLGraphTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['SPARK_LOCAL_IP']='127.0.0.1'
        os.environ['PYSPARK_PYTHON']=sys.executable
        import nbformat
        from graphframes import GraphFrame
        from pyspark.sql import SparkSession, functions as F
        cls.spark=(SparkSession.builder.master('local[2]').appName('AML-fixtures')
            .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2')
            .config('spark.jars.packages','io.graphframes:graphframes-spark4_2.13:0.12.3').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.cells=[c.source for c in nbformat.read(ROOT/'loi_giai/04_Lab4_GianLan.ipynb',as_version=4).cells if c.cell_type=='code']
        cls.GraphFrame=GraphFrame;cls.F=F

    @classmethod
    def tearDownClass(cls):cls.spark.stop()

    def load_csv(self,rows):
        header=['Timestamp','From Bank','Account','To Bank','Account','Amount Received','Receiving Currency','Amount Paid','Payment Currency','Payment Format','Is Laundering']
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'transactions.csv'
            with p.open('w',newline='') as f:
                w=csv.writer(f);w.writerow(header);w.writerows(rows)
            os.environ['AML_PATH']=str(p)
            ctx={'ROOT':ROOT,'Path':Path,'os':os,'csv':csv,'spark':self.spark,'F':self.F,'GraphFrame':self.GraphFrame,'AML_FRACTION':1.0}
            source=next(s for s in self.cells if s.startswith('from pyspark.sql.types'))
            exec(source,ctx)
            # Cache and force reading before removing the fixture file.
            ctx['edges'].count();ctx['vertices']=ctx['vertices'].cache();ctx['vertices'].count()
        return ctx

    @staticmethod
    def tx(time,src,dst,amount=1000,currency='USD',src_bank='1',dst_bank='1'):
        return [f'2022/09/01 {time}',src_bank,src,dst_bank,dst,amount,currency,amount,currency,'ACH',0]

    def test_one_node_for_same_bank_account_on_both_sides(self):
        ctx=self.load_csv([self.tx('00:00','A','B'),self.tx('00:10','B','C'),self.tx('00:20','C','A'),self.tx('00:30','D','B')])
        self.assertEqual(ctx['vertices'].count(),4,'Cùng bank/account phải là cùng nút ở cả hai đầu')
        self.assertEqual(ctx['g'].find('(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)').count(),3)

    def test_same_account_different_banks_stays_separate(self):
        ctx=self.load_csv([self.tx('00:00','A','A',src_bank='1',dst_bank='2')])
        self.assertEqual(ctx['vertices'].count(),2)
        row=ctx['edges'].first();self.assertNotEqual(row.src,row.dst)

    def test_mule_excludes_two_hour_boundary_and_other_currency(self):
        ctx=self.load_csv([self.tx('00:00','A','M'),self.tx('01:59','M','B',980),
            self.tx('02:00','M','C',980),self.tx('01:00','M','D',980,'EUR')])
        exec(next(s for s in self.cells if s.startswith('mules=')),ctx)
        result=ctx['mule_result'].collect()
        self.assertEqual(len(result),1)
        self.assertEqual(result[0].sent,980);self.assertEqual(result[0].delay_minutes,119)

    def test_fanin_counts_distinct_senders_and_excludes_self_transfers(self):
        rows=[self.tx('00:10',f'S{i}','M',5000) for i in range(5)]
        ctx=self.load_csv(rows)
        exec(next(s for s in self.cells if s.startswith('MIN_SENDERS=')),ctx)
        result=ctx['fan_in'].collect()
        self.assertTrue(result)
        self.assertTrue(all(r.distinct_senders==5 and r.total_received==25000 for r in result))
        rows=[self.tx('00:10',f'S{i}','M',5000) for i in range(4)]
        rows += [self.tx('00:10','S0','M',5000),self.tx('00:10','M','M',5000)]
        ctx=self.load_csv(rows)
        exec(next(s for s in self.cells if s.startswith('MIN_SENDERS=')),ctx)
        self.assertEqual(ctx['fan_in'].count(),0)

    def test_layering_includes_one_and_five_percent_fees(self):
        ctx=self.load_csv([self.tx('00:00','A','B',1000),self.tx('00:10','B','C',990),self.tx('00:20','C','D',940.5)])
        exec(next(s for s in self.cells if s.startswith('layering=')),ctx)
        result=ctx['layering_result'].collect()
        self.assertEqual(len(result),1)
        self.assertAlmostEqual(result[0].fee_at_B,.01)
        self.assertAlmostEqual(result[0].fee_at_C,.05)

if __name__=='__main__':unittest.main()
