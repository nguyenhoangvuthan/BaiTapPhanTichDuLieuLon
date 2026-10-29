"""Kiểm chứng cả snapshot, xác suất và ngưỡng khi nạp lại mô hình AML."""
import os
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'loi_giai'))


class AMLPersistenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
        os.environ['PYSPARK_PYTHON'] = sys.executable
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master('local[2]').appName('AML-persistence')
                     .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_round_trip_preserves_scores_threshold_and_unseen_accounts(self):
        from aml_model import AMLModelBundle, attach_history, load_aml_bundle, save_aml_bundle
        from pyspark.ml import Pipeline
        from pyspark.ml.classification import LogisticRegression
        from pyspark.ml.feature import VectorAssembler

        snapshot = self.spark.createDataFrame([('A', 2.), ('B', 7.)], ['id', 'in_degree'])
        train = self.spark.createDataFrame([
            ('A', 'B', float(i), float(i % 3 != 0)) for i in range(1, 13)
        ], ['src', 'dst', 'amount', 'is_laundering'])
        inference = self.spark.createDataFrame([
            ('known', 'A', 'B', 6.), ('unseen', 'NEW', 'B', 9.),
        ], ['transaction_id', 'src', 'dst', 'amount'])
        for with_graph in (False, True):
            with self.subTest(with_graph=with_graph), tempfile.TemporaryDirectory() as directory:
                features = ['amount'] + (['src_in_degree', 'dst_in_degree', 'src_unseen', 'dst_unseen'] if with_graph else [])
                training = attach_history(train, snapshot, ['in_degree']) if with_graph else train
                model = Pipeline(stages=[VectorAssembler(inputCols=features, outputCol='features'),
                    LogisticRegression(labelCol='is_laundering', regParam=.1, maxIter=10)]).fit(training)
                original = save_aml_bundle(model, snapshot, directory, threshold=.9,
                    graph_metrics=['in_degree'], with_graph=with_graph,
                    metadata={'cut_history_epoch': 123., 'aml_fraction': .02})
                restored = load_aml_bundle(self.spark, directory)
                before = original.predict(inference).orderBy('transaction_id').collect()
                after = restored.predict(inference).orderBy('transaction_id').collect()
                self.assertEqual(restored.metadata['threshold'], .9)
                self.assertEqual(restored.metadata['cut_history_epoch'], 123.)
                self.assertTrue(any(r.score > .5 and r.prediction == 0 for r in after),
                                'Fixture phải phân biệt ngưỡng đã lưu với ngưỡng mặc định .5')
                for left, right in zip(before, after):
                    self.assertEqual(left.transaction_id, right.transaction_id)
                    self.assertAlmostEqual(left.score, right.score, places=12)
                    self.assertEqual(left.prediction, right.prediction)
                    self.assertEqual(right.prediction, float(right.score >= .9))
                if with_graph:
                    unseen = next(r for r in after if r.transaction_id == 'unseen')
                    self.assertEqual(unseen.src_unseen, 1.)
                    self.assertEqual(unseen.src_in_degree, 0.)
                    self.assertEqual(unseen.dst_unseen, 0.)
                # Quy tắc bao gồm đúng biên threshold, không phải dấu >.
                score = after[0].score
                boundary = AMLModelBundle(restored.model, restored.snapshot, dict(restored.metadata, threshold=score))
                row = boundary.predict(inference.filter("transaction_id = 'known'")).first()
                self.assertEqual(row.prediction, 1.)


if __name__ == '__main__':
    unittest.main()
