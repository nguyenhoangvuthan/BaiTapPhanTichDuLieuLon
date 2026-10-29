"""AUC phải xếp hạng xác suất lớp 1, không phải số mẫu lớp 1 trong lá cây."""
import ast
from pathlib import Path
import os
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
class MLMetricsTests(unittest.TestCase):
    def test_tree_auc_uses_positive_class_probability(self):
        os.environ['SPARK_LOCAL_IP']='127.0.0.1';os.environ['PYSPARK_PYTHON']=sys.executable
        import nbformat
        from pyspark.sql import SparkSession
        from pyspark.ml.linalg import Vectors
        from pyspark.ml.evaluation import BinaryClassificationEvaluator
        spark=(SparkSession.builder.master('local[2]').appName('metric-fixture')
            .config('spark.ui.enabled','false').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        try:
            fixture=spark.createDataFrame([
                (0.,Vectors.dense([40.,60.]),Vectors.dense([.4,.6])),
                (1.,Vectors.dense([1.,9.]),Vectors.dense([.1,.9]))
            ],['high_tip','rawPrediction','probability'])
            n=nbformat.read(ROOT/'loi_giai/03_Lab2_ML_NguyenHoangVuThan.ipynb',as_version=4)
            source=next(c.source for c in n.cells if c.cell_type=='code' and 'evaluator_auc = BinaryClassificationEvaluator' in c.source)
            statement=next(node for node in ast.parse(source).body if isinstance(node,ast.Assign)
                           and any(isinstance(t,ast.Name) and t.id=='evaluator_auc' for t in node.targets))
            ctx={'BinaryClassificationEvaluator':BinaryClassificationEvaluator}
            exec(compile(ast.Module(body=[statement],type_ignores=[]),'evaluator cell','exec'),ctx)
            self.assertAlmostEqual(ctx['evaluator_auc'].evaluate(fixture),1.0)
        finally:spark.stop()

if __name__=='__main__':unittest.main()
