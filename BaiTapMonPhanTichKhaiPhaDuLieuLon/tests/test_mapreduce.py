import importlib.util
from pathlib import Path
import unittest

class MapReduceTests(unittest.TestCase):
    def setUp(self):
        path=Path(__file__).resolve().parents[1]/'loi_giai/mapreduce.py'
        self.assertTrue(path.exists(), 'Chưa có chương trình MapReduce')
        spec=importlib.util.spec_from_file_location('solution',path)
        self.m=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.m)

    def test_unordered_pairs_count_once_per_basket(self):
        self.assertEqual(self.m.cooccurrence([['B','A','A'],['A','B'],['C'],[]]),{('A','B'):2})

    def test_one_update_and_empty_cluster(self):
        self.assertEqual(self.m.kmeans_step([(0.,0.),(2.,2.),(10.,10.)],[(0.,0.),(10.,10.),(99.,99.)]),[(1.,1.),(10.,10.),(99.,99.)])

    def test_driver_converges_and_respects_limit(self):
        centers,history=self.m.kmeans([(0.,0.),(2.,2.),(10.,10.)],[(0.,0.),(10.,10.)])
        self.assertEqual(centers,[(1.,1.),(10.,10.)]);self.assertEqual(history[-1]['max_shift'],0.)
        _,history=self.m.kmeans([(0.,),(2.,)],[(0.,)],max_iter=1)
        self.assertEqual(len(history),1)

    def test_tie_break_and_invalid_input(self):
        self.assertEqual(self.m.kmeans_step([(1.,)],[(0.,),(2.,)]),[(1.,),(2.,)])
        for points,centers in [([],[(0.,)]), ([(1.,)],[]), ([(1.,2.)],[(0.,)]), ([(float('nan'),)],[(0.,)])]:
            with self.assertRaises(ValueError): self.m.kmeans_step(points,centers)

if __name__=='__main__':unittest.main()
