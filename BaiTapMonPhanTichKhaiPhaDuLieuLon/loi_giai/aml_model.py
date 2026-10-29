"""Lưu và suy luận AML với đúng snapshot quá khứ và ngưỡng validation."""
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F


def attach_history(frame, snapshot, graph_metrics):
    """Ghép hai đầu giao dịch; tài khoản mới có đặc trưng 0 và cờ unseen=1."""
    result = frame
    for endpoint in ('src', 'dst'):
        right = snapshot.select(
            F.col('id').alias(endpoint), F.lit(1).alias(endpoint + '_known'),
            *[F.col(c).alias(endpoint + '_' + c) for c in graph_metrics],
        )
        result = result.join(right, endpoint, 'left').withColumn(
            endpoint + '_unseen', F.col(endpoint + '_known').isNull().cast('double')
        )
    return result.fillna(0, subset=[p + '_' + m for p in ('src', 'dst') for m in graph_metrics])


@dataclass
class AMLModelBundle:
    model: PipelineModel
    snapshot: object
    metadata: dict

    def predict(self, transactions):
        """Nhận giao dịch thô đã chuẩn hóa như edges, không cần nhãn/class_weight."""
        frame = transactions
        if self.metadata['with_graph']:
            frame = attach_history(frame, self.snapshot, self.metadata['graph_metrics'])
        return (self.model.transform(frame)
                .withColumn('score', vector_to_array('probability')[1])
                .withColumn('prediction', (F.col('score') >= self.metadata['threshold']).cast('double')))


def save_aml_bundle(model, snapshot, directory, *, threshold, graph_metrics, with_graph, metadata):
    threshold = float(threshold)
    if not math.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError('Ngưỡng phải hữu hạn và nằm trong [0, 1].')
    path = Path(directory).resolve()
    path.mkdir(parents=True, exist_ok=True)
    # Bỏ marker cũ trước khi ghi: lần lưu dở không được nạp như bộ hoàn chỉnh.
    marker = path / 'metadata.json'
    marker.unlink(missing_ok=True)
    model.write().overwrite().save(str(path / 'pipeline'))
    if with_graph:
        snapshot.write.mode('overwrite').parquet(str(path / 'history_snapshot'))
    info = dict(metadata)
    info.update(schema_version=1, threshold=threshold, with_graph=bool(with_graph),
                graph_metrics=list(graph_metrics), prediction_rule='score >= threshold',
                saved_utc=datetime.now(timezone.utc).isoformat())
    temporary = path / 'metadata.json.tmp'
    temporary.write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(marker)
    return AMLModelBundle(model, snapshot if with_graph else None, info)


def load_aml_bundle(spark, directory):
    path = Path(directory).resolve()
    info = json.loads((path / 'metadata.json').read_text(encoding='utf-8'))
    if info.get('schema_version') != 1 or info.get('prediction_rule') != 'score >= threshold':
        raise ValueError('Định dạng bộ mô hình AML không được hỗ trợ.')
    threshold = float(info['threshold'])
    if not math.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError('Ngưỡng trong metadata không hợp lệ.')
    info['threshold'] = threshold
    model = PipelineModel.load(str(path / 'pipeline'))
    snapshot = spark.read.parquet(str(path / 'history_snapshot')) if info['with_graph'] else None
    return AMLModelBundle(model, snapshot, info)
