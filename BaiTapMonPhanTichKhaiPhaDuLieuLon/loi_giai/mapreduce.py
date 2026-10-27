"""Mô phỏng Map → Shuffle → Reduce bằng Python chuẩn; không cần Spark."""
from collections import defaultdict
from itertools import combinations
from math import isfinite, sqrt


def cooccurrence(transactions):
    """Mỗi giỏ đóng góp tối đa 1 lần cho một cặp sản phẩm không thứ tự."""
    shuffled = defaultdict(list)
    for basket in transactions:
        for pair in combinations(sorted(set(basket)), 2):  # Map
            shuffled[pair].append(1)  # Shuffle
    return {pair: sum(values) for pair, values in shuffled.items()}  # Reduce


def _validate(points, centers):
    if not points or not centers or not centers[0]:
        raise ValueError('Cần dữ liệu và ít nhất một tâm có số chiều dương.')
    dim = len(centers[0])
    if any(len(x) != dim or not all(isfinite(v) for v in x) for x in points + centers):
        raise ValueError('Số chiều phải đồng nhất; tọa độ phải hữu hạn.')


def kmeans_step(points, centers):
    """Một job MapReduce. Cụm rỗng giữ tâm cũ; hòa khoảng cách chọn ID nhỏ."""
    points = [tuple(x) for x in points]
    centers = [tuple(c) for c in centers]
    _validate(points, centers)
    shuffled = defaultdict(list)
    for x in points:
        cluster = min(range(len(centers)),
                      key=lambda j: sum((a-b)**2 for a,b in zip(x,centers[j])))
        shuffled[cluster].append((x, 1))  # Map: (cluster_id, (vector, count))
    updated = centers.copy()
    for cluster, values in shuffled.items():
        count = sum(n for _,n in values)
        updated[cluster] = tuple(sum(x[d] for x,_ in values)/count
                                 for d in range(len(centers[0])))
    return updated


def kmeans(points, initial_centers, tol=1e-4, max_iter=100):
    if tol < 0 or not isfinite(tol) or max_iter < 1:
        raise ValueError('tol phải hữu hạn >= 0; max_iter >= 1.')
    centers = [tuple(c) for c in initial_centers]
    points = [tuple(x) for x in points]
    _validate(points, centers)
    history = []
    for iteration in range(1, max_iter + 1):
        new_centers = kmeans_step(points, centers)
        shift = max(sqrt(sum((a-b)**2 for a,b in zip(old,new)))
                    for old,new in zip(centers,new_centers))
        history.append({'iteration': iteration, 'centers': new_centers, 'max_shift': shift})
        centers = new_centers
        if shift <= tol:
            break
    return centers, history


if __name__ == '__main__':
    print('Cặp sản phẩm:', cooccurrence([
        ['Sữa', 'Bánh mì', 'Trứng'], ['Sữa', 'Bánh mì'], ['Sữa', 'Sữa', 'Trứng']]))
    centers, history = kmeans([(0.,0.), (2.,2.), (10.,10.), (12.,12.)],
                              [(0.,0.), (10.,10.)])
    print('Các tâm cuối:', centers)
    for row in history:
        print(row)
