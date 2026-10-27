# Lời giải bài tập thuật toán MapReduce

## Bài 1 — Tần suất hai sản phẩm được mua cùng nhau

**Đầu vào:** mỗi bản ghi là một giao dịch `(transaction_id, [item_1, …, item_m])`.
**Đầu ra:** `((a, b), count)` với `a < b`, `a ≠ b`; `count` là số giao dịch chứa cả `a` và `b`.
Một sản phẩm xuất hiện nhiều lần trong cùng giỏ chỉ được tính một lần. Hai dòng giao dịch riêng biệt vẫn tính là hai giao dịch; nếu dữ liệu có dòng trùng do lỗi nhập, phải khử trùng theo `transaction_id` trước.

### Map

```text
MAP(transaction_id, items):
    unique_items = SORT(SET(items))
    FOR i = 0 .. LENGTH(unique_items)-2:
        FOR j = i+1 .. LENGTH(unique_items)-1:
            EMIT((unique_items[i], unique_items[j]), 1)
```

Sắp thứ tự sản phẩm giúp `(Sữa, Trứng)` và `(Trứng, Sữa)` trở thành cùng một khóa. Không sinh cặp `(a, a)`.

### Combiner, Shuffle và Reduce

```text
COMBINE(pair, local_counts):
    EMIT(pair, SUM(local_counts))

SHUFFLE:
    Chuyển tất cả giá trị cùng khóa pair tới cùng một reducer.

REDUCE(pair, partial_counts):
    EMIT(pair, SUM(partial_counts))
```

Phép cộng có tính kết hợp và giao hoán nên combiner tùy chọn không làm thay đổi kết quả, kể cả khi chạy nhiều lần hoặc không được chạy.

### Ví dụ

| Giao dịch | Sản phẩm |
|---|---|
| T1 | Sữa, Bánh mì, Trứng |
| T2 | Sữa, Bánh mì |
| T3 | Sữa, Sữa, Trứng |

Kết quả: `{Bánh mì, Sữa}: 2`, `{Bánh mì, Trứng}: 1`, `{Sữa, Trứng}: 2`.
T3 chỉ đóng góp một lần cho `{Sữa, Trứng}`.

### Độ phức tạp và tính đúng

Với giỏ có `m` sản phẩm khác nhau, Map sinh `m(m−1)/2` cặp. Chi phí toàn bộ là `O(Σ m_t²)` cộng chi phí sắp xếp từng giỏ; tổng dữ liệu shuffle trước combiner cũng tỷ lệ số cặp sinh ra. Mỗi cặp trong một giao dịch được phát đúng một lần, rồi Reduce cộng đúng tất cả giao dịch chứa cặp đó. Cặp có tần suất cao có thể gây lệch tải reducer; dùng cộng gộp cục bộ hoặc salting hai pha nếu cần.

## Bài 2 — K-Means dạng lặp

**Đầu vào:** tập `N` điểm `x ∈ R^d`, số cụm `K`, các tâm ban đầu `C^(0)`, sai số `ε ≥ 0`, số vòng tối đa `T ≥ 1`.
Giả sử `1 ≤ K ≤ N`, các tọa độ hữu hạn và đồng nhất số chiều; dữ liệu lớn được giữ trên hệ thống tệp phân tán. Chọn tâm bằng K-Means++ hoặc mẫu cố định với seed để tái lập.

Mỗi vòng gồm **một job MapReduce**. Driver đưa `K` tâm hiện tại vào distributed cache/broadcast; mỗi mapper chỉ đọc bộ tâm của vòng đó.

### Map — Gán điểm vào tâm gần nhất

```text
MAP(point_id, x):
    j = ARGMIN_j SUM_q (x[q] - C[j][q])²
    # Nếu hòa, chọn j nhỏ nhất để bảo đảm quyết định nhất quán.
    EMIT(j, (x, 1))
```

### Combiner — Tổng vector và số điểm cục bộ

```text
COMBINE(j, [(sum_vector, n), …]):
    EMIT(j, (SUM(sum_vector), SUM(n)))
```

**Không lấy trung bình các trung bình** vì các nhóm có thể có số điểm khác nhau. Giữ cặp `(tổng vector, số lượng)` làm đầu ra Combiner cũng như đầu vào Reducer.

### Reduce — Cập nhật tâm

```text
REDUCE(j, partials):
    S = tổng vector trong partials
    n = tổng số lượng trong partials
    EMIT(j, S / n)
```

Đây là `c_j^(t+1) = (1 / |S_j|) Σ_(x ∈ S_j) x`, nghiệm cực tiểu tổng bình phương khoảng cách trong cụm khi giữ nguyên việc gán điểm.

### Driver — Lặp và kiểm tra dừng

```text
C = initial_centers
FOR t = 1 .. T:
    job = RUN_MAPREDUCE(points, broadcast_centers=C)
    WAIT_FOR_SUCCESS(job)      # Không dùng kết quả job chưa xong/thất bại.
    C_new = COPY(C)
    FOR (j, new_center) IN READ_OUTPUT(job):
        C_new[j] = new_center
    # Cụm không có điểm sẽ không có đầu ra Reduce: giữ tâm cũ.
    delta = MAX_j EUCLIDEAN_DISTANCE(C_new[j], C[j])
    C = C_new
    IF delta <= ε:
        BREAK
RETURN C
```

Phải ghép kết quả theo **ID cụm**, không dựa vào thứ tự tệp đầu ra. Mỗi vòng ghi vào thư mục riêng. Điều kiện dừng là độ dịch chuyển tâm lớn nhất không quá `ε` hoặc đã chạy đủ `T` vòng. Nếu cần nhãn cuối cùng, chạy thêm một lượt Map gán điểm theo bộ tâm cuối.

### Ví dụ kiểm chứng

Các điểm `(0,0), (2,2), (10,10), (12,12)`; tâm đầu `(0,0), (10,10)`.
Vòng 1 tạo tâm `(1,1), (11,11)`, dịch chuyển lớn nhất `√2`.
Vòng 2 cho cùng bộ tâm, dịch chuyển bằng `0`, dừng.

### Độ phức tạp và nhận xét

Mỗi vòng tốn `O(NKd)` phép toán gán điểm, phân tán giữa các mapper; reducer cần `O(Nd)` phép cộng trước tối ưu combiner. Broadcast cần `O(Kd)` dữ liệu mỗi worker; shuffle tối đa `O(Nd)`, có thể giảm tới khoảng `O(PKd)` khi mỗi mapper trong `P` mapper gộp đủ từng cụm. Driver chỉ thu `K` tâm, không kéo cả `N` điểm về bộ nhớ.
K-Means hội tụ về nghiệm cục bộ, nhạy với tâm khởi tạo và thang đo. Cần chuẩn hóa đặc trưng, thử nhiều seed và đánh giá Silhouette/SSE; giữ tâm cũ ở cụm rỗng là một chính sách đơn giản, có thể thay bằng tái khởi tạo điểm xa nhất.

## Chạy ví dụ

Từ thư mục gốc dự án:

```bash
python loi_giai/mapreduce.py
python -m unittest discover -s tests -v
```

`mapreduce.py` mô phỏng Map/Shuffle/Reduce cục bộ để minh họa và kiểm tra thuật toán. Khi triển khai Hadoop thật, thay lớp gom khóa trong bộ nhớ bằng shuffle của Hadoop và distributed cache như giả mã trên.
