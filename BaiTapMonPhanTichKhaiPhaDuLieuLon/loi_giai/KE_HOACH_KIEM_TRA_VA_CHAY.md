# Kiểm tra lời giải và kế hoạch chạy code

## Hoàn tất bổ sung lời giải — 30/10/2026

- Ba notebook đã chạy đầy đủ trong ngày 29/10: Lab1 **27/27 ô mã**, Lab2 **26/26**, Lab4 **20/20**; không có output lỗi. Đã lưu bảng kết quả và hình trong notebook.
- **13 kiểm thử qua**, gồm kiểm tra bổ sung Scatter-Gather phải chuyển tiếp gần đủ tổng tiền nhận và giữ cùng tiền tệ.
- MapReduce bổ sung dữ liệu trung gian, chứng minh tính đúng và kiểm tra hàm mục tiêu K-Means. Lab1 bổ sung kiểm tra mean/median/P90, khóa nối, tổng pivot và benchmark hai nguồn Parquet.
- Lab2 bổ sung baseline lớp phổ biến, chỉ số từng lớp, kiểm tra thang đo KMeans và so nhóm đặc trưng trên cùng validation.
- Lab4 bổ sung độ nhạy fan-in, ví dụ cảnh báo, bảng ngưỡng validation, feature importance và kịch bản chi phí. Ngưỡng mới là **0,8** cho cơ sở, **0,9** cho đồ thị; bảng tóm tắt đã cập nhật theo lượt chạy này.
- Log hiện tại: `.work/upgrade-tests.log`, `.work/upgrade-sql-final.log`, `.work/upgrade-ml.log`, `.work/upgrade-aml.log`. Chi tiết số liệu mới nằm trong README và các notebook.

Các phần từ mục cập nhật **28/10/2026** trở xuống là lịch sử kiểm tra, gồm những nhận xét và kế hoạch đã được xử lý; không dùng các ô chưa đánh dấu hoặc số liệu cũ ở đó làm trạng thái hiện tại.

---

## Cập nhật hoàn tất Lab4 — 28/10/2026

Các nhận xét P1 và trạng thái Lab4 ở lượt kiểm tra cũ bên dưới đã được xử lý:

- Đã chạy toàn bộ 5,078,345 giao dịch, đủ 16/16 ô mã, gồm Scatter-Gather; notebook hoàn thành trong 467,9 giây.
- Đã thêm `aml_model.py`, lưu/nạp pipeline, snapshot, ngưỡng validation 0,8, mốc thời gian và hash dữ liệu.
- Kiểm tra 31 giao dịch trước/sau lưu-nạp, gồm tài khoản mới: không sai khác về xác suất hoặc nhãn.
- Toàn bộ 12 kiểm thử qua, gồm kiểm thử ngưỡng không mặc định, đúng biên `>=`, mô hình cơ sở và mô hình đồ thị.
- Mẫu và toàn bộ có thư mục mô hình riêng. Đã cập nhật tóm tắt theo kết quả đầy đủ; accuracy tăng nhưng PR-AUC/recall giảm.
- Môi trường lượt hoàn tất: Python 3.12.16, Java 21, các thư viện theo `requirements.txt`.
- Notebook: `loi_giai/04_Lab4_GianLan_NguyenHoangVuThan.ipynb`; mô hình: `loi_giai/models/aml_full/`; log: `.work/aml-full-run.log`, `.work/tests-lab4.log`.

Phần còn lại lưu bằng chứng và kế hoạch của **lượt kiểm tra trước khi sửa**; các mục chưa đánh dấu ở đó không mô tả trạng thái Lab4 hiện tại.

---

Ngày kiểm tra: **28/10/2026**. Mã nguồn tại commit `0ea0092`.

**Cập nhật sau khi sửa lỗi `JAVA_GATEWAY_EXITED`:** đã cài JDK Temurin 21.0.13 cục bộ tại `.work/java-21/`, xác minh SHA-256, thêm `loi_giai/kich_hoat_moi_truong.sh` và cấu hình kernel `labs` dùng đúng Java/Python. Đã chạy lại **10/10 kiểm thử qua** (18,155 giây). Các mô tả thiếu môi trường và test lỗi import bên dưới ghi lại trạng thái ở lượt kiểm tra ban đầu; chưa chạy lại các notebook phân tích đầy đủ. Lỗi lưu ngưỡng Lab4 vẫn là việc riêng cần xử lý.

## 1. Kết luận và phạm vi

**Cần chỉnh phần lưu/nạp Lab4 và chuẩn bị môi trường trước khi xác nhận chạy lại được.** Đã đối chiếu đủ hai bài MapReduce, năm bài Lab1 (bài 5 có hai phần), chín bài Lab2 và bốn bài Lab4 với đề gốc. Các lời giải đều có phần trả lời tương ứng.

Đã kiểm tra mã, output lưu trong notebook và bảy hình PNG nhúng. Đã chạy ví dụ MapReduce, bốn kiểm thử MapReduce và phân tích cú pháp toàn bộ 60 ô code. Chưa chạy lại Spark, huấn luyện hay tải dữ liệu. Những số được trích từ notebook bên dưới là kết quả cũ; phép tính lại từ bảng kết quả chỉ xác nhận tính nhất quán, không thay thế chạy lại từ dữ liệu gốc.

Tài liệu này bổ sung nhận xét và kế hoạch; không sửa các lời giải hay ghi đè notebook hiện có.

## 2. Các vấn đề theo thứ tự ưu tiên

### P1 — Ngưỡng suy luận Lab4 chưa được lưu cùng mô hình

Vị trí: `04_Lab4_GianLan_NguyenHoangVuThan.ipynb`, ô code có `def class_metrics` và vòng lặp huấn luyện (cell index 32, đánh số từ 0).

`best_threshold` chỉ được dùng khi tính metric. Lệnh lưu cuối ô chỉ ghi `aml_rf_history_pipeline` và `aml_history_snapshot`, không lưu ngưỡng hay cấu hình ngưỡng vào classifier. Theo hướng dẫn hiện tại, nạp pipeline rồi `transform` sẽ lấy nhãn mặc định của classifier, không tái tạo quyết định theo ngưỡng validation.

Bằng chứng ngay trong output đã lưu: với mô hình đồ thị, accuracy ở ngưỡng chọn **0,7** là **0,9807276303**, còn `accuracy_at_0_5` là **0,9450835792**.

**Đề xuất sửa trước khi sử dụng mô hình đã lưu:** lưu metadata gồm ngưỡng đã chọn cho mô hình tương ứng, mốc cắt lịch sử, danh sách đặc trưng đồ thị, tỷ lệ mẫu và vị trí snapshot. Khi suy luận, nạp đúng snapshot, ghép lịch sử, lấy `probability[1]`, rồi áp dụng `score >= threshold`. Thêm một kiểm tra có ý nghĩa: xác suất và nhãn theo ngưỡng phải khớp trước/sau lưu-nạp trên cùng dữ liệu, gồm cả tài khoản chưa có lịch sử. Hiện chưa xác nhận sửa vì máy chưa có môi trường Spark để thực thi kiểm tra này.

### Điều kiện môi trường — README không mô tả checkout hiện tại

- Không có `.venv-labs`, `nyc_data/`, `aml_data/` hay các mô hình đã lưu ở checkout này.
- Máy có Python **3.14.4**, Java **25.0.4.1**; chưa có `python3.12`, PySpark, GraphFrames, nbformat và các thư viện trong requirements.
- Spark 4.0.2 công bố hỗ trợ Java **17/21**. Chọn Java 21 và Python 3.12 để khớp môi trường lời giải; chưa kết luận Java 25 chắc chắn lỗi vì chưa thử chạy. [Tài liệu Spark 4.0.2](https://spark.apache.org/docs/4.0.2/).
- Đã xác minh cả 10 phiên bản ghim trong `requirements.txt` tồn tại trên PyPI. Chưa cài hay kiểm tra giải phụ thuộc hoàn chỉnh. Gói JVM đang dùng, `io.graphframes:graphframes-spark4_2.13:0.12.3`, đúng nhánh Spark 4/Scala 2.13 theo [hướng dẫn GraphFrames](https://graphframes.io/02-quick-start/01-installation.html).

**Đường dẫn:** các lệnh `loi_giai/...` trong README giả định đang ở `BaiTapMonPhanTichKhaiPhaDuLieuLon/`, không phải thư mục gốc Git `BaiTapPhanTichDuLieuLon/`. Các lệnh dưới đây luôn `cd` rõ ràng.

### P2 — Những điểm cần giữ rõ khi chạy và diễn giải

1. **Biên giờ Lab1:** code tính cao điểm gồm cả giờ 9 và 19; thấp điểm gồm cả giờ 4. Nghĩa là nhận cả 09:59, 19:59 và 04:59. Đề “7h–9h, 17h–19h, 22h–4h” có thể được hiểu khác. Lời giải đã công bố quy ước; giữ quy ước đó khi tái lập, hoặc thống nhất khoảng nửa mở trước khi thay code và cập nhật kết quả.
2. **Bộ nhớ Lab1:** `slice(sort_array(collect_list(...)), 1, 10)` chỉ cắt sau khi thu toàn bộ nhóm, không giới hạn bộ nhớ ở 10 chuyến. Nếu cần giảm bộ nhớ, xếp hạng/lọc 10 hàng mỗi quận trước khi gom mảng và tính tổng số chuyến riêng. Chưa quan sát lỗi thiếu bộ nhớ ở máy này.
3. **Độ bền đọc AML:** `cast`/`to_timestamp` đứng trước `dropna`. Nếu đầu vào khác bản chuẩn có chuỗi sai kiểu, chế độ ANSI có thể ném lỗi trước khi loại null. Nên kiểm tra đầu vào hoặc dùng phép chuyển đổi chịu lỗi khi mở rộng sang dữ liệu khác. Chưa có bằng chứng bản IBM chuẩn bị lỗi này.
4. **Kết quả tóm tắt viết tay:** đầu notebook ghi các số của lần chạy cũ. Sau khi đổi tỷ lệ mẫu/chạy lại, cần cập nhật hoặc ghi rõ đây là kết quả lịch sử; output mới không tự cập nhật markdown.
5. **Ghi đè mô hình:** mẫu 2% và bản đầy đủ đều ghi cùng đường dẫn `loi_giai/models/`. Lưu trữ mô hình của từng giai đoạn riêng trước khi chuyển giai đoạn. Đổi `--output-dir` chỉ bảo vệ notebook kết quả, không đổi đường dẫn mô hình/dữ liệu bên trong code.

## 3. Đánh giá từng lời giải

| Phần | Những gì đã xác nhận | Giới hạn / việc còn lại |
|---|---|---|
| MapReduce | Khử sản phẩm trùng trong giỏ, cặp không thứ tự, combiner cộng tổng; K-Means gán tâm gần nhất, phá hòa, giữ tâm rỗng, kiểm tra hội tụ và giới hạn vòng. Ví dụ và 4 test chạy qua. | Đây là mô phỏng cục bộ dùng bộ nhớ, không phải thực thi Hadoop phân tán. |
| Lab1 | Đủ các truy vấn EDA, thời gian ngược trên dữ liệu thô, pivot, top 3 ngày, top 5 tuyến, benchmark có action và `PartitionFilters` trong output. | Chưa chạy lại trên dữ liệu gốc. Mức tăng tốc cũ khoảng 69,62 lần gồm cả tính lại pipeline/định dạng/cache, không đo riêng tác dụng pruning. |
| Lab2 | Chia train/test trước fit scaler; cùng test khi so sánh; không đưa tip/total_amount vào feature; regularization chọn trên validation; AUC dùng xác suất lớp 1. | Chưa chạy lại test Spark/AUC hay huấn luyện. Baseline luôn đoán lớp 1 trên test cũ đạt khoảng 75,10%; accuracy mô hình khoảng 75,5% cần được đọc cùng AUC. |
| Lab4 | ID là cặp ngân hàng+tài khoản; phân biệt hai cột Account theo vị trí; fan-in đếm nguồn phân biệt; mule có điều kiện dưới 2 giờ; layering kiểm tra phí 1–5% và tiền tệ. Snapshot ML chỉ lấy quá khứ. | Cần sửa lưu ngưỡng. Các kết quả cũ chỉ từ mẫu 2%, test có 30 nhãn dương; chưa có bằng chứng notebook chạy hết dữ liệu IBM đầy đủ. |

Các phép tính độc lập đã khớp output: số dòng bị loại Lab1 (**465.266**), tỷ số median benchmark, tổng nhãn Lab2 (**4.523.472**) và accuracy từ confusion matrix DT; accuracy/precision/recall/F1 của cả hai mô hình AML từ TP/FP/FN/TN. ROC-AUC, PR-AUC, Silhouette và số dòng gốc chưa được tính lại độc lập từ dữ liệu.

Kết luận AML hiện có là phù hợp: thêm đồ thị làm accuracy/F1 tăng nhẹ nhưng PR-AUC và recall giảm. Không được viết “đồ thị tốt hơn toàn diện”.

### Chất lượng và độ đầy đủ của lời giải

Các mẫu số là số thành phần trong phạm vi kiểm tra; `0 / N` nghĩa là chưa quan sát lỗi, không có nghĩa toàn bộ N thành phần đã được xác minh bằng chạy lại. Các nhóm có thể chồng lặp, không cộng thành tỷ lệ chính xác chung.

| Hạng mục | Lỗi quan sát / tổng | Đánh giá |
|---|---|---|
| Đủ yêu cầu bài tập | 0 / 4 | Đã đối chiếu bốn bộ lời giải với PDF và ba notebook đề gốc. |
| Trình bày và hướng dẫn dùng | 1 / 4 | Lab4 thiếu ngưỡng trong quy trình lưu/nạp; README còn giả định môi trường có sẵn. |
| Hình và tương tác | 0 / 7 | Đã mở bảy PNG nhúng: hình/nhãn đọc được. Không có dashboard hay tương tác cần kiểm tra. |

### Tính đúng và khả năng kiểm chứng

| Hạng mục | Lỗi quan sát / tổng | Đánh giá |
|---|---|---|
| Nguồn và phiên bản dữ liệu | 0 / 3 | Có nguồn cho ba lab; dữ liệu thô chưa hiện diện để đối chiếu nội dung/hash. |
| Tính toán và thực thi | 1 / 4 | Bốn nhóm lời giải đã đọc mã; Lab4 có lệch ngưỡng giữa đánh giá và suy luận. Mới chạy lại MapReduce. |
| Giá trị trên hình so với output | 0 / 7 | Các hình đã mở và đối chiếu bảng/logic liên quan; chưa tái tính dữ liệu gốc của từng điểm. |
| Chi tiết nguồn tương tác | N/A | Notebook không có giao diện xem nguồn tương tác. |
| Nhất quán giữa các tài liệu | 0 / 3 | Số liệu ba notebook khớp phần tổng kết README; đó là bằng chứng lịch sử. |
| Chất lượng dữ liệu đầu vào | 0 / 3 | Đã đọc bộ lọc/khóa ghép; chưa đo null, trùng, phân phối trên bản tải mới. |
| Kết luận có căn cứ | 0 / 4 | Kết luận chính có nêu giới hạn; không thấy suy diễn AML tốt hơn toàn diện hay tốc độ chỉ do pruning. |

## 4. Kế hoạch chạy theo giai đoạn

Chạy **tuần tự**, mỗi lần một Spark session. Máy có khoảng 15 GiB RAM tổng và 9,2 GiB khả dụng khi kiểm tra; các notebook đang cấu hình `local[2]`, driver 2–3 GiB. Đây là điểm bắt đầu, chưa bảo đảm đủ cho mọi motif trên AML đầy đủ. Dành thêm dung lượng cho Parquet, mô hình, checkpoint và log; không ước lượng thời gian chạy toàn bộ khi chưa có phép đo trên máy này.

### Giai đoạn A — Chuẩn bị môi trường

Các lệnh là kế hoạch, chưa được thực thi trong lượt kiểm tra này. Từ thư mục gốc Git:

```bash
cd /home/kent/KentProject/BaiTapPhanTichDuLieuLon/BaiTapMonPhanTichKhaiPhaDuLieuLon

# Cần cài JDK 21 thật, không chỉ export đường dẫn dự kiến.
# Ubuntu: sudo apt-get install openjdk-21-jdk-headless
# Hoặc giải nén JDK 21 vào .work/java-21/ sao cho có bin/java.
# Checkout này đã có JDK Temurin 21 cục bộ sau lượt sửa lỗi Java.

# Máy đã có uv; tải Python 3.12 và tạo môi trường riêng.
uv python install 3.12
uv venv --python 3.12 --seed .venv-labs
uv pip install --python .venv-labs/bin/python -r loi_giai/requirements.txt
.venv-labs/bin/python -m pip check
source loi_giai/kich_hoat_moi_truong.sh
java -version
.venv-labs/bin/python -m ipykernel install --user --name labs --display-name 'Python (labs)' \
  --env JAVA_HOME "$JAVA_HOME" --env PYSPARK_PYTHON "$PYSPARK_PYTHON" \
  --env SPARK_LOCAL_IP "$SPARK_LOCAL_IP"
.venv-labs/bin/jupyter kernelspec list
```

Chỉ tiếp tục khi Java báo 21, kernel `labs` trỏ đúng Python của môi trường và cài đặt không lỗi. Cần mạng tới PyPI và Maven Central. Kiểm thử ở giai đoạn B sẽ kiểm chứng cả Spark gateway và gói JVM GraphFrames; cài được gói Python chưa đủ.

### Giai đoạn B — Kiểm thử nhỏ, không cần dữ liệu gốc

```bash
.venv-labs/bin/python loi_giai/mapreduce.py
.venv-labs/bin/python -m unittest discover -s tests -v
```

Điều kiện qua: **10 test** (4 MapReduce, 5 AML, 1 AUC) đều qua, không có lỗi import/JVM/Maven. Test AML tự tạo CSV nhỏ để kiểm tra định danh và các biên tiền/thời gian. Sau đó thực hiện sửa ngưỡng Lab4 và kiểm tra lưu-nạp nêu ở mục 2; nếu thêm test mới, tổng số test sẽ tăng.

### Giai đoạn C — Tải và xác minh dữ liệu

```bash
.venv-labs/bin/python loi_giai/tai_du_lieu.py
```

Script tải cả NYC và AML. Kiểm tra:

- Hai Parquet NYC tháng 01, 02/2023 mở được, bảng zone có `LocationID` duy nhất để tránh nhân dòng khi join.
- `aml_data/HI-Small_Trans.csv` thực sự là CSV có 11 cột, bao gồm hai vị trí Account; không nhầm trang HTML hoặc file ZIP thành CSV. Header được notebook kiểm tra nhưng downloader chưa kiểm tra nội dung.
- README ghi kích thước AML bản Kaggle 8 là **475.664.283 bytes**; dùng làm mốc đối chiếu, chưa xác minh bản tải mới. Ghi lại kích thước và SHA-256 các file để tái lập.
- Downloader bỏ qua mọi file đã tồn tại, nên cần kiểm tra file cũ trước khi tin thông báo “Đã có”. Nếu Kaggle yêu cầu đăng nhập, tải đúng file/version qua tài khoản rồi đặt vào đường dẫn trên hoặc đặt `AML_PATH` tuyệt đối.

### Giai đoạn D — Chạy Lab1 để tạo đầu vào cho Lab2

Lưu notebook đã chạy sang thư mục riêng; không dùng `--inplace` trong lượt thử.

```bash
RUN_ID=$(date +%Y%m%d-%H%M%S)
RUN_DIR="$PWD/.work/runs/$RUN_ID"
mkdir -p "$RUN_DIR/lab1" "$RUN_DIR/ml-sample" "$RUN_DIR/aml-sample" "$RUN_DIR/full"

.venv-labs/bin/jupyter nbconvert --execute --to notebook \
  --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 \
  --output-dir="$RUN_DIR/lab1" loi_giai/02_Lab1_SparkSQL_NguyenHoangVuThan.ipynb \
  >"$RUN_DIR/lab1.log" 2>&1
```

`timeout=1800` giới hạn **mỗi cell**, không phải toàn notebook. `RUN_DIR` cần được giữ trong cùng shell cho các bước sau. Notebook nguồn nằm trong `loi_giai/`; nbconvert khởi chạy tại đó và cell thiết lập ROOT chuyển về thư mục bài tập. Trong IDE cũng nên đặt working directory là thư mục bài tập hoặc `loi_giai/`, vì ROOT hiện chỉ xử lý hai trường hợp này.

Điều kiện qua:

- Tất cả cell hoàn thành, tạo `nyc_data/yellow_tripdata_2023-01-02-parquet/` và đọc lại được.
- Với đúng dữ liệu/phiên bản cũ, đối chiếu mốc **5.980.721** dòng thô, **5.515.455** dòng sạch, **279** lỗi thời gian, **10.585,13 USD** tổng có dấu. Chênh lệch phải được giải thích bằng đầu vào/kiểu dữ liệu/lọc/quantile; không sửa số để ép khớp.
- Mỗi quận có tối đa 3 ngày top; tuyến đủ 5.000 chuyến trước chọn top 5; pivot chỉ có thanh toán 1/2. Rà các trường hợp null/khóa zone không khớp.
- Benchmark hai nguồn trả cùng count ngày 15/01; kế hoạch đọc có `PartitionFilters`. Không yêu cầu tái lập tỷ số tốc độ cũ.
- Mở biểu đồ sau khi chạy; không dùng thành công của lệnh như bằng chứng hình hiển thị đúng.

Lab1 chưa có biến tỷ lệ mẫu. Không thêm mẫu ngẫu nhiên trực tiếp nếu muốn giữ điều kiện tuyến >= 5.000 và các mốc kiểm tra đầy đủ.

### Giai đoạn E — Chạy thử Lab2 và Lab4 trên mẫu

```bash
ML_FRACTION=0.02 .venv-labs/bin/jupyter nbconvert --execute --to notebook \
  --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 \
  --output-dir="$RUN_DIR/ml-sample" loi_giai/03_Lab2_ML_NguyenHoangVuThan.ipynb \
  >"$RUN_DIR/ml-sample.log" 2>&1

AML_FRACTION=0.02 .venv-labs/bin/jupyter nbconvert --execute --to notebook \
  --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 \
  --output-dir="$RUN_DIR/aml-sample" loi_giai/04_Lab4_GianLan_NguyenHoangVuThan.ipynb \
  >"$RUN_DIR/aml-sample.log" 2>&1
```

Điều kiện qua Lab2: scaler/indexer chỉ học từ train tương ứng; các mô hình so trên cùng test; đủ hai lớp; có Silhouette k=3/4, hệ số LR, confusion matrix, mô hình lưu/nạp và dự đoán hai chuyến giả định. Kết quả mẫu không bắt buộc khớp số của bản đầy đủ.

Điều kiện qua Lab4: đọc CSV đúng hai đầu; có kết quả/không có kết quả motif đều được nếu tính đúng; mỗi tập train/validation/test có đủ hai lớp; mốc lịch sử nằm trước train. Báo cáo confusion matrix, ROC/PR-AUC, precision/recall/F1, baseline luôn đoán bình thường và ngưỡng đã chọn. Nếu mẫu thiếu nhãn dương, tăng tỷ lệ rồi chạy lại, không bỏ điều kiện kiểm tra hai lớp.

Sau lượt mẫu, chép `loi_giai/models/` sang `$RUN_DIR/models-sample/` để giữ mô hình và snapshot cùng nhau. Theo dõi thời gian từng cell, RAM và kích thước checkpoint trước khi tăng dữ liệu. `sample` giảm số cạnh sau đọc; CSV vẫn cần được quét, không giảm tương ứng chi phí tải/đọc file gốc.

### Giai đoạn F — Chạy đầy đủ và chốt kết quả

Chỉ chuyển sang đây khi các bước trước qua. Với AML, tăng thử lên 10% trước 100% nếu thời gian/RAM của motif 2% đã đáng kể. Không suy ra chi phí motif tăng tuyến tính theo số cạnh; các phép ghép nhiều cạnh có thể sinh nhiều hàng trung gian, kể cả khi cuối cùng chỉ `.show(10)`.

```bash
ML_FRACTION=1.0 .venv-labs/bin/jupyter nbconvert --execute --to notebook \
  --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 \
  --output-dir="$RUN_DIR/full" loi_giai/03_Lab2_ML_NguyenHoangVuThan.ipynb \
  >"$RUN_DIR/ml-full.log" 2>&1

AML_FRACTION=1.0 .venv-labs/bin/jupyter nbconvert --execute --to notebook \
  --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 \
  --output-dir="$RUN_DIR/full" loi_giai/04_Lab4_GianLan_NguyenHoangVuThan.ipynb \
  >"$RUN_DIR/aml-full.log" 2>&1
```

Giữ mỗi lệnh là một bước độc lập, kiểm tra exit code và log trước lệnh kế tiếp. Khi thiếu bộ nhớ hoặc timeout, xác định cell/kế hoạch gây lỗi rồi tối ưu/tăng tài nguyên; ghi nhận “chưa chạy xong” thay vì dùng output mẫu cho tuyên bố chạy đầy đủ.

Hồ sơ hoàn tất gồm notebook đã chạy, log, phiên bản môi trường, hash dữ liệu, số dòng sau lọc/chia tập, tỷ lệ mẫu, mô hình + metadata ngưỡng + snapshot, và kết quả kiểm tra lưu-nạp. Lưu mô hình đầy đủ sang `$RUN_DIR/models-full/`, cập nhật phần tóm tắt viết tay theo kết quả mới và ghi rõ phạm vi AML thực sự hoàn thành. Chưa coi việc chạy đủ dữ liệu là bằng chứng mô hình tốt hơn; kết luận vẫn phải dựa trên các chỉ số đã báo cáo.

## 5. Trạng thái sau lượt kiểm tra này

- [x] Đối chiếu toàn bộ yêu cầu với lời giải.
- [x] Chạy ví dụ và 4 test MapReduce; kiểm tra cú pháp 60 cell; mở 7 PNG.
- [x] Tính lại các phép cộng/tỷ lệ và confusion matrix trọng yếu từ output đã lưu.
- [x] Thử chạy toàn bộ suite: 4 test MapReduce qua; 5 test AML chưa chạy do lỗi `setUpClass`, 1 test AUC lỗi import `nbformat`. Không phải lỗi thuật toán được tái hiện.
- [x] Tạo môi trường Python 3.12/Java 21 và chạy lại đủ 10 test (cập nhật sau sửa lỗi Java).
- [ ] Sửa, kiểm chứng lưu/nạp ngưỡng Lab4.
- [ ] Tải/xác minh dữ liệu; chạy tuần tự các giai đoạn D–F.
