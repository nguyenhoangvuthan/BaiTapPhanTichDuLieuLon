# Lời giải toàn bộ 4 tài liệu

Đọc và chạy các bài theo thứ tự sau. Bốn đề gốc tại thư mục gốc được giữ nguyên.

| Tài liệu | Lời giải | Phạm vi |
|---|---|---|
| Thuật toán MapReduce | [01_MapReduce_NguyenHoangVuThan.ipynb](01_MapReduce_NguyenHoangVuThan.ipynb) | Hai bài: cặp sản phẩm và K-Means lặp; Map/Combine/Shuffle/Reduce, Driver và ví dụ đã chạy có đối chiếu kết quả |
| Lab1 Spark SQL | [02_Lab1_SparkSQL_NguyenHoangVuThan.ipynb](02_Lab1_SparkSQL_NguyenHoangVuThan.ipynb) | Đủ 5 bài, gồm cả tuyến đường và đo partition pruning |
| Lab2 ML | [03_Lab2_ML_NguyenHoangVuThan.ipynb](03_Lab2_ML_NguyenHoangVuThan.ipynb) | Đủ 9 bài: KMeans, LR, DT, Pipeline, lưu/nạp, suy luận và cải tiến |
| Lab4 gian lận | [04_Lab4_GianLan_NguyenHoangVuThan.ipynb](04_Lab4_GianLan_NguyenHoangVuThan.ipynb) | Đủ 4 bài: fan-in, mule, layering, so sánh ML có/không có đồ thị |

Mỗi lời giải notebook đặt ngay sau yêu cầu tương ứng. Mở notebook để xem cả mã, bảng kết quả, biểu đồ và giải thích.

Các phần phân tích bổ sung:

- **MapReduce:** theo dõi dữ liệu qua từng mapper/combiner, chứng minh phép đếm bằng biến chỉ báo và kiểm tra hàm mục tiêu K-Means giảm sau cập nhật.
- **SparkSQL:** đối chiếu mean/median/P90 và ngày thường/cuối tuần cho 5 tuyến; kiểm tra khóa nối, tổng pivot; benchmark thêm hai nguồn Parquet với cùng phép đếm và tổng tiền.
- **ML:** so với baseline lớp phổ biến, báo cáo recall/F1 của cả hai lớp và balanced accuracy; kiểm tra thang đo KMeans, so từng nhóm thay đổi đặc trưng trên cùng validation.
- **Gian lận:** kiểm tra bảo toàn tiền và tiền tệ cho Scatter-Gather; phân tích độ nhạy ngưỡng fan-in, đọc ví dụ motif, bảng ngưỡng validation, feature importance và kịch bản chi phí cảnh báo sai/bỏ sót.

## Môi trường

### Chạy trên máy Windows qua Ubuntu/WSL

Kiểm tra ngày 28/10/2026: Java 27 trên Windows lỗi `jdk.internal.ref.Cleaner`
khi khởi tạo Spark 4.0.2; Windows cũng chưa có Hadoop native (`winutils.exe`).
Dùng Ubuntu/WSL với Java 21 để chạy cả đọc/ghi Parquet và lưu mô hình.
Môi trường WSL tách biệt với Python Windows: cài `pyspark` trên Windows không
cài thư viện vào WSL và ngược lại.

Thiết lập một lần trong Ubuntu (mở terminal tại thư mục bài tập):

```bash
sudo apt-get update
sudo apt-get install -y openjdk-21-jdk-headless python3-venv
python3 -m venv ~/.venvs/bigdata-labs
~/.venvs/bigdata-labs/bin/python -m pip install -r loi_giai/requirements.txt
source loi_giai/kich_hoat_moi_truong.sh
python loi_giai/tai_du_lieu.py
```

Môi trường WSL của máy hiện dùng Python 3.14.4, Java 21 và các phiên bản thư viện
trong `requirements.txt`. Script kích hoạt tìm môi trường này nếu không có
`.venv-labs/bin/python`; có thể chỉ định môi trường khác bằng `LABS_VENV`.
Script cũng đặt `SPARK_LOCAL_DIRS=~/.cache/bigdata-labs/spark` để shuffle nằm
trên ổ đĩa. `/tmp` của Ubuntu hiện là tmpfs chỉ 3,8 GiB: bài AML toàn bộ dữ liệu
đã gặp `No space left on device` khi dùng mặc định này. Có thể đặt
`SPARK_LOCAL_DIRS` sang thư mục trên ổ đĩa khác còn dung lượng.

Chạy từ thư mục gốc Git trong PowerShell:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\BaiTapMonPhanTichKhaiPhaDuLieuLon\loi_giai\chay_wsl.ps1
```

Hoặc trong Ubuntu, từ thư mục bài tập:

```bash
source loi_giai/kich_hoat_moi_truong.sh
python loi_giai/chay_bai_tap.py
# Chạy riêng sau khi đã kiểm thử:
python loi_giai/chay_bai_tap.py --only ml --skip-tests
```

Runner chạy MapReduce, bộ kiểm thử và ba notebook theo thứ tự. Mỗi notebook dùng
kernel mới từ chính interpreter đã kích hoạt; chỉ ghi đè notebook sau khi chạy
thành công. Khi lỗi, bản thực thi dở được lưu ở `.work/runs/*.failed.ipynb`;
log kiểm thử và trạng thái từng lần chạy cũng ở `.work/runs/`.

Để chạy từng cell trong VS Code, chọn **WSL: Reopen Folder in WSL**, chọn
interpreter `~/.venvs/bigdata-labs/bin/python`, rồi kernel **Python (labs)**.
Kernel này được runner đăng ký cùng `JAVA_HOME`, Python worker và thư mục shuffle.
Không chọn kernel Python Windows cho phiên chạy WSL.

### Thiết lập Linux với Python 3.12

Python **3.12**, Java **21**, PySpark **4.0.2**, GraphFrames **0.12.3**. Các phiên bản Python được ghi trong [requirements.txt](requirements.txt).
Spark và GraphFrames cần Java và tải các thư viện Maven khi khởi tạo lần đầu. Không dùng gói GraphFrames 0.8.3 cho Spark 4.

Từ thư mục `BaiTapMonPhanTichKhaiPhaDuLieuLon/` (bên trong thư mục gốc Git), tạo môi trường nếu chưa có:

```bash
python3.12 -m venv .venv-labs
.venv-labs/bin/python -m pip install -r loi_giai/requirements.txt
```

Cần cài Java trước khi chạy Spark. Trên Ubuntu có thể dùng `sudo apt-get install openjdk-21-jdk-headless`; hoặc giải nén JDK 21 vào `.work/java-21/` sao cho có `.work/java-21/bin/java`.
Trong mỗi terminal chạy bài, kích hoạt bằng:

```bash
source loi_giai/kich_hoat_moi_truong.sh
java -version
.venv-labs/bin/python -m ipykernel install --user --name labs --display-name 'Python (labs)' \
  --env JAVA_HOME "$JAVA_HOME" --env PYSPARK_PYTHON "$PYSPARK_PYTHON" \
  --env SPARK_LOCAL_IP "$SPARK_LOCAL_IP"
```

Script tìm Java 17/21 đã cài, bỏ qua `JAVA_HOME` không hợp lệ và chọn Python của `.venv-labs`. Không đặt `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64` nếu thư mục đó chưa tồn tại. JDK giải nén tại `.work/java-21/` là file cục bộ, không nằm trong Git; checkout mới cần cài Java hoặc tự đặt JDK vào đây. Lệnh đăng ký kernel ở trên lưu cả cấu hình Java/Python, dùng được khi chọn kernel `labs` trong IDE. Kernel đang chạy cần được khởi động lại sau khi đổi cấu hình.

Khi mở thư mục gốc Git `BaiTapPhanTichDuLieuLon/` trong VS Code, `.vscode/settings.json` cấu hình interpreter và tìm môi trường `.venv-labs` trong thư mục bài tập. Nếu IDE còn giữ `/bin/python` hoặc hiện `Unable to handle /bin/python`, chạy **Python: Select Interpreter** → **Enter interpreter path…** và chọn `BaiTapMonPhanTichKhaiPhaDuLieuLon/.venv-labs/bin/python` bên trong thư mục gốc Git, rồi chạy **Developer: Reload Window**. Với notebook chọn riêng kernel **Python (labs)**. Không thay symlink Python hệ thống để sửa lỗi chọn môi trường của IDE.

Trên Windows dùng `.venv-labs\Scripts\python.exe` thay cho `.venv-labs/bin/python`, và chọn kernel `labs` trong VS Code/Jupyter.
Các notebook tự đặt Python worker bằng Python của kernel để tránh trộn Python 3.12 và 3.14.

## Dữ liệu và thứ tự chạy

Chạy từ thư mục gốc; script tải bỏ qua tệp đã tồn tại:

```bash
.venv-labs/bin/python loi_giai/tai_du_lieu.py
.venv-labs/bin/jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.kernel_name=labs loi_giai/01_MapReduce_NguyenHoangVuThan.ipynb
.venv-labs/bin/jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 loi_giai/02_Lab1_SparkSQL_NguyenHoangVuThan.ipynb
.venv-labs/bin/jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 loi_giai/03_Lab2_ML_NguyenHoangVuThan.ipynb
.venv-labs/bin/jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 loi_giai/04_Lab4_GianLan_NguyenHoangVuThan.ipynb
```

Lab1 tạo dữ liệu sạch tại `nyc_data/yellow_tripdata_2023-01-02-parquet`; Lab2 đọc chính thư mục này.
Lab4 đọc `aml_data/HI-Small_Trans.csv`. Có thể đặt biến môi trường `AML_PATH` để đổi đường dẫn.
Notebook hỗ trợ chạy khi thư mục làm việc là thư mục gốc hoặc `loi_giai/`.

Notebook mặc định chạy **toàn bộ** dữ liệu (`ML_FRACTION=1.0`, `AML_FRACTION=1.0`). Chạy nhanh với mẫu tái lập bằng seed 42, ví dụ trên Bash:

```bash
ML_FRACTION=0.02 .venv-labs/bin/jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 loi_giai/03_Lab2_ML_NguyenHoangVuThan.ipynb
AML_FRACTION=0.02 .venv-labs/bin/jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 loi_giai/04_Lab4_GianLan_NguyenHoangVuThan.ipynb
```

Mẫu được xác định theo seed và phân hoạch đầu vào; thay cách đọc/phiên bản Spark có thể thay mẫu. Luôn xem số dòng và tỷ lệ in trong output.

## Cách diễn giải kết quả

- Lab1: skewness và excess kurtosis lớn chỉ là dấu hiệu ngoại lệ; không chứng minh mọi chuyến dài là lỗi. Tiền gắn với bản ghi lỗi thời gian không tự động đồng nghĩa tiền không thu được.
- Benchmark Lab1 so sánh DataFrame còn tính toán làm sạch với Parquet đã ghi và phân vùng. Vì vậy tỷ số thời gian bao gồm cả chi phí tính lại pipeline, định dạng lưu trữ và cache hệ điều hành; không thể quy hết mức tăng tốc cho partition pruning. Kế hoạch `PartitionFilters` xác nhận có cắt phân vùng.
- Lab2: StandardScaler chỉ fit trên train; cải tiến one-hot, biến chu kỳ và regularization được chọn bằng validation, rồi so cùng test. Không mặc định mô hình mới tốt hơn.
- Lab4: tài khoản là cặp ngân hàng + tài khoản; hai cột CSV `Account` được đọc theo vị trí. Mọi phép so tiền giữ cùng loại tiền tệ. Motif chỉ tạo cảnh báo, không kết luận rửa tiền.
- Lab4 ML: chia theo phân vị timestamp, lấy khoảng 20% số giao dịch đầu làm snapshot lịch sử, 40% tiếp theo huấn luyện, 20% validation và 20% test. Đây là tỷ lệ số hàng, không phải tỷ lệ thời lượng. Không dùng nhãn hay cạnh tương lai cho snapshot; giao dịch cùng timestamp đi cùng tập. Các mốc xấp xỉ có thể thay đổi giữa lượt chạy, nên metadata lưu mốc cụ thể của từng bộ mô hình.
- Accuracy cao vẫn có thể không nhận ra giao dịch rửa tiền. So thêm PR-AUC, precision, recall và F1 lớp 1. Lấy mẫu cạnh làm đứt mạng và mẫu test có thể rất ít nhãn dương; không suy rộng kết quả mẫu thành chất lượng trên toàn bộ IBM AML.

## Lưu mô hình

Mô hình nằm trong `loi_giai/models/` sau khi chạy. Lab2 lưu pipeline cơ sở và pipeline cải tiến riêng.
Lab4 lưu hai bộ mô hình tại `loi_giai/models/aml_full/{baseline,with_graph}/`
khi chạy toàn bộ. Mỗi bộ có `pipeline/`, `metadata.json` chứa ngưỡng đã chọn,
mốc chia thời gian, tỷ lệ mẫu và SHA-256 dữ liệu; bộ `with_graph` có thêm
`history_snapshot/`. Chạy mẫu dùng thư mục `aml_sample_<tỷ lệ>/` riêng, ví dụ
`aml_sample_0p02/`, để không ghi đè mô hình toàn bộ.

Suy luận bằng [aml_model.py](aml_model.py), từ thư mục bài tập:

```python
from loi_giai.aml_model import load_aml_bundle

bundle = load_aml_bundle(spark, 'loi_giai/models/aml_full/with_graph')
predictions = bundle.predict(new_transactions)
predictions.select('src', 'dst', 'score', 'prediction').show()
```

`new_transactions` có schema như `edges` trong Lab4, không cần nhãn hay
`class_weight`. Helper tự ghép snapshot quá khứ, điền đặc trưng cho tài khoản
mới và đặt `prediction = (score >= threshold)` theo metadata. Không gọi riêng
`bundle.model.transform()` nếu muốn tái tạo nhãn theo ngưỡng validation.
Notebook kiểm tra xác suất và nhãn trước/sau lưu-nạp trên các giao dịch test
và một giao dịch có hai tài khoản chưa xuất hiện trong snapshot.
Notebook ghi đè đúng thư mục đầu ra của chính bài khi chạy lại.

## Kiểm tra mã

Từ thư mục gốc:

```bash
.venv-labs/bin/python -m unittest discover -s tests -v
```

Kiểm thử MapReduce gồm cặp không thứ tự, sản phẩm trùng trong giỏ, hòa khoảng cách, tâm rỗng, hội tụ và điều kiện dừng.
Kiểm thử GraphFrames dùng CSV nhỏ có đáp án xác định: định danh hai đầu, tài khoản trùng mã khác ngân hàng, nguồn fan-in phân biệt, tự chuyển, biên 2 giờ, khác tiền tệ và phí biên 1%/5%.
Kiểm thử Scatter-Gather so kết quả khi đổi thứ tự ghép cạnh, kiểm tra biên
2 giờ, cùng tiền tệ và số tiền chuyển tiếp. Kiểm thử lưu/nạp kiểm tra cả mô hình cơ sở và mô hình đồ thị, xác suất,
ngưỡng 0,9 so với mặc định 0,5, dấu `>=` và tài khoản mới.

## Nguồn

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page): Yellow Taxi tháng 01–02/2023 và taxi zone lookup.
- [IBM AML trên Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml): `HI-Small_Trans.csv`, phiên bản 8, 475.664.283 bytes. Đây là dữ liệu tổng hợp do IBM cung cấp; dùng theo giấy phép của bộ dữ liệu.
- [Kho IBM AML-Data](https://github.com/IBM/AML-Data): nguồn và mô tả bộ dữ liệu.
- [Altman và cộng sự, NeurIPS 2023](https://papers.neurips.cc/paper_files/paper/2023/file/5f38404edff6f3f642d6fa5892479c42-Paper-Datasets_and_Benchmarks.pdf): nghiên cứu gốc; HI là Higher Illicit, chỉ tỷ lệ giao dịch rửa tiền cao hơn LI.
- [Cài GraphFrames](https://graphframes.io/02-quick-start/01-installation.html): client Python và gói JVM phải tương thích Spark/Scala.
- [Spark MLlib 4.0.2](https://spark.apache.org/docs/4.0.2/ml-guide.html): Pipeline, thuật toán và đánh giá.

## Phạm vi đã thực thi — lần chạy đầy đủ ngày 29/10/2026

- MapReduce: chương trình ví dụ chạy được; kiểm thử cặp sản phẩm và K-Means có đáp án cụ thể.
- Lab1: đã chạy tuần tự trên toàn bộ **5.980.721** dòng đầu vào, tạo **5.515.455** dòng sạch. Có **279** lỗi thời gian ngược, tổng tiền **10.585,13 USD**.
- Lab2: đã chạy tuần tự trên toàn bộ dữ liệu sạch, gồm **4.523.472** chuyến thẻ hợp lệ để gán nhãn.
- Lab4: đã chạy tuần tự toàn bộ **5.078.345** giao dịch IBM gốc (`AML_FRACTION=1.0`), gồm **5.177** nhãn rửa tiền. Cả **20 ô mã** đã thực thi, bao gồm Scatter-Gather và các phân tích bổ sung. Test có **1.012.925** giao dịch và **1.792** nhãn dương. Ngưỡng đã lưu là **0,8** cho mô hình cơ sở và **0,9** cho mô hình đồ thị; kiểm tra lưu/nạp trên 31 giao dịch, gồm tài khoản mới, không có sai khác. Thời gian notebook khoảng **585,5 giây**.
- Các notebook lưu output và hình; các biểu đồ đã được mở để kiểm tra nhãn và giá trị. Bản HTML phục vụ kiểm tra nằm trong `.work/previews/`; bản Lab4 được kiểm tra lại sau lượt chạy toàn bộ.

Lab4 trên toàn bộ dữ liệu: accuracy cơ sở **96,1309%**, thêm đồ thị
**98,4064%**; ROC-AUC **0,935333 → 0,965196**,
F1 **0,060010 → 0,079599**. Tuy nhiên PR-AUC giảm
**0,062245 → 0,038593**, recall giảm **69,8103% → 38,9509%**.
Đồ thị chưa tốt hơn toàn diện. Baseline luôn đoán bình thường có accuracy
**99.8231%** nhưng không phát hiện giao dịch rửa tiền nào.
Đồ thị giảm cảnh báo sai từ **38.650** xuống **15.048**, đồng thời tăng số bỏ sót
từ **541** lên **1.094**. Với chi phí giả định `FP + c × FN`, hai mô hình có chi phí
bằng nhau khi `c ≈ 42,68`; đây là kịch bản trên các ngưỡng đã cố định, không phải
ước lượng chi phí thực tế. Số liệu lượt này thay thế bản trước; chưa đo lặp để
quy chênh lệch giữa các lượt huấn luyện cho một thay đổi cụ thể.
SHA-256 tệp IBM: `b19d39f515523373f991b689c07e11e7b0b95c17a2c27a87d91584ae16c5b040`. Kích thước và số dòng
đã được đối chiếu độc lập từ CSV.

Lab2 sau khi đánh giá bằng xác suất lớp 1: ROC-AUC LR cơ sở **0,584662**, DT depth=5 **0,589745**, DT depth=10 **0,600730**, LR cải tiến **0,608202**. PR-AUC của LR tăng **0,799646 → 0,811161**. Validation PR-AUC khi kiểm tra nhóm đặc trưng tăng **0,798679 → 0,804882 → 0,810888** (mã dạng số → one-hot → thêm log/sin/cos). Dù vậy, accuracy của LR cải tiến **75,6168%** chỉ hơn baseline **75,1035%** khoảng **0,51 điểm phần trăm**; recall lớp tip thấp chỉ **3,1236%**.

Đã chạy thành công **27/27 ô mã Lab1**, **26/26 ô mã Lab2**, **20/20 ô mã Lab4** và **13 kiểm thử**, gồm kiểm thử AUC và điều kiện tiền/tệ của Scatter-Gather. Benchmark đối chứng Lab1 cùng trả **89.240** chuyến và **2.384.969,40 USD**; median khoảng **0,059 giây** không phân vùng so với **0,054 giây** phân vùng. Chênh lệch nhỏ và các lượt đo chồng lấn, không kết luận một mức tăng tốc ổn định.
