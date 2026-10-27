# Lời giải toàn bộ 4 tài liệu

Đọc và chạy các bài theo thứ tự sau. Bốn đề gốc tại thư mục gốc được giữ nguyên.

| Tài liệu | Lời giải | Phạm vi |
|---|---|---|
| Thuật toán MapReduce | [01_MapReduce.md](01_MapReduce.md) | Hai bài: cặp sản phẩm và K-Means lặp; Map/Combine/Shuffle/Reduce, Driver, ví dụ và độ phức tạp |
| Lab1 Spark SQL | [02_Lab1_SparkSQL.ipynb](02_Lab1_SparkSQL.ipynb) | Đủ 5 bài, gồm cả tuyến đường và đo partition pruning |
| Lab2 ML | [03_Lab2_ML.ipynb](03_Lab2_ML.ipynb) | Đủ 9 bài: KMeans, LR, DT, Pipeline, lưu/nạp, suy luận và cải tiến |
| Lab4 gian lận | [04_Lab4_GianLan.ipynb](04_Lab4_GianLan.ipynb) | Đủ 4 bài: fan-in, mule, layering, so sánh ML có/không có đồ thị |

Mỗi lời giải notebook đặt ngay sau yêu cầu tương ứng. Mở notebook để xem cả mã, bảng kết quả, biểu đồ và giải thích.

## Môi trường

Python **3.12**, Java **21**, PySpark **4.0.2**, GraphFrames **0.12.3**. Các phiên bản Python được ghi trong [requirements.txt](requirements.txt).
Spark và GraphFrames cần Java và tải các thư viện Maven khi khởi tạo lần đầu. Không dùng gói GraphFrames 0.8.3 cho Spark 4.

Môi trường `.venv-labs` đã được cài sẵn trong dự án hiện tại. Để tạo lại ở máy khác, từ thư mục gốc:

```bash
python3.12 -m venv .venv-labs
.venv-labs/bin/python -m pip install -r loi_giai/requirements.txt
.venv-labs/bin/python -m ipykernel install --user --name labs --display-name 'Python (labs)'
```

Trên Windows dùng `.venv-labs\Scripts\python.exe` thay cho `.venv-labs/bin/python`, và chọn kernel `labs` trong VS Code/Jupyter.
Các notebook tự đặt Python worker bằng Python của kernel để tránh trộn Python 3.12 và 3.14.

## Dữ liệu và thứ tự chạy

Chạy từ thư mục gốc; script tải bỏ qua tệp đã tồn tại:

```bash
.venv-labs/bin/python loi_giai/tai_du_lieu.py
.venv-labs/bin/python loi_giai/mapreduce.py
.venv-labs/bin/jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 loi_giai/02_Lab1_SparkSQL.ipynb
.venv-labs/bin/jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 loi_giai/03_Lab2_ML.ipynb
.venv-labs/bin/jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 loi_giai/04_Lab4_GianLan.ipynb
```

Lab1 tạo dữ liệu sạch tại `nyc_data/yellow_tripdata_2023-01-02-parquet`; Lab2 đọc chính thư mục này.
Lab4 đọc `aml_data/HI-Small_Trans.csv`. Có thể đặt biến môi trường `AML_PATH` để đổi đường dẫn.
Notebook hỗ trợ chạy khi thư mục làm việc là thư mục gốc hoặc `loi_giai/`.

Notebook mặc định chạy **toàn bộ** dữ liệu (`ML_FRACTION=1.0`, `AML_FRACTION=1.0`). Chạy nhanh với mẫu tái lập bằng seed 42, ví dụ trên Bash:

```bash
ML_FRACTION=0.02 .venv-labs/bin/jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 loi_giai/03_Lab2_ML.ipynb
AML_FRACTION=0.02 .venv-labs/bin/jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.kernel_name=labs --ExecutePreprocessor.timeout=1800 loi_giai/04_Lab4_GianLan.ipynb
```

Mẫu được xác định theo seed và phân hoạch đầu vào; thay cách đọc/phiên bản Spark có thể thay mẫu. Luôn xem số dòng và tỷ lệ in trong output.

## Cách diễn giải kết quả

- Lab1: skewness và excess kurtosis lớn chỉ là dấu hiệu ngoại lệ; không chứng minh mọi chuyến dài là lỗi. Tiền gắn với bản ghi lỗi thời gian không tự động đồng nghĩa tiền không thu được.
- Benchmark Lab1 so sánh DataFrame còn tính toán làm sạch với Parquet đã ghi và phân vùng. Vì vậy tỷ số thời gian bao gồm cả chi phí tính lại pipeline, định dạng lưu trữ và cache hệ điều hành; không thể quy hết mức tăng tốc cho partition pruning. Kế hoạch `PartitionFilters` xác nhận có cắt phân vùng.
- Lab2: StandardScaler chỉ fit trên train; cải tiến one-hot, biến chu kỳ và regularization được chọn bằng validation, rồi so cùng test. Không mặc định mô hình mới tốt hơn.
- Lab4: tài khoản là cặp ngân hàng + tài khoản; hai cột CSV `Account` được đọc theo vị trí. Mọi phép so tiền giữ cùng loại tiền tệ. Motif chỉ tạo cảnh báo, không kết luận rửa tiền.
- Lab4 ML: snapshot đồ thị chỉ lấy từ 20% thời gian đầu, không dùng nhãn hay cạnh tương lai. 40% kế tiếp huấn luyện, 20% validation chọn ngưỡng, 20% cuối kiểm tra. Tỷ lệ gần đúng do các giao dịch cùng timestamp không được tách.
- Accuracy cao vẫn có thể không nhận ra giao dịch rửa tiền. So thêm PR-AUC, precision, recall và F1 lớp 1. Lấy mẫu cạnh làm đứt mạng và mẫu test có thể rất ít nhãn dương; không suy rộng kết quả mẫu thành chất lượng trên toàn bộ IBM AML.

## Lưu mô hình

Mô hình nằm trong `loi_giai/models/` sau khi chạy. Lab2 lưu pipeline cơ sở và pipeline cải tiến riêng.
Lab4 lưu pipeline và snapshot lịch sử riêng; khi suy luận phải nạp snapshot, gọi `attach_history` rồi mới gọi `transform`.
Notebook ghi đè đúng thư mục đầu ra của chính bài khi chạy lại.

## Kiểm tra mã

Từ thư mục gốc:

```bash
.venv-labs/bin/python -m unittest discover -s tests -v
```

Kiểm thử MapReduce gồm cặp không thứ tự, sản phẩm trùng trong giỏ, hòa khoảng cách, tâm rỗng, hội tụ và điều kiện dừng.
Kiểm thử GraphFrames dùng CSV nhỏ có đáp án xác định: định danh hai đầu, tài khoản trùng mã khác ngân hàng, nguồn fan-in phân biệt, tự chuyển, biên 2 giờ, khác tiền tệ và phí biên 1%/5%.

## Nguồn

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page): Yellow Taxi tháng 01–02/2023 và taxi zone lookup.
- [IBM AML trên Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml): `HI-Small_Trans.csv`, phiên bản 8, 475.664.283 bytes. Đây là dữ liệu tổng hợp do IBM cung cấp; dùng theo giấy phép của bộ dữ liệu.
- [Kho IBM AML-Data](https://github.com/IBM/AML-Data): nguồn và mô tả bộ dữ liệu.
- [Cài GraphFrames](https://graphframes.io/02-quick-start/01-installation.html): client Python và gói JVM phải tương thích Spark/Scala.
- [Spark MLlib 4.0.2](https://spark.apache.org/docs/4.0.2/ml-guide.html): Pipeline, thuật toán và đánh giá.

## Phạm vi đã thực thi

- MapReduce: chương trình ví dụ chạy được; kiểm thử cặp sản phẩm và K-Means có đáp án cụ thể.
- Lab1: đã chạy tuần tự trên toàn bộ **5.980.721** dòng đầu vào, tạo **5.515.455** dòng sạch. Có **279** lỗi thời gian ngược, tổng tiền **10.585,13 USD**.
- Lab2: đã chạy tuần tự trên toàn bộ dữ liệu sạch, gồm **4.523.472** chuyến thẻ hợp lệ để gán nhãn.
- Lab4: đã chạy tuần tự trên **mẫu 2%** của tệp IBM gốc, gồm **101.813** giao dịch hợp lệ; **chưa thực thi trên toàn bộ tệp IBM**. Test có 20.340 giao dịch, chỉ 30 nhãn rửa tiền. Notebook có đầy đủ mã chạy toàn bộ bằng `AML_FRACTION=1.0`.
- Các notebook lưu output và hình; các biểu đồ đã được mở để kiểm tra nhãn và giá trị. Bản HTML phục vụ kiểm tra nằm trong `.work/previews/`; chưa kiểm tra giao diện HTML trong trình duyệt.

Lab4 trên mẫu: accuracy của mô hình cơ sở **97,6647%**, mô hình thêm đồ thị **98,0728%**, nhưng PR-AUC giảm **0,029607 → 0,026411** và recall giảm **0,5000 → 0,4333**. Kết quả chưa chứng minh đồ thị tốt hơn toàn diện. Baseline luôn đoán bình thường có accuracy **99,8525%** nhưng không phát hiện được giao dịch rửa tiền nào.

Lab2 sau khi đánh giá bằng xác suất lớp 1: ROC-AUC LR cơ sở **0,584663**, DT depth=5 **0,589745**, DT depth=10 **0,600730**, LR cải tiến **0,608202**. PR-AUC của LR tăng **0,799646 → 0,811161**. Có **10 kiểm thử đã qua**, gồm cả kiểm thử AUC để tránh dùng số mẫu trong lá thay cho xác suất lớp 1.
