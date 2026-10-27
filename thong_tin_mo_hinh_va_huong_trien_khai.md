# THÔNG TIN MÔ HÌNH VÀ HƯỚNG TRIỂN KHAI TIỂU LUẬN

**Môn học:** Khai phá dữ liệu lớn (Big Data Mining)

**Tên đề tài:** Ứng dụng kiến trúc Delta Lake và Apache Spark trong xử lý dữ liệu lớn và xây dựng hệ thống gợi ý sản phẩm.

**Hình thức thực hiện:** Trình bày nội dung, mã nguồn, kết quả và nhận xét trong một Jupyter Notebook.

## 1. Bài toán và mục tiêu

Đề tài xây dựng hệ thống gợi ý **Top-10 sản phẩm** dựa trên lịch sử tương tác người dùng trong miền Retail. Mô hình khai phá dữ liệu chính là **Implicit Alternating Least Squares (ALS)**, triển khai bằng Spark MLlib.

Mục tiêu gồm:

- Xây dựng pipeline xử lý batch bằng PySpark và Spark SQL, tổ chức dữ liệu theo kiến trúc Bronze – Silver – Gold trên Delta Lake và MinIO.
- Huấn luyện ALS để sinh danh sách sản phẩm phù hợp với từng người dùng.
- So sánh chất lượng gợi ý với Popularity Baseline; khảo sát tham số mô hình và thời gian xử lý khi thay đổi quy mô dữ liệu.

## 2. Dữ liệu sử dụng

Sử dụng **T-ECD Small, chỉ miền Retail**, tại `dataset/small/retail/`. Theo kích thước tệp nguồn kiểm tra ngày 25/10/2026, dữ liệu gồm 227 tệp sự kiện và một tệp danh mục sản phẩm, tổng khoảng **2,13 GB Parquet**. Đây là dung lượng lưu trữ nguồn, chưa bao gồm bảng trung gian và mô hình. [Nguồn dữ liệu Retail](https://huggingface.co/datasets/t-tech/T-ECD/tree/main/dataset/small/retail)

Các thông tin chính cần khai thác là định danh người dùng, định danh sản phẩm, loại hành vi và thời gian sự kiện. Schema, mã hành vi mua hàng hoàn tất, số sự kiện, số người dùng, số sản phẩm và độ thưa sẽ được xác minh khi khảo sát dữ liệu. Hai bảng dùng chung `users.pq` và `brands.pq` chỉ bổ sung khi cần thuộc tính mô tả.

## 3. Mô hình đề xuất

### 3.1. Implicit ALS

ALS học các vector nhân tố ẩn của người dùng và sản phẩm; tích vô hướng giữa hai vector tạo điểm dùng để xếp hạng sản phẩm. Biến thể implicit khai thác mức độ tương tác như tín hiệu về độ tin cậy của sở thích, phù hợp với dữ liệu hành vi. Đề tài sử dụng triển khai có sẵn của Spark MLlib với `implicitPrefs=True`. [Tài liệu ALS của Spark](https://spark.apache.org/docs/latest/ml-collaborative-filtering.html)

| Thành phần | Hướng thực hiện |
|---|---|
| Đầu vào | Bảng `user_index`, `item_index`, `interaction_strength`; mỗi dòng là một cặp user–item tổng hợp từ lịch sử được phép dùng. |
| Biểu diễn tương tác | Tổng số sự kiện theo từng hành vi, có trọng số; lựa chọn trọng số trên validation sau khi xác minh các hành vi thực tế. |
| Tham số khảo sát | `rank`, `regParam`, `alpha`; cố định `maxIter` và seed trong mỗi phép so sánh. |
| Lựa chọn mô hình | Chọn cấu hình theo NDCG@10 trên validation, đồng thời báo cáo Recall@10 và thời gian huấn luyện. |
| Đầu ra | Danh sách tối đa 10 sản phẩm có điểm cao nhất cho mỗi user đủ điều kiện. |

Đề tài tập trung vào chuẩn bị dữ liệu, áp dụng và đánh giá mô hình; không đề xuất thuật toán ALS mới. Kết quả tốt hơn baseline là câu hỏi thực nghiệm, không phải kết luận giả định trước.

### 3.2. Popularity Baseline

Phương án đối chứng xếp hạng sản phẩm theo số sự kiện mua hàng hoàn tất trong lịch sử được phép dùng. Mỗi người dùng nhận danh sách phổ biến theo cùng tập ứng viên và quy tắc lọc của ALS. Điểm baseline được tính lại khi huấn luyện lại mô hình trước test. Quy tắc đếm sự kiện và xử lý đồng điểm được cố định trong notebook.

## 4. Kiến trúc và hướng triển khai

```text
T-ECD Small — Retail
         │
         ▼
PySpark Ingestion
         │
         ▼
Bronze → Silver → Gold Data Warehouse → Gold ML Data Mart
                          │                    │
                          ▼                    ▼
                  Phân tích Spark SQL    ALS và Popularity
                                               │
                                               ▼
                                      Gợi ý Top-10 và đánh giá
```

**Bronze, Silver và hai nhóm bảng Gold đều lưu ở định dạng Delta trên MinIO.** Spark thực hiện các bước xử lý; Jupyter Notebook là nơi điều khiển thực nghiệm và trình bày bài tiểu luận.

| Bước | Nội dung triển khai | Sản phẩm đầu ra |
|---|---|---|
| 1. Khảo sát và thiết lập | Kiểm tra schema, thời gian, hành vi; thiết lập Spark, Delta Lake và kết nối MinIO. | Thống kê dữ liệu và cấu hình môi trường. |
| 2. Bronze | Nạp dữ liệu gốc theo batch ngày, bổ sung metadata nguồn; nạp lại đúng phân vùng để tránh nhân đôi. | Bảng sự kiện gốc có khả năng truy vết. |
| 3. Silver | Chuẩn hóa kiểu dữ liệu và thời gian, xử lý thiếu/sai; giữ các tương tác lặp hợp lệ. | Bảng tương tác sạch và báo cáo chất lượng. |
| 4. Gold Data Warehouse | Tổ chức bảng fact tương tác và các bảng dimension cần thiết; phân tích hành vi bằng Spark SQL. | Các bảng phân tích, thống kê và biểu đồ. |
| 5. Gold ML Data Mart | Chia sự kiện theo thời gian trước khi tổng hợp; tạo ánh xạ ID, bảng user–item và nhãn đánh giá. | Dữ liệu train, validation, lịch sử huấn luyện lại và test. |
| 6. Huấn luyện | Xây baseline, khảo sát tham số ALS trên validation, chốt cấu hình. | Mô hình được chọn và bảng kết quả validation. |
| 7. Đánh giá và báo cáo | Huấn luyện lại trên lịch sử train + validation hợp lệ; đánh giá test và đo hiệu năng pipeline. | Kết quả thực nghiệm, minh họa gợi ý và nhận xét. |

Các kiểm chứng Delta Lake gồm nạp lại batch không nhân đôi, ghi nhận lịch sử thay đổi và đọc lại phiên bản bảng đã dùng cho thực nghiệm. Các phép thử thay đổi schema được thực hiện trên bảng thử nghiệm riêng.

## 5. Thiết kế đánh giá

### 5.1. Phân chia dữ liệu và chống rò rỉ

- Dự kiến dùng 14 ngày cuối làm test, 14 ngày liền trước làm validation và phần trước đó làm lịch sử train; mốc cụ thể được chốt sau khảo sát, trước khi xem kết quả test.
- Lịch sử dùng để dự đoán phải kết thúc trước đầu cửa sổ đánh giá ít nhất **12 giờ**, theo yêu cầu của T-ECD; nếu thời gian chỉ xác định theo ngày, dùng gap một ngày. [Hướng dẫn thời gian của dataset](https://huggingface.co/datasets/t-tech/T-ECD)
- Chỉ tổng hợp user–item trong lịch sử được phép dùng. Validation chọn cấu hình; sau đó huấn luyện lại trên lịch sử train + validation, vẫn giữ gap trước test.
- Nhãn đánh giá là sản phẩm có sự kiện mua hàng hoàn tất trong cửa sổ đánh giá; một sản phẩm chỉ tính một lần trong tập nhãn của mỗi user.

### 5.2. Quy tắc so sánh

ALS và Popularity dùng chung người dùng đánh giá, nhãn và tập ứng viên. Xếp hạng trên toàn bộ item xuất hiện trong lịch sử được phép dùng, không lấy mẫu ứng viên.

Giao thức chính cho phép gợi ý lại sản phẩm đã tương tác. Chỉ số chính tính trên user có lịch sử và có nhãn mua hàng trong cửa sổ đánh giá. Các item mới chưa có trong lịch sử vẫn nằm trong tập nhãn để phản ánh những sản phẩm hệ thống không thể gợi ý; tỷ lệ này được báo riêng. User không có lịch sử được thống kê riêng và có thể đánh giá bằng Popularity.

Giao thức phụ đánh giá khám phá sản phẩm mới: loại item đã tương tác khỏi cả tập ứng viên và tập nhãn của từng user; chỉ tính chỉ số trên user còn ít nhất một nhãn và báo số lượng user bị loại. Hai mô hình dùng cùng quy tắc trong từng giao thức.

### 5.3. Thực nghiệm và chỉ số

| Thực nghiệm | Mục tiêu | Kết quả cần trình bày |
|---|---|---|
| **E1** | So sánh ALS và Popularity trên test cố định. | Recall@10, NDCG@10, số user được đánh giá và tỷ lệ cold-start. |
| **E2** | Khảo sát ảnh hưởng tham số ALS trên validation. | Bảng/biểu đồ chất lượng và thời gian huấn luyện theo cấu hình. |
| **E3** | Khảo sát thời gian xử lý ở 25%, 50%, 75% và 100% số ngày Retail, tính từ ngày đầu. | Số dòng, dung lượng, thời gian từng bước ETL và huấn luyện; minh chứng Spark UI. |

**Recall@10** của mỗi user bằng số sản phẩm liên quan tìm được trong Top-10 chia cho tổng số sản phẩm liên quan trong tập nhãn của user đó. **NDCG@10** đo chất lượng thứ hạng với relevance nhị phân. Hai chỉ số được lấy trung bình trên các user đủ điều kiện của từng giao thức. [Định nghĩa chỉ số trong Spark](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.mllib.evaluation.RankingMetrics.html)

E3 giữ cố định tài nguyên, cấu hình ALS và chính sách cache; mỗi quy mô chạy ba lần, báo trung vị và min–max, kèm thời gian lần đầu riêng. Chỉ đo đến khi Spark hoàn tất thực thi. Ghi rõ trạng thái cache Spark; việc khởi động lại Spark không bảo đảm cache hệ điều hành hoặc lưu trữ đã trống.

## 6. Tài liệu tham khảo

1. [T-ECD Dataset](https://huggingface.co/datasets/t-tech/T-ECD).
2. [Apache Spark — Collaborative Filtering](https://spark.apache.org/docs/latest/ml-collaborative-filtering.html).
3. [Apache Spark — RankingMetrics](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.mllib.evaluation.RankingMetrics.html).
4. [Delta Lake Documentation](https://docs.delta.io/).
5. [Medallion Architecture](https://docs.databricks.com/aws/en/lakehouse/medallion).
