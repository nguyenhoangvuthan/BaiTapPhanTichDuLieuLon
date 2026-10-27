# Kế hoạch hoàn thiện bốn tài liệu bài tập

**Mục tiêu:** giải toàn bộ yêu cầu trong PDF MapReduce, Lab1, Lab2 và Lab4 bằng tiếng Việt.
**Cách thực hiện:** giữ nguyên các đề gốc; tạo lời giải trong `loi_giai/`. Notebook giữ các bước thực hành, thêm mã và nhận xét tại từng bài. Chỉ ghi kết luận số khi đã chạy trên dữ liệu ghi rõ nguồn.
**Công nghệ:** Python, PySpark, GraphFrames, Jupyter; môi trường Python riêng.
**Đề tham chiếu:** bốn tài liệu tại thư mục gốc.

- [x] MapReduce: trình bày Map/Combine/Reduce/Driver, ví dụ, độ phức tạp, mã Python chạy được; kiểm tra cặp không thứ tự, hàng trùng và cụm rỗng.
- [x] Lab1: giải EDA, lỗi thời gian, pivot, top ngày, tuyến giờ cao/thấp điểm và partition pruning. Kiểm tra ranh giới giờ, ngày và thực thi action khi đo thời gian.
- [x] Lab2: hoàn thiện 9 bài; chia train/test trước fit scaler; so sánh cùng tập test, giải thích hệ số và cụm từ kết quả chạy; xử lý RatecodeID khi suy luận.
- [x] Lab4: hoàn thiện fan-in, mule, layering và so sánh mô hình; chuẩn hóa tên cột CSV trùng, định danh ngân hàng+tài khoản; chỉ so số tiền cùng tiền tệ; đặc trưng đồ thị chỉ lấy từ lịch sử train.
- [x] Kiểm tra cấu trúc notebook, chạy tuần tự trong môi trường phù hợp; ghi rõ việc kiểm tra bằng dữ liệu mẫu và việc chưa chạy trên dữ liệu gốc nếu thiếu.
- [x] Viết README hướng dẫn mở/chạy, nguồn dữ liệu, kết quả và giới hạn kiểm chứng.

## Nhật ký kiểm chứng

- MapReduce: 4 kiểm thử qua.
- Lab1: chạy hết trên hai tệp 01–02/2023, 5.980.721 dòng gốc và 5.515.455 dòng sạch.
- Lab4: chạy hết mẫu 2% dữ liệu IBM thật, 101.813 dòng; 5 kiểm thử đồ thị qua. Sửa định danh JSON dùng chung khóa bank/account ở cả hai đầu.
- AUC: kiểm thử cho thấy dùng rawPrediction số mẫu ở lá cây xếp hạng sai; chuyển ROC/PR sang xác suất, đã chạy lại toàn bộ Lab2, 5.515.455 dòng đầu vào và 4.523.472 chuyến thẻ hợp lệ.
- Đã yêu cầu một lượt rà soát độc lập; chưa nhận được báo cáo, đã dừng lượt rà soát. Kết luận bàn giao dựa trên việc chạy notebook, kiểm tra trực tiếp và 10 kiểm thử.

- Lượt chạy cuối Lab2: ROC/PR dùng probability, đã chạy hết không lỗi. Toàn bộ 10 kiểm thử qua. Đã mở kiểm tra cả 7 biểu đồ và thêm tóm tắt từ output đã thực thi.
