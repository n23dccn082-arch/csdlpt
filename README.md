# Đồ án Cơ sở dữ liệu phân tán - Đề tài #38
## Time-Based Leases for Reliability: "IoT Sensor Data"

Đồ án này hiện thực và đánh giá giải pháp **Time-Based Leases** nhằm khắc phục điểm yếu **Blocking** của giao thức Cam kết hai pha (**Two-Phase Commit - 2PC**) truyền thống trên dòng dữ liệu cảm biến IoT.

### 1. Thành phần thư mục dự án
*   `advanced_2pc_simulator.py`: Mã nguồn Python cốt lõi mô phỏng toàn bộ hệ thống phân tán gồm 1 Coordinator đa luồng song song và 3 Participant Sites (`Site_North`, `Site_Central`, `Site_South`). Chương trình tự động sinh bộ dữ liệu đĩa vật lý, phân mảnh ngang dữ liệu theo `Region`, quản lý khóa tài nguyên dòng qua **Lock Table**, và kích hoạt **Local Auto-Abort** giải phóng khóa khi hết hạn Lease.
*   `sensor_readings.csv`: Bộ dữ liệu dòng cảm biến nhiệt độ & độ ẩm gốc được sinh vật lý trên đĩa.
*   `Site_North_storage.csv`, `Site_Central_storage.csv`, `Site_South_storage.csv`: Các tệp dữ liệu phân mảnh vật lý cục bộ lưu trữ các bản ghi cảm biến đã được cam kết thành công (COMMIT) tại từng Site tương ứng.
*   `lease_analysis.png`: Biểu đồ thực nghiệm kép (Dual-subplot Chart) phân tích chi tiết bài toán **Đánh đổi (Trade-off)**:
    1. Đồ thị bên trái: Tỷ lệ giao dịch thành công (Success Rate %) theo Lease Duration (ms).
    2. Đồ thị bên phải: Thời gian khóa tài nguyên trung bình (Avg Resource Lock Time ms) theo Lease Duration (ms).

### 2. Hướng dẫn cài đặt thư viện hỗ trợ
Để chạy chương trình mô phỏng và tự động sinh biểu đồ phân tích thực nghiệm, máy tính của bạn cần cài đặt các thư viện Python:
```bash
pip install pandas matplotlib
```


### 3. Cách chạy mô phỏng & Thực nghiệm
Chạy file mô phỏng bằng Python tại Terminal của thư mục `d:\ckcsdlpt`:
```bash
python advanced_2pc_simulator.py
```
**Chương trình sẽ tự động thực hiện các bước sau:**
1.  **Ghi file dữ liệu**: Kiểm tra và tự sinh file dữ liệu cảm biến IoT vật lý `sensor_readings.csv` (500 dòng).
2.  **Đọc luồng & Phân mảnh**: Đọc dữ liệu theo từng batch 5 dòng trực tiếp từ file CSV, định tuyến phân mảnh ngang dựa trên thuộc tính `Region` tới 3 Node cảm biến phân tán.
3.  **Điều phối song song**: Coordinator sử dụng đa luồng bất đồng bộ (`ThreadPoolExecutor`) để truyền thông điệp PREPARE song song tới các Site. Các Site tiến hành khóa tài nguyên trên Lock Table cục bộ.
4.  **Thử nghiệm thời gian Lease**: Chạy 30 giao dịch trên các mốc Lease từ 20ms đến 300ms với độ trễ mạng ngẫu nhiên (10ms - 50ms) và tỷ lệ lỗi ghi DB cục bộ (3%).
5.  **Ghi đĩa cục bộ**: Lưu trữ dữ liệu cam kết vật lý cục bộ tại các Site (`Site_North_storage.csv`, v.v.).
6.  **Vẽ biểu đồ Trade-off kép**: Xuất biểu đồ kép `lease_analysis.png` phân tích tính hiệu năng và rủi ro nghẽn tài nguyên.
7.  **Mô phỏng sự cố**: Coordinator bị sập đột ngột ngay sau Prepare. Log hiển thị rõ các Site phát hiện quá hạn bộ đếm Lease, tự động kích hoạt hủy giao dịch đơn phương và giải phóng Lock Table an toàn về 0.


### 4. Thông tin nhóm thực hiện
*   **Thành viên thực hiện**: Nguyễn Minh Đại Dương
*   **Môn học**: Cơ sở dữ liệu phân tán (Distributed Databases)
