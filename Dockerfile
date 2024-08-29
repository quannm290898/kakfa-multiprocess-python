# Sử dụng hình ảnh Python chính thức làm hình ảnh cơ bản
FROM python:3.11.9-alpine

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Sao chép tệp requirements.txt vào container
COPY requirements.txt .

# Cài đặt các gói Python từ requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ mã nguồn của bạn vào container
COPY . .

# Chạy ứng dụng khi container khởi động
CMD ["python", "main.py"]