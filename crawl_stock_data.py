"""
Script thu thập dữ liệu chứng khoán từ Alpha Vantage API
Lưu dữ liệu vào thư mục datack theo định dạng CSV
"""

import requests
import time
import csv
import os
from datetime import datetime

# Cấu hình API
API_KEY = "YOUR_ALPHA_VANTAGE_API_KEY"  # Đăng ký miễn phí tại https://www.alphavantage.co/support/#api-key
BASE_URL = "https://www.alphavantage.co/query"

# Tạo thư mục lưu dữ liệu
DATA_DIR = "datack"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def load_symbols(file_path="Crawl_code/symbol.txt"):
    """Đọc danh sách mã cổ phiếu từ file"""
    symbols = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                symbol = line.strip()
                if symbol and not symbol.startswith('#'):
                    symbols.append(symbol)
        print(f"Đã tải {len(symbols)} mã cổ phiếu")
        return symbols
    except FileNotFoundError:
        print(f"Không tìm thấy file {file_path}")
        # Trả về một số mã cổ phiếu mặc định
        return ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'FB', 'NVDA', 'JPM', 'V', 'WMT']

def fetch_stock_data(symbol):
    """
    Lấy dữ liệu lịch sử giá cổ phiếu từ Alpha Vantage
    
    Args:
        symbol: Mã cổ phiếu
        
    Returns:
        dict: Dữ liệu thời gian series hoặc None nếu lỗi
    """
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': API_KEY,
        'outputsize': 'full',  # Lấy toàn bộ dữ liệu (20+ năm)
        'datatype': 'json'
    }
    
    try:
        print(f"Đang tải dữ liệu cho {symbol}...", end=' ')
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Kiểm tra lỗi từ API
        if 'Error Message' in data:
            print(f"❌ Lỗi: {data['Error Message']}")
            return None
        
        if 'Note' in data:
            print(f"⚠️  Vượt giới hạn API")
            return None
            
        if 'Time Series (Daily)' in data:
            print("✅ Thành công")
            return data['Time Series (Daily)']
        else:
            print("❌ Không có dữ liệu")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Lỗi kết nối: {e}")
        return None
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return None

def save_to_csv(symbol, time_series):
    """
    Lưu dữ liệu vào file CSV
    
    Args:
        symbol: Mã cổ phiếu
        time_series: Dữ liệu thời gian series
    """
    if not time_series:
        return
    
    filename = os.path.join(DATA_DIR, f"stock_market_data-{symbol}_2020-12-15.csv")
    
    try:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Header
            writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
            
            # Sắp xếp theo ngày
            sorted_dates = sorted(time_series.keys(), reverse=True)
            
            # Ghi dữ liệu
            for date in sorted_dates:
                data = time_series[date]
                writer.writerow([
                    date,
                    data.get('1. open', ''),
                    data.get('2. high', ''),
                    data.get('3. low', ''),
                    data.get('4. close', ''),
                    data.get('5. volume', '')
                ])
        
        print(f"  💾 Đã lưu vào {filename}")
        
    except Exception as e:
        print(f"  ❌ Lỗi khi lưu file: {e}")

def crawl_all_stocks(symbols, delay=12):
    """
    Thu thập dữ liệu cho tất cả các mã cổ phiếu
    
    Args:
        symbols: Danh sách mã cổ phiếu
        delay: Thời gian chờ giữa các request (giây) để tránh vượt giới hạn API
    """
    total = len(symbols)
    success = 0
    failed = 0
    
    print(f"\n🚀 Bắt đầu thu thập dữ liệu cho {total} mã cổ phiếu")
    print(f"⏱️  Thời gian chờ giữa các request: {delay}s")
    print("=" * 60)
    
    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{total}] ", end='')
        
        time_series = fetch_stock_data(symbol)
        
        if time_series:
            save_to_csv(symbol, time_series)
            success += 1
        else:
            failed += 1
        
        # Chờ trước khi request tiếp theo (trừ lần cuối)
        if i < total:
            print(f"  ⏳ Chờ {delay}s...\n")
            time.sleep(delay)
    
    print("\n" + "=" * 60)
    print(f"✅ Hoàn thành: {success}/{total} thành công, {failed} thất bại")
    print(f"📁 Dữ liệu đã được lưu trong thư mục: {DATA_DIR}/")

def main():
    """Hàm chính"""
    print("=" * 60)
    print("      THU THẬP DỮ LIỆU CHỨNG KHOÁN - ALPHA VANTAGE")
    print("=" * 60)
    
    # Kiểm tra API key
    if API_KEY == "YOUR_ALPHA_VANTAGE_API_KEY":
        print("\n⚠️  CẢNH BÁO: Bạn cần thay thế API_KEY bằng key thực của bạn!")
        print("📌 Đăng ký miễn phí tại: https://www.alphavantage.co/support/#api-key")
        print("\n💡 Hoặc sử dụng dữ liệu mẫu có sẵn trong thư mục datack/")
        return
    
    # Tải danh sách mã cổ phiếu
    symbols = load_symbols()
    
    # Thu thập dữ liệu
    # Lưu ý: API miễn phí giới hạn 5 requests/phút, 500 requests/ngày
    crawl_all_stocks(symbols[:10], delay=15)  # Chỉ lấy 10 mã đầu tiên để demo

if __name__ == "__main__":
    main()