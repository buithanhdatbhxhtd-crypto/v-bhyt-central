import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from unidecode import unidecode
import time

# --- 1. CẤU HÌNH TRANG ---
st.set_page_config(
    page_title="Hệ thống Tra cứu BHYT Việt Nam",
    page_icon="🏥",
    layout="wide"
)

# --- 2. KẾT NỐI DATABASE ---
# Lưu ý: Chúng ta lấy URI từ secrets để bảo mật, không viết trực tiếp vào code
def get_db_connection():
    try:
        # st.secrets sẽ được cấu hình trên giao diện Streamlit Cloud sau này
        conn = psycopg2.connect(st.secrets["SUPABASE_DB_URL"])
        return conn
    except Exception as e:
        st.error(f"Không thể kết nối cơ sở dữ liệu: {e}")
        return None

# --- 3. LOGIC TRUY VẤN DỮ LIỆU ---
def search_participants(search_query, search_type, limit=50):
    conn = get_db_connection()
    if not conn:
        return []
    
    cur = conn.cursor()
    try:
        if search_type == "Mã BHXH":
            query = "SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the FROM participants WHERE ma_so_bhxh = %s LIMIT %s"
            cur.execute(query, (search_query, limit))
        elif search_type == "CCCD":
            query = "SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the FROM participants WHERE cccd = %s LIMIT %s"
            cur.execute(query, (search_query, limit))
        else: # Tìm theo tên (Sử dụng Fuzzy Search qua Trigram Index đã tạo trong Canvas)
            # Chuẩn hóa chuỗi tìm kiếm: viết thường, không dấu
            search_norm = unidecode(search_query).lower()
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE ho_ten_unsigned % %s OR ho_ten_unsigned ILIKE %s
                ORDER BY similarity(ho_ten_unsigned, %s) DESC
                LIMIT %s
            """
            cur.execute(query, (search_norm, f"%{search_norm}%", search_norm, limit))
        
        results = cur.fetchall()
        return results
    except Exception as e:
        st.error(f"Lỗi truy vấn: {e}")
        return []
    finally:
        cur.close()
        conn.close()

# --- 4. GIAO DIỆN NGƯỜI DÙNG ---

def main():
    st.title("🏥 Hệ thống Quản lý & Tra cứu BHYT")
    st.markdown("---")

    # Thanh bên (Sidebar) để chuyển đổi chức năng
    menu = ["Tra cứu nhanh", "Hướng dẫn sử dụng"]
    choice = st.sidebar.selectbox("Chức năng chính", menu)

    if choice == "Tra cứu nhanh":
        col1, col2 = st.columns([3, 1])
        
        with col1:
            q = st.text_input("Nhập thông tin cần tìm (Tên, Mã BHXH hoặc số CCCD)", placeholder="Ví dụ: Nguyen Van A")
        with col2:
            stype = st.selectbox("Tìm kiếm theo", ["Tên", "Mã BHXH", "CCCD"])

        if q:
            with st.spinner("Đang lục tìm trong 500.000 bản ghi..."):
                start_time = time.time()
                data = search_participants(q, stype)
                duration = time.time() - start_time
                
                if data:
                    st.success(f"Tìm thấy {len(data)} kết quả trong {duration:.3f} giây.")
                    
                    # Chuyển dữ liệu sang DataFrame để hiển thị đẹp hơn
                    df = pd.DataFrame(data, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "SĐT", "Hạn Thẻ"])
                    
                    # Che giấu thông tin nhạy cảm (Data Masking)
                    df['CCCD'] = df['CCCD'].apply(lambda x: f"{x[:3]}****{x[-3:]}" if x and len(str(x)) > 6 else x)
                    df['SĐT'] = df['SĐT'].apply(lambda x: f"{x[:4]}***{x[-3:]}" if x and len(str(x)) > 7 else x)
                    
                    # Hiển thị bảng
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    
                    # Nút xuất file nếu cần
                    st.download_button(
                        label="📥 Tải kết quả về Excel (CSV)",
                        data=df.to_csv(index=False).encode('utf-8-sig'),
                        file_name=f"ket_qua_tra_cuu_{q}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("Không tìm thấy dữ liệu phù hợp. Vui lòng kiểm tra lại thông tin nhập.")

    elif choice == "Hướng dẫn sử dụng":
        st.subheader("Hướng dẫn dành cho cán bộ")
        st.info("""
        1. **Tìm theo tên:** Bạn có thể gõ tiếng Việt có dấu hoặc không dấu. Hệ thống hỗ trợ tìm kiếm gần đúng (sai lệch 1-2 ký tự vẫn ra kết quả).
        2. **Tìm theo mã:** Luôn ưu tiên tìm theo Mã BHXH hoặc CCCD để có độ chính xác tuyệt đối.
        3. **Bảo mật:** Hệ thống tự động che số CCCD và SĐT để bảo vệ quyền riêng tư.
        """)

if __name__ == "__main__":
    main()