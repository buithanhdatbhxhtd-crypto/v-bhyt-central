import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from unidecode import unidecode
import time

# --- 1. CẤU HÌNH TRANG ---
st.set_page_config(
    page_title="Hệ thống Quản lý & Tra cứu BHYT",
    page_icon="🏥",
    layout="wide"
)

# --- 2. KẾT NỐI DATABASE ---
def get_db_connection():
    try:
        # Khuyên dùng Connection Pooler (Port 6543) để tránh lỗi connection
        conn = psycopg2.connect(st.secrets["SUPABASE_DB_URL"])
        return conn
    except Exception as e:
        return None

# --- 3. LOGIC TRUY VẤN DỮ LIỆU ---
def search_participants(search_query, search_type, limit=100):
    conn = get_db_connection()
    if not conn: 
        st.error("Không thể kết nối cơ sở dữ liệu. Vui lòng kiểm tra lại cấu hình trong Secrets.")
        return []
    
    cur = conn.cursor()
    try:
        # Làm sạch input tìm kiếm, loại bỏ khoảng trắng thừa
        q_clean = search_query.strip()
        
        if search_type == "Mã BHXH":
            # Tìm kiếm chính xác mã số, loại bỏ khoảng trắng trong DB để so khớp
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE TRIM(ma_so_bhxh) = %s OR ma_so_bhxh ILIKE %s
                LIMIT %s
            """
            cur.execute(query, (q_clean, f"%{q_clean}%", limit))
            
        elif search_type == "CCCD":
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE TRIM(cccd) = %s OR cccd ILIKE %s
                LIMIT %s
            """
            cur.execute(query, (q_clean, f"%{q_clean}%", limit))
            
        else: # Tìm kiếm theo Tên (Thu hẹp phạm vi để chính xác hơn)
            # 1. Chuẩn hóa chuỗi tìm kiếm (không dấu, viết thường)
            q_norm = unidecode(q_clean).lower()
            
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE 
                    -- Ưu tiên: Tên chứa toàn bộ cụm từ tìm kiếm (Ví dụ: 'nguyen van quyen')
                    ho_ten_unsigned ILIKE %s 
                    -- Hoặc có độ tương đồng cực cao (ngưỡng 0.7 thay vì 0.5 để thu hẹp kết quả)
                    OR (similarity(ho_ten_unsigned, %s) > 0.7)
                ORDER BY 
                    (ho_ten_unsigned = %s) DESC, -- Khớp chính xác tuyệt đối lên đầu
                    similarity(ho_ten_unsigned, %s) DESC
                LIMIT %s
            """
            # Tham số: %query%, query, query, query, limit
            cur.execute(query, (f"%{q_norm}%", q_norm, q_norm, q_norm, limit))
        
        return cur.fetchall()
    except Exception as e:
        st.error(f"Lỗi khi tìm kiếm: {e}")
        return []
    finally:
        cur.close()
        conn.close()

# --- 4. LOGIC NHẬP DỮ LIỆU BATCH ---
def import_excel_to_db(df):
    # Chuẩn hóa tên cột
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # Ánh xạ tên cột linh hoạt
    mapping = {
        'ma so bhxh': 'ma_so_bhxh',
        'ma the bhyt': 'ma_the_bhyt',
        'ho ten': 'ho_ten',
        'ngay sinh': 'ngay_sinh',
        'socmnd': 'cccd',
        'sodient': 'sdt',
        'diachilh': 'dia_chi',
        'hantheden': 'han_the'
    }
    df = df.rename(columns=mapping)
    
    conn = get_db_connection()
    if not conn: 
        st.error("Lỗi kết nối cơ sở dữ liệu. Hãy đảm bảo bạn đang dùng Connection Pooler (Port 6543).")
        return
    
    cur = conn.cursor()
    try:
        data = []
        if 'ma_so_bhxh' not in df.columns or 'ho_ten' not in df.columns:
            st.error(f"File thiếu cột 'ma so bhxh' hoặc 'ho ten'. Cột hiện có: {list(df.columns)}")
            return

        for _, row in df.iterrows():
            def parse_date(val):
                if pd.isnull(val) or str(val).strip() == "": return None
                try: return pd.to_datetime(val).date()
                except: return None

            def clean_str(val):
                if pd.isnull(val): return ""
                # Giữ nguyên chuỗi, chỉ xử lý trường hợp bị dính .0 do Excel hiểu nhầm là số float
                s = str(val).strip()
                return s.split('.')[0] if s.endswith('.0') else s

            data.append((
                clean_str(row['ma_so_bhxh']), 
                clean_str(row.get('ma_the_bhyt', '')), 
                str(row['ho_ten']).strip(),
                parse_date(row.get('ngay_sinh')),
                clean_str(row.get('cccd', '')),
                clean_str(row.get('sdt', '')),
                str(row.get('dia_chi', '')).strip(),
                parse_date(row.get('han_the'))
            ))

        sql = """
            INSERT INTO participants (ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, dia_chi, han_the)
            VALUES %s
            ON CONFLICT (ma_so_bhxh) DO UPDATE SET
                ma_the_bhyt = EXCLUDED.ma_the_bhyt,
                ho_ten = EXCLUDED.ho_ten,
                ngay_sinh = EXCLUDED.ngay_sinh,
                cccd = EXCLUDED.cccd,
                sdt = EXCLUDED.sdt,
                dia_chi = EXCLUDED.dia_chi,
                han_the = EXCLUDED.han_the,
                updated_at = NOW();
        """
        
        batch_size = 2000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            execute_values(cur, sql, batch)
            conn.commit()
            yield min(i + batch_size, len(data))
            
    except Exception as e:
        st.error(f"Lỗi SQL khi nạp: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# --- 5. GIAO DIỆN CHÍNH ---
def main():
    st.title("🏥 Hệ thống Quản lý & Tra cứu BHYT")
    st.sidebar.header("🛡️ Bảng điều khiển")
    mode = st.sidebar.radio("Chọn chức năng", ["Tra cứu dữ liệu", "Quản trị viên (Nhập dữ liệu)"])

    if mode == "Tra cứu dữ liệu":
        st.subheader("🔍 Tìm kiếm người tham gia")
        st.caption("Lưu ý: Hệ thống ưu tiên kết quả khớp chính xác cụm từ bạn nhập.")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            q = st.text_input("Nhập nội dung tìm kiếm...", placeholder="Nhập tên, mã BHXH hoặc số CCCD")
        with col2:
            stype = st.selectbox("Loại tìm kiếm", ["Tên", "Mã BHXH", "CCCD"])

        if q:
            with st.spinner("Đang truy xuất dữ liệu..."):
                start_time = time.time()
                data = search_participants(q, stype)
                duration = time.time() - start_time
                
                if data:
                    st.success(f"Tìm thấy {len(data)} bản ghi trong {duration:.3f} giây.")
                    df_res = pd.DataFrame(data, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "SĐT", "Hạn Thẻ"])
                    
                    # Data Masking cho CCCD
                    df_res['CCCD'] = df_res['CCCD'].apply(lambda x: f"{x[:3]}****{x[-3:]}" if x and len(str(x)) > 6 else x)
                    
                    st.dataframe(df_res, use_container_width=True, hide_index=True)
                    
                    st.download_button(
                        "📥 Tải kết quả này (CSV)",
                        df_res.to_csv(index=False).encode('utf-8-sig'),
                        f"tra_cuu_{q}.csv",
                        "text/csv"
                    )
                else:
                    st.warning("Không tìm thấy kết quả phù hợp. Hãy kiểm tra lại từ khóa hoặc loại tìm kiếm.")

    else: # Quản trị viên
        st.subheader("📥 Nhập dữ liệu từ Excel (XLSX/XLSB)")
        st.warning("⚠️ Hệ thống sẽ giữ nguyên các số 0 ở đầu mã số BHXH/CCCD.")
        
        uploaded_file = st.file_uploader("Chọn tệp dữ liệu", type=["xlsx", "xlsb"])
        if uploaded_file:
            try:
                engine = 'pyxlsb' if uploaded_file.name.endswith('.xlsb') else None
                # QUAN TRỌNG: dtype=str để không bị mất số 0 đầu
                df_preview = pd.read_excel(uploaded_file, engine=engine, dtype=str)
                
                st.write(f"📊 Phát hiện: **{len(df_preview):,}** hàng dữ liệu.")
                st.dataframe(df_preview.head(10)) # Hiện 10 dòng để kiểm tra số 0 đầu
                
                if st.button("🚀 Thực hiện nạp vào hệ thống"):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    total = len(df_preview)
                    success_count = 0
                    for count in import_excel_to_db(df_preview):
                        percent = count / total
                        progress_bar.progress(percent)
                        status_text.text(f"Đang xử lý: {count:,} / {total:,} hàng...")
                        success_count = count
                    
                    if success_count > 0:
                        st.success(f"✅ Đã nạp thành công {success_count:,} bản ghi!")
                        st.balloons()
            except Exception as e:
                st.error(f"Lỗi: {e}")

if __name__ == "__main__":
    main()
