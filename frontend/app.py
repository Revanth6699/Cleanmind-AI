import streamlit as st
import requests
import pandas as pd
import time

# =========================================
# CONFIG – BACKEND URL
# =========================================
BACKEND_URL = "http://127.0.0.1:8000"


# =========================================
# BACKEND CALL HELPERS
# =========================================
def backend_register(email: str, password: str):
    r = requests.post(
        f"{BACKEND_URL}/auth/register",
        json={"email": email, "password": password},
    )
    r.raise_for_status()
    return r.json()  # {email: ...}


def backend_login(email: str, password: str) -> str:
    r = requests.post(
        f"{BACKEND_URL}/auth/login",
        json={"email": email, "password": password},
    )
    r.raise_for_status()
    data = r.json()
    return data["access_token"]  # JWT


def backend_me(token: str):
    r = requests.get(
        f"{BACKEND_URL}/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r.json()  # {email: ...}


def upload_file(file):
    files = {"file": (file.name, file.getvalue())}
    r = requests.post(f"{BACKEND_URL}/datasets/upload", files=files)
    r.raise_for_status()
    return r.json()["dataset_id"]


def get_profile(dataset_id: str):
    r = requests.get(f"{BACKEND_URL}/datasets/{dataset_id}/profile")
    r.raise_for_status()
    return r.json()


def get_quality(dataset_id: str):
    r = requests.get(f"{BACKEND_URL}/datasets/{dataset_id}/quality_score")
    r.raise_for_status()
    return r.json()


def run_cleaner(dataset_id: str):
    payload = {
        "drop_duplicates": True,
        "impute_missing": True,
        "impute_strategy": "median",
        "remove_outliers": True,
        "outlier_zscore_threshold": 3,
    }
    r = requests.post(
        f"{BACKEND_URL}/datasets/{dataset_id}/clean",
        json=payload,
    )
    r.raise_for_status()
    return r.json()


def download_file(dataset_id: str) -> bytes:
    r = requests.get(f"{BACKEND_URL}/datasets/{dataset_id}/download")
    r.raise_for_status()
    return r.content


# =========================================
# SESSION INIT
# =========================================
def init_session():
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
    if "access_token" not in st.session_state:
        st.session_state["access_token"] = None
    if "current_user" not in st.session_state:
        st.session_state["current_user"] = None


# =========================================
# AUTH UI
# =========================================
def render_header():
    st.set_page_config(page_title="CleanMind AI", layout="wide")
    st.markdown(
        """
        <style>
        .top-bar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0.6rem 0.2rem 0.4rem 0.2rem;
            border-bottom: 1px solid #444;
        }
        .brand {
            font-size: 1.4rem;
            font-weight: 700;
        }
        .tagline {
            font-size: 0.9rem;
            color: #888;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <div class="top-bar">
            <div>
                <div class="brand">🧠 CleanMind AI</div>
                <div class="tagline">Smart data cleaning & profiling assistant</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_auth_view():
    st.markdown("### 🔐 Sign in to continue")
    tab_login, tab_signup = st.tabs(["Login", "Create Account"])

    with tab_login:
        st.subheader("Welcome back")
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Password", type="password", key="login_password")
        if st.button("Login", type="primary"):
            try:
                token = backend_login(email, password)
                st.session_state["access_token"] = token
                # confirm user via /auth/me
                user_info = backend_me(token)
                st.session_state["current_user"] = user_info["email"]
                st.session_state["logged_in"] = True
                st.success(f"Welcome, {user_info['email']}!")
                st.experimental_rerun()
            except requests.HTTPError as e:
                try:
                    detail = e.response.json().get("detail", str(e))
                except Exception:
                    detail = str(e)
                st.error(f"Login failed: {detail}")

    with tab_signup:
        st.subheader("Create a new account")
        email = st.text_input("New Email", key="signup_email")
        password = st.text_input("New Password", type="password", key="signup_password")
        confirm = st.text_input(
            "Confirm Password", type="password", key="signup_confirm"
        )
        if st.button("Create Account"):
            if not email or not password or not confirm:
                st.warning("All fields are required.")
            elif password != confirm:
                st.error("Passwords do not match.")
            else:
                try:
                    user = backend_register(email, password)
                    st.success(f"Account created for {user['email']}. You can log in now.")
                except requests.HTTPError as e:
                    try:
                        detail = e.response.json().get("detail", str(e))
                    except Exception:
                        detail = str(e)
                    st.error(f"Signup failed: {detail}")


def render_top_user_bar():
    col_left, col_right = st.columns([3, 1])
    with col_left:
        st.subheader("📊 Data Cleaning Workspace")
    with col_right:
        user = st.session_state["current_user"]
        st.write(f"👤 {user}")
        if st.button("Logout"):
            st.session_state["logged_in"] = False
            st.session_state["access_token"] = None
            st.session_state["current_user"] = None
            st.experimental_rerun()


# =========================================
# MAIN CLEANING UI
# =========================================
def render_cleaning_app():
    st.markdown("#### Step 1 · Upload your dataset")

    uploaded = st.file_uploader(
        "📂 Upload your dataset (any supported format)",
        type=[
            "csv",
            "tsv",
            "txt",
            "log",
            "xlsx",
            "xls",
            "ods",
            "json",
            "jsonl",
            "ndjson",
            "xml",
            "yaml",
            "yml",
            "html",
            "htm",
            "parquet",
            "feather",
            "orc",
            "h5",
            "hdf5",
            "pdf",
            "docx",
            "png",
            "jpg",
            "jpeg",
            "wav",
        ],
        help="Supports CSV, Excel, JSON, XML, YAML, Parquet, Feather, PDF, DOCX, images & more (as configured in backend).",
    )

    if not uploaded:
        st.info("Choose a file to start the profiling & cleaning workflow.")
        return

    # 1) Upload
    with st.spinner("📤 Uploading dataset to backend..."):
        dataset_id = upload_file(uploaded)
        st.success(f"✔ Uploaded — dataset_id: {dataset_id}")

    time.sleep(0.2)

    # 2) Profile
    with st.spinner("📊 Analyzing dataset profile..."):
        profile = get_profile(dataset_id)
        st.markdown("#### Step 2 · Dataset Profile")
        st.write(f"Rows: **{profile['n_rows']}** | Columns: **{profile['n_cols']}**")

        rows = []
        for col_name, info in profile["columns"].items():
            rows.append(
                {
                    "Column": col_name,
                    "Type": info["dtype"],
                    "Missing": info["n_missing"],
                    "% Missing": round(info["pct_missing"], 2),
                    "Unique": info["n_unique"],
                }
            )
        if rows:
            df_profile = pd.DataFrame(rows)
            st.dataframe(df_profile, use_container_width=True)

    time.sleep(0.2)

    # 3) Quality
    with st.spinner("🧮 Calculating data quality score..."):
        quality = get_quality(dataset_id)
        st.markdown("#### Step 3 · Data Quality")
        st.metric("Quality Score", f"{quality['quality_score']:.2f}")
        st.json(quality["metrics"])

    time.sleep(0.2)

    # 4) Cleaning
    with st.spinner("🧼 Cleaning dataset automatically..."):
        cleaned = run_cleaner(dataset_id)

        st.markdown("#### Step 4 · Cleaning Summary")
        st.success("Cleaning complete ✅")

        c1, c2 = st.columns(2)
        with c1:
            st.write(
                f"Rows **Before → After**: "
                f"**{cleaned['n_rows_before']} → {cleaned['n_rows_after']}**"
            )
            st.write(
                f"Missing Values **Before → After**: "
                f"**{cleaned['n_missing_before']} → {cleaned['n_missing_after']}**"
            )
        with c2:
            st.write(f"Outlier Rows Removed: **{cleaned['outlier_rows_removed']}**")
            st.write(
                f"Duplicate Rows Removed: **{cleaned['duplicate_rows_removed']}**"
            )

        preview_df = pd.DataFrame(cleaned["preview_rows"])
        st.markdown("#### Step 5 · Cleaned Data Preview (Top 20 Rows)")
        st.dataframe(preview_df, use_container_width=True)

    cleaned_id = cleaned["cleaned_dataset_id"]

    st.markdown("---")
    st.markdown("#### Step 6 · Download Cleaned Dataset")

    cleaned_bytes = download_file(cleaned_id)
    st.download_button(
        label="💾 Download Cleaned CSV",
        data=cleaned_bytes,
        file_name=f"Cleaned_{cleaned_id}.csv",
        mime="text/csv",
    )


# =========================================
# MAIN ENTRY
# =========================================
def main():
    init_session()
    render_header()

    if not st.session_state["logged_in"]:
        render_auth_view()
    else:
        render_top_user_bar()
        st.markdown("---")
        render_cleaning_app()


if __name__ == "__main__":
    main()
