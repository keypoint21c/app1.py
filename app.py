# app1.py (Sales-grade Consulting PDF Engine)
import os
import io
import json
import math
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple

import pandas as pd
import streamlit as st

# PDF (ReportLab)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
import os

BASE_DIR = os.path.dirname(__file__)
FONT_REG = os.path.join(BASE_DIR, "fonts", "NotoSansKR-Regular.ttf")
FONT_BOLD = os.path.join(BASE_DIR, "fonts", "NotoSansKR-Bold.ttf")

pdfmetrics.registerFont(TTFont("NotoSansKR", FONT_REG))
if os.path.exists(FONT_BOLD):
    pdfmetrics.registerFont(TTFont("NotoSansKR-Bold", FONT_BOLD))

styles = getSampleStyleSheet()
STYLE_BODY = ParagraphStyle(
    "body",
    parent=styles["Normal"],
    fontName="NotoSansKR",
    fontSize=10.5,
    leading=14,
)
STYLE_H1 = ParagraphStyle(
    "h1",
    parent=styles["Heading1"],
    fontName="NotoSansKR-Bold" if os.path.exists(FONT_BOLD) else "NotoSansKR",
    fontSize=18,
    leading=22,
    alignment=TA_LEFT,
)
STYLE_H2 = ParagraphStyle(
    "h2",
    parent=styles["Heading2"],
    fontName="NotoSansKR-Bold" if os.path.exists(FONT_BOLD) else "NotoSansKR",
    fontSize=13,
    leading=18,
)


# OpenAI (new style)
from openai import OpenAI

# Supabase
from supabase import create_client


# -----------------------------
# 0) Streamlit page config (must be first)
# -----------------------------
st.set_page_config(page_title="승인형 성실신고/법인전환 납품 PDF 엔진", layout="wide")


# -----------------------------
# 1) Secrets / ENV
# -----------------------------
def sget(key: str, default: Optional[str] = None) -> Optional[str]:
    # Streamlit Cloud secrets first, then env
    if hasattr(st, "secrets") and key in st.secrets:
        v = st.secrets.get(key)
        return str(v) if v is not None else default
    return os.getenv(key, default)

SUPABASE_URL = sget("SUPABASE_URL")
SUPABASE_KEY = sget("SUPABASE_KEY")  # service role 권장
ADMIN_EMAIL = (sget("ADMIN_EMAIL") or "").strip().lower()
ADMIN_BOOTSTRAP_KEY = (sget("ADMIN_BOOTSTRAP_KEY") or "").strip()
OPENAI_API_KEY = (sget("OPENAI_API_KEY") or "").strip()

DAILY_LIMIT = 5
MONTHLY_LIMIT = 100


# -----------------------------
# 2) Helpers
# -----------------------------
def now_kr() -> dt.datetime:
    # KST fixed
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def period_keys(now: dt.datetime) -> Tuple[str, str]:
    return now.strftime("%Y-%m-%d"), now.strftime("%Y-%m")

def is_secrets_ok() -> Tuple[bool, str]:
    missing = []
    if not SUPABASE_URL: missing.append("SUPABASE_URL")
    if not SUPABASE_KEY: missing.append("SUPABASE_KEY")
    if not ADMIN_EMAIL: missing.append("ADMIN_EMAIL")
    if not ADMIN_BOOTSTRAP_KEY: missing.append("ADMIN_BOOTSTRAP_KEY")
    if not OPENAI_API_KEY: missing.append("OPENAI_API_KEY")
    if missing:
        return False, "Secrets 설정이 부족합니다. 누락: " + ", ".join(missing)
    return True, ""

def get_sb():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def audit(sb, email: Optional[str], event_type: str, detail: Dict[str, Any]):
    try:
        sb.table("audit_logs").insert({
            "email": email,
            "event_type": event_type,
            "detail": detail
        }).execute()
    except Exception:
        pass  # 로깅 실패는 앱 중단 사유 아님

def upsert_user(sb, email: str, approved: bool = False, is_admin: bool = False):
    email = email.lower().strip()
    sb.table("users").upsert({
        "email": email,
        "approved": approved,
        "is_admin": is_admin,
        "updated_at": dt.datetime.utcnow().isoformat()
    }, on_conflict="email").execute()

def get_user(sb, email: str) -> Optional[Dict[str, Any]]:
    email = email.lower().strip()
    res = sb.table("users").select("*").eq("email", email).limit(1).execute()
    if res.data:
        return res.data[0]
    return None

def set_approval(sb, email: str, approved: bool):
    email = email.lower().strip()
    sb.table("users").update({
        "approved": approved,
        "updated_at": dt.datetime.utcnow().isoformat()
    }).eq("email", email).execute()

def set_admin(sb, email: str, is_admin: bool):
    email = email.lower().strip()
    sb.table("users").update({
        "is_admin": is_admin,
        "updated_at": dt.datetime.utcnow().isoformat()
    }).eq("email", email).execute()

def list_users(sb) -> pd.DataFrame:
    res = sb.table("users").select("*").order("created_at", desc=True).execute()
    return pd.DataFrame(res.data or [])

def usage_get(sb, email: str, period_type: str, period_key: str) -> int:
    res = sb.table("usage_counters").select("count").eq("email", email).eq("period_type", period_type).eq("period_key", period_key).limit(1).execute()
    if res.data:
        return int(res.data[0]["count"] or 0)
    return 0

def usage_can(sb, email: str) -> Tuple[bool, Dict[str, int]]:
    nk = now_kr()
    dkey, mkey = period_keys(nk)
    d = usage_get(sb, email, "daily", dkey)
    m = usage_get(sb, email, "monthly", mkey)
    ok = (d < DAILY_LIMIT) and (m < MONTHLY_LIMIT)
    return ok, {"daily": d, "monthly": m, "daily_left": max(0, DAILY_LIMIT - d), "monthly_left": max(0, MONTHLY_LIMIT - m)}

def usage_inc(sb, email: str):
    """
    중복키 절대 안 남: unique(email, period_type, period_key) 기반 upsert로 증가
    """
    nk = now_kr()
    dkey, mkey = period_keys(nk)

    def inc(period_type: str, period_key: str):
        current = usage_get(sb, email, period_type, period_key)
        sb.table("usage_counters").upsert({
            "email": email,
            "period_type": period_type,
            "period_key": period_key,
            "count": current + 1,
            "updated_at": dt.datetime.utcnow().isoformat()
        }, on_conflict="email,period_type,period_key").execute()

    inc("daily", dkey)
    inc("monthly", mkey)


# -----------------------------
# 3) Excel parsing (업종코드/소득율 계산)
# -----------------------------
@dataclass
class IncomeRateResult:
    industry_code: str
    biz_code: Optional[str]
    q_value: Optional[float]
    income_rate: Optional[float]
    notes: str

def compute_income_rate_from_excel(xlsx: pd.ExcelFile, industry_code: str) -> IncomeRateResult:
    """
    대표님이 쓰던 로직:
    - F열에서 산업분류코드 찾기
    - 해당 행의 C열 = 업종코드
    - K열에서 업종코드 찾기
    - 해당 행의 Q열 = Q값
    - 소득율 = 100 - Q값
    """
    industry_code = str(industry_code).strip()

    # 첫 시트 사용(대표님 파일 구조가 시트 1개인 경우가 많음)
    df = pd.read_excel(xlsx, sheet_name=0)

    # 컬럼을 엑셀 열문자 기준으로 맞추기: C,F,K,Q는 3,6,11,17(1-based)
    # pandas는 0-based, 따라서 C=2, F=5, K=10, Q=16
    try:
        colC = df.columns[2]
        colF = df.columns[5]
        colK = df.columns[10]
        colQ = df.columns[16]
    except Exception:
        return IncomeRateResult(industry_code, None, None, None, "엑셀 컬럼 구조(C/F/K/Q)가 예상과 다릅니다. 업로드한 파일을 확인해주세요.")

    # F에서 찾기
    hit = df[df[colF].astype(str).str.strip() == industry_code]
    if hit.empty:
        return IncomeRateResult(industry_code, None, None, None, f"F열에서 산업분류코드({industry_code})를 찾지 못했습니다.")

    biz_code = str(hit.iloc[0][colC]).strip()

    # K에서 biz_code 찾기
    hit2 = df[df[colK].astype(str).str.strip() == biz_code]
    if hit2.empty:
        return IncomeRateResult(industry_code, biz_code, None, None, f"K열에서 업종코드({biz_code})를 찾지 못했습니다.")

    try:
        qv = float(hit2.iloc[0][colQ])
        income_rate = 100.0 - qv
    except Exception:
        return IncomeRateResult(industry_code, biz_code, None, None, "Q값 변환에 실패했습니다(숫자인지 확인).")

    return IncomeRateResult(industry_code, biz_code, qv, income_rate, "OK")


# -----------------------------
# 4) Consulting calculation (5-year simulation)
# -----------------------------
@dataclass
class SimRow:
    year: int
    sales: float
    profit_rate: float
    profit: float
    est_tax_personal: float
    est_tax_corp: float
    est_health: float
    delta: float

def estimate_personal_tax(profit: float) -> float:
    # 매우 단순화된 추정(납품용: “추정치” 명시)
    # 누진을 대충 곡선화. (현실 세법 완전일치 아님)
    if profit <= 0:
        return 0.0
    # 8%~35% 사이로 완만하게 증가
    rate = min(0.35, 0.08 + (profit / 500_000_000) * 0.12)
    return profit * rate

def estimate_corp_tax(profit: float) -> float:
    if profit <= 0:
        return 0.0
    # 9%~19% 수준 단순화
    rate = 0.09 if profit <= 200_000_000 else 0.19
    return profit * rate

def estimate_health(profit: float, is_regional: bool) -> float:
    if profit <= 0:
        return 0.0
    # 지역가입자일 때 부담이 커지도록 단순 추정
    base = 0.07 if is_regional else 0.04
    return profit * base

def build_5y_sim(sales: float, profit_rate: float, is_regional: bool) -> pd.DataFrame:
    rows = []
    for i in range(5):
        y = now_kr().year + i
        # 매출 연 4% 성장 가정(납품용 기본값)
        s = sales * ((1.04) ** i)
        p = s * (profit_rate / 100.0)
        t_p = estimate_personal_tax(p)
        t_c = estimate_corp_tax(p)
        h = estimate_health(p, is_regional)
        # “개인 대비 법인 전환 시 절감 잠재” 단순 delta(세금+건보 차이 중심)
        delta = (t_p + h) - (t_c + (h * 0.6))  # 법인전환 후 건보부담 일부 완화 가정
        rows.append({
            "연도": y,
            "매출(원)": round(s),
            "소득률(%)": profit_rate,
            "추정 순이익(원)": round(p),
            "개인 추정세금(원)": round(t_p),
            "법인 추정법인세(원)": round(t_c),
            "추정 건강보험(원)": round(h),
            "절감잠재(원)": round(delta),
        })
    return pd.DataFrame(rows)


# -----------------------------
# 5) OpenAI text generation
# -----------------------------
def gen_consulting_text(payload: Dict[str, Any]) -> str:
    client = OpenAI(api_key=OPENAI_API_KEY)

    # “영업용 납품” 문체로 강하게
    system = (
        "너는 대한민국 중소기업 세무/재무 컨설팅 전문가다. "
        "사용자에게 납품되는 컨설팅 보고서 문장을 작성한다. "
        "문장은 과장 없이 '추정/가정'을 명확히 표시하되, 설득력 있게 구조화한다. "
        "반드시: (1) Executive Summary (2) 핵심 리스크 3~5개 (3) 5개년 시뮬레이션 해석 "
        "(4) 법인전환 실행 로드맵 3단계 (5) 상담 유도 문장 을 포함해라."
    )

    user = f"""
[입력 요약]
- 금년 예상 매출: {payload['sales']} 원
- 직원 수(대표 제외): {payload['employees']} 명
- 업종/산업코드: {payload['industry_code']}
- 소득률(%) 추정치: {payload['income_rate']}
- 현재 고민/리스크: {payload['concerns']}
- 대표자 보험유형: {"지역가입자" if payload['is_regional'] else "직장가입자/기타"}

[5개년 시뮬레이션 표]
{payload['sim_table_markdown']}

[작성 톤]
- "승인형 제안서/보고서"처럼 전문적이고 숫자 중심
- 문단 제목을 붙이고, 표를 해석하는 문장을 반드시 포함
- '추정치'임을 문서 곳곳에 명시
- '유형자산의 감가상각'과 '세액공제, 세액감면' 고려 안함 명시
"""

    resp = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.25,
    )
    return resp.output_text


# -----------------------------
# 6) PDF builder (sales-grade)
# -----------------------------
def try_register_korean_font():
    """
    Streamlit Cloud에서는 폰트 파일이 없을 수 있어, 가능한 경우만 등록.
    폰트가 없어도 PDF는 생성되지만 한글이 깨질 수 있음.
    """
    candidates = [
        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                pdfmetrics.registerFont(TTFont("KFont", path))
                return "KFont"
            except Exception:
                pass
    return None

def money(x: float) -> str:
    try:
        return f"{int(x):,}"
    except Exception:
        return str(x)

def df_to_table_data(df: pd.DataFrame):
    return [list(df.columns)] + df.values.tolist()

def build_pdf(
    title: str,
    client_email: str,
    summary_text: str,
    sim_df: pd.DataFrame,
    input_block: Dict[str, Any],
) -> bytes:
    font_name = try_register_korean_font()
    styles = getSampleStyleSheet()
    base = styles["Normal"]

    if font_name:
        base.fontName = font_name
        styles["Heading1"].fontName = font_name
        styles["Heading2"].fontName = font_name
        styles["Heading3"].fontName = font_name

    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=18, spaceAfter=8)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=13, spaceAfter=6)
    p = ParagraphStyle("p", parent=base, fontSize=10.5, leading=15)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=18*mm, rightMargin=18*mm,
        topMargin=18*mm, bottomMargin=18*mm
    )

    story = []

    # Cover
    story.append(Paragraph("대외비 / Confidential", p))
    story.append(Spacer(1, 6))
    story.append(Paragraph(title, h1))
    story.append(Paragraph(f"클라이언트: {client_email}", p))
    story.append(Paragraph(f"작성일: {now_kr().strftime('%Y-%m-%d')}", p))
    story.append(Spacer(1, 12))
    story.append(Paragraph("※ 본 보고서는 입력 정보와 공개 기준에 근거한 ‘추정 분석으로 감가상각과 세액공제,감면’을 고려하지 않았으며, 최종 세무신고/의사결정은 세무전문가 검토가 필요합니다.", p))
    story.append(PageBreak())

    # Executive Summary
    story.append(Paragraph("1) Executive Summary", h2))
    for line in summary_text.split("\n"):
        if line.strip():
            story.append(Paragraph(line.strip(), p))
    story.append(Spacer(1, 10))

    # Inputs
    story.append(Paragraph("2) 입력 정보 요약", h2))
    input_df = pd.DataFrame([{
        "항목": "금년 예상 매출", "값": f"{money(input_block['sales'])} 원"
    },{
        "항목": "직원 수(대표 제외)", "값": f"{input_block['employees']} 명"
    },{
        "항목": "산업분류코드", "값": str(input_block["industry_code"])
    },{
        "항목": "소득률(%)", "값": f"{input_block['income_rate']:.2f} %"
    },{
        "항목": "대표 보험유형", "값": "지역가입자" if input_block["is_regional"] else "직장/기타"
    },{
        "항목": "현재 고민/리스크", "값": str(input_block["concerns"])
    }])
    t = Table(df_to_table_data(input_df), colWidths=[45*mm, 120*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),colors.lightgrey),
        ("GRID",(0,0),(-1,-1),0.3,colors.grey),
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("FONTNAME",(0,0),(-1,-1), font_name or "Helvetica"),
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("BOTTOMPADDING",(0,0),(-1,0),6),
        ("TOPPADDING",(0,0),(-1,0),6),
    ]))
    story.append(t)
    story.append(Spacer(1, 12))

    # 5y sim
    story.append(Paragraph("3) 5개년 시뮬레이션(추정)", h2))
    sim_tbl = Table(df_to_table_data(sim_df), repeatRows=1)
    sim_tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#f2f2f2")),
        ("GRID",(0,0),(-1,-1),0.25,colors.grey),
        ("FONTNAME",(0,0),(-1,-1), font_name or "Helvetica"),
        ("FONTSIZE",(0,0),(-1,-1),8.5),
        ("ALIGN",(1,1),(-1,-1),"RIGHT"),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))
    story.append(sim_tbl)
    story.append(Spacer(1, 10))
    story.append(Paragraph("해석 가이드: ‘절감잠재’는 법인 전환 시 세금구조 변화에 따른 잠재 차이를 단순화하여 산출한 값입니다(실제는 인건비, 대표 급여/배당, 가족종업원, 비용처리 구조에 따라 달라짐).", p))
    story.append(PageBreak())

    # Close
    story.append(Paragraph("4) 결론 및 실행 제안", h2))
    story.append(Paragraph("• 본 보고서 결과를 토대로 ‘전환 타이밍/대표 급여·배당 구조/비용처리 체계/증빙 리스크’를 함께 설계하면 절감 효과와 리스크 관리가 동시에 가능합니다.", p))
    story.append(Paragraph("• 다음 단계(권장): (1) 비용/증빙 점검  (2) 대표자 소득/보험 시뮬레이션 정밀화  (3) 법인 전환 실행 로드맵 확정", p))
    story.append(Spacer(1, 12))
    story.append(Paragraph("상담/납품용 문서이므로, 수치 근거(매출·인건비·원가·고정비)를 업데이트하면 보고서의 설득력이 크게 상승합니다.", p))

    doc.build(story)
    return buf.getvalue()


# -----------------------------
# 7) UI
# -----------------------------
ok, msg = is_secrets_ok()
if not ok:
    st.error(msg)
    st.stop()

sb = get_sb()

st.title("✅ 승인형 성실신고/법인전환 ‘영업용 납품 PDF’ 엔진 (OpenAI + 비용방어)")

# Sidebar login / gate
with st.sidebar:
    st.subheader("🔐 접근 제어")
    email = st.text_input("이메일", value=st.session_state.get("email", "")).strip().lower()

    col1, col2 = st.columns(2)
    if col1.button("로그인"):
        if not email:
            st.warning("이메일을 입력하세요.")
        else:
            st.session_state["email"] = email
            # 첫 사용 자동 생성(승인 false)
            upsert_user(sb, email, approved=False, is_admin=(email == ADMIN_EMAIL))
            audit(sb, email, "login", {"status": "ok"})
            st.success(f"로그인: {email}")

    if col2.button("로그아웃"):
        st.session_state.pop("email", None)
        st.success("로그아웃")

    user = None
    if st.session_state.get("email"):
        user = get_user(sb, st.session_state["email"])
        if user:
            st.markdown(f"**로그인:** {user['email']}")
            st.markdown(f"**승인:** {'✅' if user.get('approved') else '⏳(대기)'}")
            st.markdown(f"**관리자:** {'👑' if user.get('is_admin') else '-'}")

            ok_use, usage = usage_can(sb, user["email"])
            st.divider()
            st.markdown("📌 **사용량(비용 방어)**")
            st.write(f"• 오늘: {usage['daily']} / {DAILY_LIMIT} (잔여 {usage['daily_left']})")
            st.write(f"• 이번달: {usage['monthly']} / {MONTHLY_LIMIT} (잔여 {usage['monthly_left']})")

    st.divider()
    with st.expander("🛠 관리자 초기설정(최초 1회)"):
        st.caption("처음에 관리자 계정을 승인+관리자로 설정합니다.")
        bootstrap_key = st.text_input("ADMIN_BOOTSTRAP_KEY", type="password")
        if st.button("관리자 계정 생성/갱신"):
            if not st.session_state.get("email"):
                st.error("먼저 로그인하세요.")
            elif bootstrap_key != ADMIN_BOOTSTRAP_KEY:
                st.error("키가 일치하지 않습니다.")
            else:
                me = st.session_state["email"]
                upsert_user(sb, me, approved=True, is_admin=True)
                audit(sb, me, "approve", {"target": me, "approved": True, "is_admin": True})
                st.success("관리자 계정을 승인+관리자로 설정했습니다. 새로고침 후 확인하세요.")


# Gate check
if not st.session_state.get("email"):
    st.info("왼쪽 사이드바에서 이메일 로그인 후 진행하세요.")
    st.stop()

user = get_user(sb, st.session_state["email"])
if not user:
    st.error("사용자 정보를 불러오지 못했습니다.")
    st.stop()

# Admin panel
if user.get("is_admin"):
    st.subheader("👑 관리자: 승인/사용자 관리")
    dfu = list_users(sb)
    if not dfu.empty:
        dfu_view = dfu[["email", "approved", "is_admin", "created_at", "updated_at"]].copy()
        st.dataframe(dfu_view, use_container_width=True)

        st.markdown("### 승인/해제")
        c1, c2, c3 = st.columns([2,1,1])
        target = c1.text_input("대상 이메일", value="")
        if c2.button("승인"):
            if target:
                upsert_user(sb, target, approved=True, is_admin=False)
                set_approval(sb, target, True)
                audit(sb, user["email"], "approve", {"target": target, "approved": True})
                st.success(f"승인 완료: {target}")
        if c3.button("승인 해제"):
            if target:
                set_approval(sb, target, False)
                audit(sb, user["email"], "approve", {"target": target, "approved": False})
                st.warning(f"승인 해제: {target}")
    else:
        st.info("현재 users 테이블이 비어 있습니다.")

st.divider()

# Approved gate
if not user.get("approved"):
    st.warning("⏳ 승인 대기 상태입니다. 관리자 승인이 필요합니다.")
    st.stop()

# -----------------------------
# Main: Input + Excel upload
# -----------------------------
st.subheader("1) 입력(엑셀 업로드 + 실시간 계산)")

colA, colB = st.columns([1,1])

with colA:
    sales = st.number_input("금년 예상 매출(원)", min_value=0, value=900_000_000, step=10_000_000, format="%d")
    employees = st.number_input("직원 수(대표 제외)", min_value=0, value=5, step=1)
    industry_code = st.text_input("산업분류코드(숫자 그대로)", value="22232")
    is_regional = st.checkbox("대표자가 지역가입자(건보 리스크 큼)", value=True)
    concerns = st.text_area("현재 고민/리스크(선택)", value="성실신고, 건강보험료, 세무조사 리스크")

with colB:
    st.markdown("#### 업종코드 엑셀 업로드")
    uploaded_file = st.file_uploader("업종코드 엑셀 업로드(.xlsx)", type=["xlsx"])
    income_rate = None
    ir_notes = ""
    if uploaded_file is not None:
        try:
            xlsx = pd.ExcelFile(uploaded_file)
            ir = compute_income_rate_from_excel(xlsx, industry_code)
            ir_notes = ir.notes
            if ir.income_rate is not None:
                income_rate = float(ir.income_rate)
                st.success(f"소득율(%) 계산 완료: {income_rate:.2f}% (Q={ir.q_value}, 업종코드={ir.biz_code})")
            else:
                st.error(f"소득율 계산 실패: {ir_notes}")
        except Exception as e:
            st.error(f"엑셀 처리 오류: {e}")
    else:
        st.info("엑셀 업로드 시 산업분류코드 기반으로 소득율을 자동 계산합니다.")

# fallback if no excel
if income_rate is None:
    income_rate = st.number_input("소득률(%) 수동 입력(엑셀 없을 때)", min_value=0.0, max_value=100.0, value=12.0, step=0.1)

sim_df = build_5y_sim(float(sales), float(income_rate), bool(is_regional))
st.markdown("#### 5개년 시뮬레이션(미리보기)")
st.dataframe(sim_df, use_container_width=True)

st.divider()
st.subheader("2) 보고서 생성(승인된 사용자만 / 사용량 제한 적용)")

ok_use, usage = usage_can(sb, user["email"])
if not ok_use:
    st.error(f"사용량 초과입니다. 오늘 잔여 {usage['daily_left']}회 / 이번달 잔여 {usage['monthly_left']}회")
    st.stop()

tone = st.selectbox("문서 톤", ["전문적/숫자중심/리스크체감형", "임팩트 강한 영업형(과장 없이)", "조용한 프리미엄형(고급 보고서)"])

btn = st.button("🚀 영업용 납품 PDF 생성(OpenAI)", use_container_width=True)

if btn:
    payload = {
        "sales": int(sales),
        "employees": int(employees),
        "industry_code": str(industry_code),
        "income_rate": float(income_rate),
        "concerns": str(concerns),
        "is_regional": bool(is_regional),
        "sim_table_markdown": sim_def df_to_pdf_table(df, max_rows=30):
    df2 = df.head(max_rows).copy()

    # 표 데이터 (헤더 + 행)
    data = [list(df2.columns)] + df2.astype(str).values.tolist()

    # 컬럼 너비(대충 자동) - 필요하면 수동 조정
    col_count = len(df2.columns)
    total_width = 180 * mm
    col_widths = [total_width / col_count] * col_count

    t = Table(data, colWidths=col_widths, repeatRows=1)

    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), "NotoSansKR"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F2F2F2")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("ALIGN", (0, 1), (-1, -1), "RIGHT"),
        ("LEFTPADDING", (0,0), (-1,-1), 4),
        ("RIGHTPADDING", (0,0), (-1,-1), 4),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ]))
    return t
,
        "tone": tone
    }

    # OpenAI 호출 성공 시에만 차감(비용 방어)
    try:
        summary = gen_consulting_text(payload)
        audit(sb, user["email"], "openai_ok", {"tone": tone})
    except Exception as e:
        audit(sb, user["email"], "openai_fail", {"err": str(e)})
        st.error(f"OpenAI 호출 실패: {e}")
        st.stop()

    # usage inc AFTER success
    try:
        usage_inc(sb, user["email"])
    except Exception as e:
        # usage 실패해도 PDF는 만들어주되, 관리자 로그 남김
        audit(sb, user["email"], "usage_fail", {"err": str(e)})

    # PDF 생성
    try:
        pdf_bytes = build_pdf(
            title="개인사업자 성실신고 리스크 & 법인전환 납품 컨설팅 보고서(추정)",
            client_email=user["email"],
            summary_text=summary,
            sim_df=sim_df,
            input_block=payload
        )
        audit(sb, user["email"], "pdf_ok", {"size": len(pdf_bytes)})
    except Exception as e:
        audit(sb, user["email"], "pdf_fail", {"err": str(e)})
        st.error(f"PDF 생성 실패: {e}")
        st.stop()

    st.success("PDF 생성 완료!")
    filename = f"컨설팅_보고서_{user['email'].split('@')[0]}_{now_kr().strftime('%Y%m%d_%H%M')}.pdf"
    st.download_button("⬇️ PDF 다운로드", data=pdf_bytes, file_name=filename, mime="application/pdf")
