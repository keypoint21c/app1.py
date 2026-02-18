# app1.py (Sales-grade Consulting PDF Engine)
import os
import io
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple

import pandas as pd
import streamlit as st

# OpenAI (new style)
from openai import OpenAI

# Supabase
from supabase import create_client

# PDF (ReportLab)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm


# -----------------------------
# 0) Streamlit page config (must be first)
# -----------------------------
st.set_page_config(page_title="승인형 성실신고/법인전환 납품 PDF 엔진", layout="wide")


# -----------------------------
# 1) Secrets / ENV
# -----------------------------
def sget(key: str, default: Optional[str] = None) -> Optional[str]:
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
        pass


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
    return res.data[0] if res.data else None


def set_approval(sb, email: str, approved: bool):
    email = email.lower().strip()
    sb.table("users").update({
        "approved": approved,
        "updated_at": dt.datetime.utcnow().isoformat()
    }).eq("email", email).execute()


def list_users(sb) -> pd.DataFrame:
    # created_at 컬럼이 없는 경우가 많아서 "정렬 없이" 가져옵니다.
    res = sb.table("users").select("*").execute()
    return pd.DataFrame(res.data or [])


def usage_get(sb, email: str, period_type: str, period_key: str) -> int:
    res = (
        sb.table("usage_counters")
        .select("count")
        .eq("email", email)
        .eq("period_type", period_type)
        .eq("period_key", period_key)
        .limit(1)
        .execute()
    )
    if res.data:
        return int(res.data[0].get("count") or 0)
    return 0


def usage_can(sb, email: str) -> Tuple[bool, Dict[str, int]]:
    nk = now_kr()
    dkey, mkey = period_keys(nk)
    d = usage_get(sb, email, "daily", dkey)
    m = usage_get(sb, email, "monthly", mkey)
    ok = (d < DAILY_LIMIT) and (m < MONTHLY_LIMIT)
    return ok, {
        "daily": d, "monthly": m,
        "daily_left": max(0, DAILY_LIMIT - d),
        "monthly_left": max(0, MONTHLY_LIMIT - m),
        "dkey": dkey, "mkey": mkey
    }


def usage_inc(sb, email: str):
    """
    중복키 절대 안 남: unique(email, period_type, period_key) 기반 upsert
    (성공시에만 호출)
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


def compute_income_rate_from_excel(uploaded_bytes: bytes, industry_code: str) -> IncomeRateResult:
    """
    - F열에서 산업분류코드 찾기
    - 해당 행의 C열 = 업종코드
    - K열에서 업종코드 찾기
    - 해당 행의 Q열 = Q값
    - 소득율 = 100 - Q값
    """
    industry_code = str(industry_code).strip()
    xlsx = pd.ExcelFile(io.BytesIO(uploaded_bytes))
    df = pd.read_excel(xlsx, sheet_name=0)

    try:
        colC = df.columns[2]
        colF = df.columns[5]
        colK = df.columns[10]
        colQ = df.columns[16]
    except Exception:
        return IncomeRateResult(industry_code, None, None, None, "엑셀 컬럼 구조(C/F/K/Q)가 예상과 다릅니다. 업로드한 파일을 확인해주세요.")

    hit = df[df[colF].astype(str).str.strip() == industry_code]
    if hit.empty:
        return IncomeRateResult(industry_code, None, None, None, f"F열에서 산업분류코드({industry_code})를 찾지 못했습니다.")

    biz_code = str(hit.iloc[0][colC]).strip()

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
def estimate_personal_tax(profit: float) -> float:
    if profit <= 0:
        return 0.0
    rate = min(0.35, 0.08 + (profit / 500_000_000) * 0.12)
    return profit * rate


def estimate_corp_tax(profit: float) -> float:
    if profit <= 0:
        return 0.0
    rate = 0.09 if profit <= 200_000_000 else 0.19
    return profit * rate


def estimate_health(profit: float, is_regional: bool) -> float:
    if profit <= 0:
        return 0.0
    base = 0.07 if is_regional else 0.04
    return profit * base


def build_5y_sim(sales: float, profit_rate: float, is_regional: bool) -> pd.DataFrame:
    rows = []
    base_year = now_kr().year
    for i in range(5):
        y = base_year + i
        s = sales * ((1.04) ** i)  # 연 4% 성장 가정
        p = s * (profit_rate / 100.0)
        t_p = estimate_personal_tax(p)
        t_c = estimate_corp_tax(p)
        h = estimate_health(p, is_regional)
        delta = (t_p + h) - (t_c + (h * 0.6))
        rows.append({
            "연도": y,
            "매출(원)": round(s),
            "소득률(%)": round(profit_rate, 2),
            "추정 순이익(원)": round(p),
            "개인 추정세금(원)": round(t_p),
            "법인 추정법인세(원)": round(t_c),
            "추정 건강보험(원)": round(h),
            "절감잠재(원)": round(delta),
        })
    return pd.DataFrame(rows)


# -----------------------------
# 5) OpenAI text generation (sales-grade narrative)
# -----------------------------
def gen_consulting_text(payload: Dict[str, Any]) -> str:
    client = OpenAI(api_key=OPENAI_API_KEY)

    system = (
        "너는 대한민국 중소기업 세무/재무 컨설팅 전문가다. "
        "사용자에게 납품되는 영업용 컨설팅 보고서 문장을 작성한다. "
        "과장 없이 '추정/가정'을 명확히 표시하되, 설득력 있게 구조화한다. "
        "반드시 포함: "
        "(1) Executive Summary "
        "(2) 성실신고(세무조사/증빙/인건비/경비율) 리스크 3~5개 "
        "(3) 5개년 시뮬레이션 해석(연도별 핵심 포인트) "
        "(4) 법인전환 실행 로드맵 3단계 "
        "(5) 상담 유도 문장"
    )

    # tabulate 없이 텍스트로 안전하게
    sim_text = payload["sim_df"].to_string(index=False)

    user = f"""
[입력 요약]
- 금년 예상 매출: {payload['sales']:,} 원
- 직원 수(대표 제외): {payload['employees']} 명
- 산업분류코드: {payload['industry_code']}
- 소득률(%) 추정치: {payload['income_rate']:.2f}%
- 현재 고민/리스크: {payload['concerns']}
- 대표자 보험유형: {"지역가입자" if payload['is_regional'] else "직장가입자/기타"}
- 문서 톤: {payload['tone']}

[5개년 시뮬레이션 표(추정)]
{sim_text}

[필수 고지]
- 본 보고서는 입력정보 기반 추정이며 감가상각/세액공제/세액감면은 고려하지 않음
- 최종 의사결정은 세무전문가 검토 필요
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
# 6) PDF Engine (한글 폰트 임베드 + Table 정렬)
# -----------------------------
def register_korean_fonts():
    """
    1순위: 레포의 fonts/NotoSansKR-*.ttf
    2순위: 시스템 폰트(있으면)
    """
    base_dir = os.path.dirname(__file__)
    font_reg = os.path.join(base_dir, "fonts", "NotoSansKR-Regular.ttf")
    font_bold = os.path.join(base_dir, "fonts", "NotoSansKR-Bold.ttf")

    chosen_reg = None
    chosen_bold = None

    # repo fonts 우선
    if os.path.exists(font_reg):
        chosen_reg = font_reg
    if os.path.exists(font_bold):
        chosen_bold = font_bold

    # fallback candidates (환경 따라 다름)
    if chosen_reg is None:
        candidates = [
            "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        ]
        for p in candidates:
            if os.path.exists(p):
                chosen_reg = p
                break

    if chosen_reg:
        try:
            pdfmetrics.registerFont(TTFont("KFONT", chosen_reg))
        except Exception:
            pass

    if chosen_bold:
        try:
            pdfmetrics.registerFont(TTFont("KFONT_B", chosen_bold))
        except Exception:
            pass

    # 등록 성공 여부
    has_reg = "KFONT" in pdfmetrics.getRegisteredFontNames()
    has_bold = "KFONT_B" in pdfmetrics.getRegisteredFontNames()
    return has_reg, has_bold


def money(x) -> str:
    try:
        return f"{int(x):,}"
    except Exception:
        return str(x)


def df_to_pdf_table(df: pd.DataFrame, font_name: str, total_width_mm: float = 180.0, max_rows: int = 30) -> Table:
    df2 = df.head(max_rows).copy()
    data = [list(df2.columns)] + df2.astype(str).values.tolist()

    col_count = len(df2.columns)
    total_width = total_width_mm * mm
    col_widths = [total_width / col_count] * col_count

    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), font_name),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F2F2F2")),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    return t


def build_pdf(
    title: str,
    client_email: str,
    summary_text: str,
    sim_df: pd.DataFrame,
    input_block: Dict[str, Any],
) -> bytes:
    has_reg, has_bold = register_korean_fonts()
    font_body = "KFONT" if has_reg else "Helvetica"
    font_bold = "KFONT_B" if has_bold else font_body

    styles = getSampleStyleSheet()
    STYLE_BODY = ParagraphStyle(
        "body",
        parent=styles["Normal"],
        fontName=font_body,
        fontSize=10.5,
        leading=15,
    )
    STYLE_H1 = ParagraphStyle(
        "h1",
        parent=styles["Heading1"],
        fontName=font_bold,
        fontSize=18,
        leading=22,
        alignment=TA_LEFT,
        spaceAfter=8,
    )
    STYLE_H2 = ParagraphStyle(
        "h2",
        parent=styles["Heading2"],
        fontName=font_bold,
        fontSize=13,
        leading=18,
        spaceAfter=6,
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=18*mm, rightMargin=18*mm,
        topMargin=18*mm, bottomMargin=18*mm
    )

    story = []

    # Cover
    story.append(Paragraph("대외비 / Confidential", STYLE_BODY))
    story.append(Spacer(1, 6))
    story.append(Paragraph(title, STYLE_H1))
    story.append(Paragraph(f"클라이언트: {client_email}", STYLE_BODY))
    story.append(Paragraph(f"작성일: {now_kr().strftime('%Y-%m-%d')}", STYLE_BODY))
    story.append(Spacer(1, 12))
    story.append(Paragraph(
        "※ 본 보고서는 입력 정보와 공개 기준에 근거한 ‘추정 분석’입니다. "
        "감가상각, 세액공제/감면 등은 고려하지 않았으며, 최종 신고/의사결정은 세무전문가 검토가 필요합니다.",
        STYLE_BODY
    ))
    story.append(PageBreak())

    # Executive Summary
    story.append(Paragraph("1) Executive Summary", STYLE_H2))
    for line in summary_text.split("\n"):
        if line.strip():
            story.append(Paragraph(line.strip().replace("•", "&bull;"), STYLE_BODY))
    story.append(Spacer(1, 10))

    # Inputs Table
    story.append(Paragraph("2) 입력 정보 요약", STYLE_H2))
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

    t_in = Table([list(input_df.columns)] + input_df.values.tolist(), colWidths=[45*mm, 120*mm], repeatRows=1)
    t_in.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#F2F2F2")),
        ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#CCCCCC")),
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("FONTNAME",(0,0),(-1,-1), font_body),
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("BOTTOMPADDING",(0,0),(-1,0),6),
        ("TOPPADDING",(0,0),(-1,0),6),
    ]))
    story.append(t_in)
    story.append(Spacer(1, 12))

    # 5y sim Table
    story.append(Paragraph("3) 5개년 시뮬레이션(추정)", STYLE_H2))
    story.append(df_to_pdf_table(sim_df, font_body, total_width_mm=180.0, max_rows=20))
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "해석 가이드: ‘절감잠재’는 법인 전환 시 세금구조 변화에 따른 잠재 차이를 단순화하여 산출한 값입니다. "
        "실제 절감/부담은 급여·배당 구조, 비용처리, 인건비, 원가, 증빙관리 수준에 따라 달라질 수 있습니다.",
        STYLE_BODY
    ))
    story.append(PageBreak())

    # Close
    story.append(Paragraph("4) 결론 및 실행 제안", STYLE_H2))
    story.append(Paragraph(
        "• 권장 다음 단계: (1) 비용/증빙 점검 (2) 대표자 소득·건보 정밀 시뮬레이션 (3) 법인전환 실행 로드맵 확정",
        STYLE_BODY
    ))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "• 납품용 문서 품질을 높이려면 실제 원가/고정비/인건비/대표 급여·배당 구조를 반영한 재산정이 가장 효과적입니다.",
        STYLE_BODY
    ))

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
    if not dfu.empty and "email" in dfu.columns:
        show_cols = [c for c in ["email", "approved", "is_admin", "created_at", "updated_at"] if c in dfu.columns]
        st.dataframe(dfu[show_cols], use_container_width=True)

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

colA, colB = st.columns([1, 1])

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
    if uploaded_file is not None:
        try:
            ir = compute_income_rate_from_excel(uploaded_file.getvalue(), industry_code)
            if ir.income_rate is not None:
                income_rate = float(ir.income_rate)
                st.success(f"소득율(%) 계산 완료: {income_rate:.2f}% (Q={ir.q_value}, 업종코드={ir.biz_code})")
            else:
                st.error(f"소득율 계산 실패: {ir.notes}")
        except Exception as e:
            st.error(f"엑셀 처리 오류: {e}")
    else:
        st.info("엑셀 업로드 시 산업분류코드 기반으로 소득율을 자동 계산합니다.")

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

tone = st.selectbox("문서 톤", [
    "전문적/숫자중심/리스크체감형",
    "임팩트 강한 영업형(과장 없이)",
    "조용한 프리미엄형(고급 보고서)"
])

btn = st.button("🚀 영업용 납품 PDF 생성(OpenAI)", use_container_width=True)

if btn:
    payload = {
        "sales": int(sales),
        "employees": int(employees),
        "industry_code": str(industry_code),
        "income_rate": float(income_rate),
        "concerns": str(concerns),
        "is_regional": bool(is_regional),
        "sim_df": sim_df,   # OpenAI 프롬프트용
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

