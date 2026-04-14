import streamlit as st
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import BytesIO

st.set_page_config(page_title="Pipeline Cleaner", layout="centered")
st.title("🧹 Smartsheets Pipeline Cleaner")
st.caption("Upload SA + UK Smartsheet exports → get one clean combined file")

# ── Constants ────────────────────────────────────────────────────────────────
EXCLUDE_REPS   = ['mariska', 'dylan', 'chris', 'justin']
EXCLUDE_STAGES = ['8 - on hold']

MONTHS = ['january','february','march','april','may','june',
          'july','august','september','october','november','december']
MONTH_ABBR = {m[:3]: m for m in MONTHS}

def get_invoice_window():
    now = datetime.today().replace(day=1)
    return [(now + relativedelta(months=i)).strftime('%B').lower() for i in range(-1, 7)]

def parse_invoice_month(raw):
    import re
    if raw is None or (isinstance(raw, float) and pd.isna(raw)): return None
    s = str(raw).strip().lower()
    if not s: return None
    m = re.match(r'^(\d{4})-(\d{1,2})', s)
    if m:
        i = int(m.group(2))
        if 1 <= i <= 12: return MONTHS[i-1]
    m = re.match(r'^(\d{1,2})/(\d{4})', s)
    if m:
        i = int(m.group(1))
        if 1 <= i <= 12: return MONTHS[i-1]
    for full in MONTHS:
        if full in s: return full
    for abbr, full in MONTH_ABBR.items():
        if re.search(r'\b' + abbr + r'\b', s): return full
    return None

def parse_money(val):
    if pd.isna(val): return 0.0
    try: return float(str(val).replace('R','').replace(',','').replace(' ','').strip())
    except: return 0.0

def parse_prob(val):
    if pd.isna(val): return 0.0
    s = str(val).replace('%','').replace(' ','').strip()
    try: v = float(s)
    except: return 0.0
    return v/100 if v > 1 else v

def load_sa(df):
    out = pd.DataFrame(index=df.index)
    out['Region']            = 'SA'
    out['Company Name']      = df.get('Company Name', '')
    out['Sales Stage']       = df.get('Sales Stage', '').str.strip()
    out['Invoice Month']     = df.get('1st Invoice Month', '').apply(parse_invoice_month)
    out['Recurring']         = df.get('Deal Size - Monthly Recurring', 0).apply(parse_money)
    out['Once Off']          = df.get('Deal Size - Once Off', 0).apply(parse_money)
    out['Win Probability']   = df.get('Win Probability', 0).apply(parse_prob)
    out['Weighted Recurring']= df.get('Weighted Deal - Monthly Recurring', 0).apply(parse_money)
    out['Weighted Once Off'] = df.get('Weighted Deal - Once off', 0).apply(parse_money)
    out['Sales Rep']         = df.get('Sales Rep', '').str.strip()
    out['Lead Source']       = df.get('Lead Source', '')
    out['Service Category']  = df.get('Service Category', '')
    out['Product']           = df.get('Product Tags', '')
    out['Sales Duration']    = pd.to_numeric(df.get('Duration (Temp)', None), errors='coerce')
    out['Notes']             = df.get('Notes', '')
    out['Expected Close Date']= df.get('Expected Close Date', '')
    out['Created']           = df.get('Created', '')
    consumed = {'Company Name','Sales Stage','1st Invoice Month',
        'Deal Size - Monthly Recurring','Deal Size - Once Off','Win Probability',
        'Weighted Deal - Monthly Recurring','Weighted Deal - Once off',
        'Sales Rep','Lead Source','Service Category','Product Tags',
        'Duration (Temp)','Notes','Expected Close Date','Created'}
    for col in df.columns:
        if col not in consumed and col not in out.columns:
            out[col] = df[col]
    return out

def load_uk(df):
    ecd = pd.to_datetime(df.get('Expected Close Date'), errors='coerce')
    today = pd.Timestamp(datetime.today().date())
    keep = ecd.notna() & (ecd >= today)
    df = df[keep].copy()
    ecd = ecd[keep]
    out = pd.DataFrame(index=df.index)
    out['Region']            = 'UK'
    out['Company Name']      = df.get('Company Name', '')
    out['Sales Stage']       = df.get('Sales Stage', '').str.strip()
    out['Invoice Month']     = ecd.dt.strftime('%B').str.lower()
    out['Recurring']         = df.get('Deal Size - Monthly Recurring', 0).apply(parse_money)
    out['Once Off']          = df.get('Deal Size - Once Off', 0).apply(parse_money)
    out['Win Probability']   = df.get('Won Probability', 0).apply(parse_prob)
    out['Weighted Recurring']= df.get('Weighted Deal - Monthly Recurring', 0).apply(parse_money)
    out['Weighted Once Off'] = df.get('Weighted Deal - Once off', 0).apply(parse_money)
    out['Sales Rep']         = df.get('Sales Rep', '').str.strip()
    out['Lead Source']       = df.get('Lead Source', '')
    out['Service Category']  = df.get('Service Categoery', df.get('Service Category', ''))
    out['Product']           = df.get('Core Product', '')
    out['Sales Duration']    = None
    out['Notes']             = df.get('Notes', '')
    out['Expected Close Date']= df.get('Expected Close Date', '')
    out['Created']           = df.get('Created', '')
    consumed = {'Company Name','Sales Stage','Invoice Month',
        'Deal Size - Monthly Recurring','Deal Size - Once Off','Won Probability',
        'Weighted Deal - Monthly Recurring','Weighted Deal - Once off',
        'Sales Rep','Lead Source','Service Categoery','Service Category',
        'Core Product','Notes','Expected Close Date','Created'}
    for col in df.columns:
        if col not in consumed and col not in out.columns:
            out[col] = df[col]
    return out

def apply_filters(df, window):
    df = df[~df['Sales Stage'].fillna('').str.lower().isin(EXCLUDE_STAGES)]
    df = df[~df['Sales Rep'].fillna('').str.lower().str.split().str[0].isin(EXCLUDE_REPS)]
    df = df[df['Invoice Month'].isin(window)]
    return df

# ── UI ───────────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)
sa_file = col1.file_uploader("SA Pipeline (xlsx)", type=['xlsx','xls'])
uk_file = col2.file_uploader("UK Pipeline (xlsx)", type=['xlsx','xls'])

window = get_invoice_window()
st.info(f"📅 Invoice window: **{window[0]} → {window[-1]}** (current month -1 to +6)")

if not sa_file or not uk_file:
    st.stop()

sa_raw = pd.read_excel(sa_file)
uk_raw = pd.read_excel(uk_file)

sa = load_sa(sa_raw)
uk = load_uk(uk_raw)
combined = pd.concat([sa, uk], ignore_index=True)
cleaned  = apply_filters(combined, window)

# ── Preview ──────────────────────────────────────────────────────────────────
st.markdown("---")
c1, c2, c3 = st.columns(3)
c1.metric("Total rows", len(cleaned))
c2.metric("SA", len(cleaned[cleaned['Region']=='SA']))
c3.metric("UK", len(cleaned[cleaned['Region']=='UK']))

st.dataframe(cleaned, use_container_width=True, hide_index=True)

# ── Download ─────────────────────────────────────────────────────────────────
st.markdown("---")
fname = f"pipeline_cleaned_{datetime.today().strftime('%Y%m%d')}"

col_csv, col_xlsx = st.columns(2)

csv_data = cleaned.to_csv(index=False).encode('utf-8')
col_csv.download_button("⬇️ Download CSV", csv_data, f"{fname}.csv", "text/csv")

buf = BytesIO()
with pd.ExcelWriter(buf, engine='openpyxl') as w:
    cleaned.to_excel(w, index=False, sheet_name='Pipeline')
col_xlsx.download_button("⬇️ Download Excel", buf.getvalue(), f"{fname}.xlsx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
