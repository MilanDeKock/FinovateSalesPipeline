import streamlit as st
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import BytesIO

st.set_page_config(page_title="Pipeline Cleaner", layout="centered")
st.title("🧹 Pipeline Cleaner")
st.caption("Upload SA + UK Smartsheet exports → get one clean combined file")

# ── Constants ────────────────────────────────────────────────────────────────
EXCLUDE_REPS   = ['mariska', 'dylan', 'chris', 'justin']
EXCLUDE_STAGES = ['8 - on hold']

def get_invoice_window():
    now = datetime.today().replace(day=1)
    return [(now + relativedelta(months=i)).strftime('%Y-%m') for i in range(-1, 7)]

MONTH_MAP = {
    'jan':'01','feb':'02','mar':'03','apr':'04','may':'05','jun':'06',
    'jul':'07','aug':'08','sep':'09','oct':'10','nov':'11','dec':'12',
    'january':'01','february':'02','march':'03','april':'04','june':'06',
    'july':'07','august':'08','september':'09','october':'10','november':'11','december':'12'
}

def parse_invoice_month(raw):
    import re
    if not raw or pd.isna(raw): return None
    raw = str(raw).strip()
    m = re.match(r'^([A-Za-z]+)[-\s](\d{2,4})
, raw)
    if m:
        mo = MONTH_MAP.get(m.group(1).lower())
        yr = '20' + m.group(2) if len(m.group(2)) == 2 else m.group(2)
        if mo: return f'{yr}-{mo}'
    if re.match(r'^\d{4}-\d{2}
, raw): return raw
    m2 = re.match(r'^(\d{1,2})/(\d{4})
, raw)
    if m2: return f'{m2.group(2)}-{m2.group(1).zfill(2)}'
    return None

def parse_money(val):
    if pd.isna(val): return 0.0
    try: return float(str(val).replace('R','').replace(',','').replace(' ','').strip())
    except: return 0.0

def load_sa(df):
    out = pd.DataFrame()
    out['Region']            = 'SA'
    out['Company Name']      = df.get('Company Name', '')
    out['Sales Stage']       = df.get('Sales Stage', '').str.strip()
    out['Invoice Month']     = df.get('1st Invoice Month', '').apply(parse_invoice_month)
    out['Recurring']         = df.get('Deal Size - Monthly Recurring', 0).apply(parse_money)
    out['Once Off']          = df.get('Deal Size - Once Off', 0).apply(parse_money)
    out['Win Probability']   = df.get('Win Probability', 0).apply(parse_money)
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
    return out

def load_uk(df):
    out = pd.DataFrame()
    out['Region']            = 'UK'
    out['Company Name']      = df.get('Company Name', '')
    out['Sales Stage']       = df.get('Sales Stage', '').str.strip()
    out['Invoice Month']     = df.get('Invoice Month', '').apply(parse_invoice_month)
    out['Recurring']         = df.get('Deal Size - Monthly Recurring', 0).apply(parse_money)
    out['Once Off']          = df.get('Deal Size - Once Off', 0).apply(parse_money)
    out['Win Probability']   = df.get('Won Probability', 0).apply(parse_money)
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
    return out

def apply_filters(df, window):
    df = df[~df['Sales Stage'].str.lower().isin(EXCLUDE_STAGES)]
    df = df[~df['Sales Rep'].str.lower().str.split().str[0].isin(EXCLUDE_REPS)]
    df = df[df['Invoice Month'].isin(window) | df['Invoice Month'].isna()]
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
