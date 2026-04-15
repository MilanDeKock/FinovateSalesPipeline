import streamlit as st
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import BytesIO

st.set_page_config(page_title="Smartsheets Pipeline Cleaner", layout="centered")
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

def load_uk(df, drop_past=True):
    ecd = pd.to_datetime(df.get('Expected Close Date'), errors='coerce')
    if drop_past:
        today = pd.Timestamp(datetime.today().date())
        keep = ecd.notna() & (ecd >= today)
    else:
        keep = ecd.notna()
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

def apply_filters(df, window, excl_reps, excl_stages):
    df = df[~df['Sales Stage'].fillna('').isin(excl_stages)]
    df = df[~df['Sales Rep'].fillna('').isin(excl_reps)]
    df = df[df['Invoice Month'].isin(window)]
    return df

def build_window(start_name, end_name):
    s = MONTHS.index(start_name.lower())
    e = MONTHS.index(end_name.lower())
    if s <= e:
        return MONTHS[s:e+1]
    return MONTHS[s:] + MONTHS[:e+1]

# ── UI ───────────────────────────────────────────────────────────────────────
MONTHS_TITLE = [m.title() for m in MONTHS]
default_win = get_invoice_window()
default_from_title = default_win[0].title()
default_to_title   = default_win[-1].title()

if st.button("🔄 Reset filters to defaults"):
    for k in ['f_from','f_to','f_reps','f_stages','f_drop_past_uk']:
        st.session_state.pop(k, None)
    st.rerun()

st.subheader("Filters")
filter_slot = st.container()

with filter_slot:
    mc1, mc2, mc3 = st.columns([1,1,1])
    from_month = mc1.selectbox("Invoice month from", MONTHS_TITLE,
        index=MONTHS_TITLE.index(default_from_title), key='f_from')
    to_month = mc2.selectbox("Invoice month to", MONTHS_TITLE,
        index=MONTHS_TITLE.index(default_to_title), key='f_to')
    drop_past_uk = mc3.checkbox("Drop UK past-dated deals", value=True, key='f_drop_past_uk')

reps_stages_slot = st.container()

st.markdown("---")
st.subheader("Upload files")
col1, col2 = st.columns(2)
sa_file = col1.file_uploader("SA Pipeline (xlsx)", type=['xlsx','xls'], key='sa_upload')
uk_file = col2.file_uploader("UK Pipeline (xlsx)", type=['xlsx','xls'], key='uk_upload')

if not sa_file or not uk_file:
    with reps_stages_slot:
        st.info("Upload both files to configure rep/stage filters")
    st.stop()

sa_raw = pd.read_excel(sa_file)
uk_raw = pd.read_excel(uk_file)

sa = load_sa(sa_raw)
uk = load_uk(uk_raw, drop_past=drop_past_uk)
combined = pd.concat([sa, uk], ignore_index=True)

all_reps   = sorted({r.strip() for r in combined['Sales Rep'].dropna().astype(str) if r.strip()})
all_stages = sorted({s for s in combined['Sales Stage'].dropna().astype(str) if s})
default_reps   = [r for r in all_reps if r.lower().split()[0] in EXCLUDE_REPS]
default_stages = [s for s in all_stages if s.lower() in EXCLUDE_STAGES]

with reps_stages_slot:
    rc1, rc2 = st.columns(2)
    excl_reps   = rc1.multiselect("Exclude reps", all_reps, default=default_reps, key='f_reps')
    excl_stages = rc2.multiselect("Exclude stages", all_stages, default=default_stages, key='f_stages')

window  = build_window(from_month, to_month)
cleaned = apply_filters(combined, window, excl_reps, excl_stages)

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
