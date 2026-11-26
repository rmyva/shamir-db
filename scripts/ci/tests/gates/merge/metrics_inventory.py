"""Read-only metrics inventory gate: rendered samples versus operator contracts."""
from __future__ import annotations
import json, re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
NAME = re.compile(r"^[a-z_][a-z0-9_]*$")
MACRO = re.compile(r'metrics::(counter|gauge|histogram)!\s*\(\s*"([A-Za-z_:][A-Za-z0-9_:]*)"')
FUNC = set("rate irate increase histogram_quantile sum avg min max count by without ignoring on group_left group_right time label_values deriv clamp_min clamp_max topk bottomk offset and or unless vector scalar absent".split())
DEPENDENCY_EXPORTS = {n: "gauge" for n in ("process_cpu_seconds_total", "process_open_fds", "process_max_fds", "process_virtual_memory_bytes", "process_virtual_memory_max_bytes", "process_resident_memory_bytes", "process_start_time_seconds", "process_threads")}
# Production's only online-backup source pins both optional fields to None.
UNREACHABLE_RENDER = {"shamir_backup_pin_seconds": "backup snapshot pin_seconds is always None in production source", "shamir_backup_wal_segments_copied_total": "backup snapshot wal_segments_copied_total is always None in production source"}
MANUAL_HISTOGRAM_FAMILIES = {
    "shamir_query_duration_seconds": {"source_file":"shamir-server-security/src/security/observability/export/engine.rs", "children":{"_bucket":"gauge","_count":"counter","_sum":"gauge"}},
    "shamir_function_duration_seconds": {"source_file":"shamir-server-security/src/security/observability/export/functions.rs", "children":{"_bucket":"gauge","_count":"counter","_sum":"gauge"}},
    "shamir_wal_fsync_seconds": {"source_file":"shamir-server-security/src/security/observability/export/storage.rs", "children":{"_bucket":"gauge","_count":"counter","_sum":"gauge"}},
    "shamir_visibility_ack_wait_seconds": {"source_file":"shamir-server-security/src/security/observability/visibility_export.rs", "children":{"_bucket":"gauge","_count":"counter"}},
    "shamir_visibility_reconciliation_wait_seconds": {"source_file":"shamir-server-security/src/security/observability/visibility_export.rs", "children":{"_bucket":"gauge","_count":"counter"}},
    "shamir_index_build_seconds": {"source_file":"shamir-server-security/src/security/observability/export/index_interner.rs", "children":{"_bucket":"gauge","_count":"counter","_sum":"gauge"}},
    "shamir_index_rebuild_seconds": {"source_file":"shamir-server-security/src/security/observability/export/index_interner.rs", "children":{"_bucket":"gauge","_count":"counter","_sum":"gauge"}},
}

def strip_comments(text: str) -> str:
    out=[]; i=0; quoted=False; escaped=False
    while i < len(text):
        c=text[i]; n=text[i+1] if i+1<len(text) else ""
        if c=='"' and not escaped: quoted=not quoted
        if not quoted and c=="/" and n=="/":
            while i<len(text) and text[i]!="\n": i+=1
            continue
        if not quoted and c=="/" and n=="*":
            i+=2
            while i+1<len(text) and text[i:i+2]!="*/":
                if text[i]=="\n": out.append("\n")
                i+=1
            i=min(len(text),i+2); continue
        out.append(c)
        escaped=(c=="\\" and not escaped)
        if c!="\\": escaped=False
        i+=1
    return "".join(out)

def valid_name(name): return bool(NAME.fullmatch(name))
def exported(root: Path):
    samples=set(DEPENDENCY_EXPORTS); types=dict(DEPENDENCY_EXPORTS); errors=[]
    server=root/"crates/server"; files=sorted(server.rglob("*.rs"))
    for p in files:
        rel=p.relative_to(server).as_posix()
        if any(x in {"tests","benches"} for x in Path(rel).parts): continue
        text=strip_comments(p.read_text(encoding="utf-8")); found=0
        def add(n, typ):
            nonlocal found
            if not valid_name(n): errors.append(f"invalid metric-shaped name {n!r} in {p}"); return
            samples.add(n); found+=1
            if typ is not None: types[n]=typ
        for typ,n in MACRO.findall(text):
            if typ!="histogram": add(n,typ); continue
            login=server/"shamir-server-security/src/security/observability/login_histogram.rs"
            lt=strip_comments(login.read_text(encoding="utf-8")) if login.exists() else ""
            configured=set(re.findall(r'"(shamir_login_http_(?:begin|finish)_duration_seconds)"',lt))
            if n in configured:
                for suf,t in (("_bucket","gauge"),("_count","counter"),("_sum","gauge")): add(n+suf,t)
            else:
                add(n,"gauge"); add(n+"_sum","gauge"); add(n+"_count","counter")
        if p.name=="durability_export.rs":
            for table in ("GAUGES","COUNTERS","JOURNAL_COUNTERS"):
                m=re.search(rf"const {table}[^=]*=\s*\[(.*?)\n\];",text,re.S)
                if m:
                    for n in re.findall(r'\(\s*"([^"]+)"',m[1]): add(n,"gauge" if table=="GAUGES" else "counter")
        if p.name=="backup_metrics.rs":
            m=re.search(r"SCRAPE_RENDERED_GAUGES[^=]*=\s*\[(.*?)\];",text,re.S)
            if m:
                for n in re.findall(r'"([^\"]+)"',m[1]): add(n,"gauge")
        if p.name in {"activity_export.rs","load_export.rs"}:
            for n in set(re.findall(r"(?<![A-Za-z0-9_])(?:shamir_[A-Za-z0-9_]+|auth_[A-Za-z0-9_]+|requests_in_flight|shutdown_unclean_[A-Za-z0-9_]+)(?![A-Za-z0-9_])",text)): add(n,None)
        if p.name=="mod.rs" and "/observability/" in p.as_posix():
            for n in set(re.findall(r'\(\s*"(shamir_(?:gate_|orphan_)[A-Za-z0-9_]+)"',text)): add(n,None)
        if p.name in {"engine.rs","functions.rs","storage.rs","visibility_export.rs","index_interner.rs"}:
            for base,spec in MANUAL_HISTOGRAM_FAMILIES.items():
                if spec["source_file"]!=rel: continue
                index=base.startswith("shamir_index_")
                context=(bool(re.search(r'format!\s*\(\s*"\{base\}_(?:bucket|count|sum)"',text)) if index else any(f'"{base}{suffix}"' in text for suffix in spec["children"]))
                if not context or any(s not in text for s in spec["children"]): errors.append(f"manual family not rendered as declared: {base} in {p}"); continue
                for suffix,typ in spec["children"].items(): add(base+suffix,typ)
    for n in UNREACHABLE_RENDER: samples.discard(n); types.pop(n,None)
    return samples,types,errors

def documented(path: Path):
    out=set(); active=False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip()=="### `/metrics`": active=True; continue
        if active and line.startswith("### "): break
        if not active or not line.lstrip().startswith("|"): continue
        cells=line.lstrip().split("|"); cell=cells[1].strip() if len(cells)>1 else ""
        m=re.fullmatch(r'`([a-z_][a-z0-9_]*(?:\{[^`]*\})?)`',cell)
        if m: out.add(m[1].split("{",1)[0])
    return out

def expr_names(expr: str):
    expr=re.sub(r'"(?:\\.|[^"\\])*"'," ",expr)
    refs=set()
    for block in re.findall(r"\{([^{}]*)\}",expr): refs.update(re.findall(r'\b__name__\s*=\s*"([a-zA-Z_:][A-Za-z0-9_:]*)"',block))
    expr=re.sub(r"\{[^{}]*\}"," ",expr)
    expr=re.sub(r"\b(?:by|without|ignoring|on|group_left|group_right)\s*\([^)]*\)"," ",expr)
    expr=re.sub(r"(?<=\d)(?:ms|s|m|h|d|w|y)\b"," ",expr)
    expr=re.sub(r"\b(?:and|or|unless)\b"," ",expr)
    names=set()
    for token in re.finditer(r"[A-Za-z_:][A-Za-z0-9_:]*",expr):
        if token[0] not in FUNC or not re.match(r"\s*\(",expr[token.end():]): names.add(token[0])
    return refs|names

def load_rules(path: Path):
    raw="\n".join(x for x in path.read_text(encoding="utf-8").splitlines() if not x.lstrip().startswith("#"))
    try: data=json.loads(raw)
    except Exception as e: return {},[f"invalid rule JSON {path}: {e}"]
    if not isinstance(data,dict) or not isinstance(data.get("groups"),list): return {},[f"invalid rules shape {path}"]
    return data,[]
def all_rules(root):
    out={}; errors=[]
    for p in sorted((root/"deploy/alerts").glob("*.yml")):
        d,e=load_rules(p); errors+=e
        if d: out[p]=d
    return out,errors
def validate_expr(expr,known,samples,where):
    return [f"unknown metric {n!r} in {where}" for n in sorted(expr_names(expr)) if n not in samples and n not in known]
def scan_dashboard(path,samples):
    errors=[]
    try: data=json.loads(path.read_text(encoding="utf-8"))
    except Exception as e: return [f"invalid dashboard JSON {path}: {e}"]
    ids=set()
    def walk(o):
        if isinstance(o,dict):
            if "id" in o and isinstance(o.get("type"),str):
                if o["id"] in ids: errors.append(f"duplicate panel id {o['id']} in {path}")
                ids.add(o["id"])
            if "targets" in o:
                if "datasource" not in o: errors.append(f"panel with targets lacks datasource in {path}")
                if not isinstance(o.get("type"),str): errors.append(f"panel without type in {path}")
                for t in o["targets"] if isinstance(o["targets"],list) else []:
                    if isinstance(t,dict) and isinstance(t.get("expr"),str): errors.extend(validate_expr(t["expr"],set(),samples,str(path)))
            for v in o.values(): walk(v)
        elif isinstance(o,list):
            for v in o: walk(v)
    walk(data); return errors

def scan(root: Path=ROOT):
    samples,types,errors=exported(root); docs=documented(root/"docs/guide-docs/guide/07-operations.md")
    errors += [f"exported not documented: {n}" for n in sorted(samples-docs)]
    errors += [f"documented not exported: {n}" for n in sorted(docs-samples)]
    rules,rule_errors=all_rules(root); errors+=rule_errors
    duration=re.compile(r"(?:\d+(?:ms|s|m|h|d|w|y))+")
    for path,data in rules.items():
        known={r.get("record") for g in data["groups"] if isinstance(g,dict) for r in g.get("rules",[]) if isinstance(r,dict) and isinstance(r.get("record"),str)}
        for group in data["groups"]:
            for r in group.get("rules",[]) if isinstance(group,dict) and isinstance(group.get("rules",[]),list) else []:
                if not isinstance(r,dict): errors.append(f"invalid rule in {path}"); continue
                rec="record" in r; backup=path.name=="shamir-backup.rules.yml"
                allowed=( {"record","expr","labels"} if rec else ({"alert","expr","annotations"}|({"for"} if "for" in r else set()) if backup else {"alert","expr","for","labels","annotations"}) )
                if set(r)!=allowed: errors.append(f"invalid rule keys in {path}: {sorted(r)}")
                expr=r.get("expr")
                if not isinstance(expr,str) or not expr.strip(): errors.append(f"empty expr in {path}")
                else: errors+=validate_expr(expr,known,samples,str(path))
                if backup and not rec:
                    if "for" in r and (not isinstance(r["for"],str) or not duration.fullmatch(r["for"])): errors.append(f"invalid for in {path}")
                    if not isinstance(r.get("annotations"),dict) or not r["annotations"]: errors.append(f"invalid annotations in {path}")
                elif not backup and not rec:
                    if not isinstance(r.get("for"),str) or not duration.fullmatch(r["for"]): errors.append(f"invalid for in {path}")
                    if not isinstance(r.get("labels"),dict) or "severity" not in r["labels"]: errors.append(f"missing severity in {path}")
                    if not isinstance(r.get("annotations"),dict) or not r["annotations"]: errors.append(f"invalid annotations in {path}")
    for p in sorted((root/"deploy/grafana").glob("*.json")): errors+=scan_dashboard(p,samples)
    return errors

def main():
    errors=scan()
    for e in errors: print(e)
    if errors: return 1
    print("metrics inventory OK"); return 0
if __name__=="__main__": raise SystemExit(main())
