import json
import re
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from gates.merge import metrics_inventory as mi


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.dependency_patch = unittest.mock.patch.object(mi, "DEPENDENCY_EXPORTS", {})
        self.dependency_patch.start()
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)
        for d in ("crates/server", "docs/guide-docs/guide", "deploy/grafana", "deploy/alerts"):
            (self.root/d).mkdir(parents=True)
        self.exporter=self.root/"crates/server/durability_export.rs"
        self.guide=self.root/"docs/guide-docs/guide/07-operations.md"
        self.dashboard=self.root/"deploy/grafana/test.json"
        self.rules=self.root/"deploy/alerts/test.yml"
        self.write_exporter(); self.write_guide(); self.write_dashboard("fixture_counter"); self.write_rules()

    def tearDown(self):
        if self.dependency_patch is not None: self.dependency_patch.stop()
        self.tmp.cleanup()
    def write_exporter(self):
        self.exporter.write_text('metrics::counter!("fixture_counter");\nconst GAUGES: [(&str, &str); 1] = [("fixture_table_gauge", "help")]\n];\n',encoding="utf-8")
        (self.root/"crates/server/activity_export.rs").write_text('fn render() { render_sample("shamir_fixture_helper"); }',encoding="utf-8")
    def write_guide(self, rows=True):
        text="### `/metrics`\n\n| Metric | Type |\n|---|---|\n"
        if rows: text += "| `fixture_counter{kind}` | counter |\n| `fixture_table_gauge` | gauge |\n| `shamir_fixture_helper` | gauge |\n"
        text += "Prose mentions `fixture_counter` only.\n"
        self.guide.write_text(text,encoding="utf-8")
    def write_dashboard(self, expr, ids=(1,), broken=False):
        if broken: self.dashboard.write_text("{broken",encoding="utf-8"); return
        self.dashboard.write_text(json.dumps({"panels":[{"id":i,"type":"timeseries","datasource":{"type":"prometheus"},"targets":[{"expr":expr}]} for i in ids]}),encoding="utf-8")
    def write_rules(self, rules=None, broken=False):
        if broken: self.rules.write_text("# fixture comment\n{broken",encoding="utf-8"); return
        if rules is None: rules=[{"alert":"Fixture","expr":"fixture_counter > 0","for":"5m","labels":{"severity":"warning"},"annotations":{"summary":"s"}}]
        groups=rules if rules and isinstance(rules[0],tuple) else [("test",rules)]
        data={"groups":[{"name":name,"rules":rs} for name,rs in groups]}
        self.rules.write_text("# fixture comment\n"+json.dumps(data),encoding="utf-8")
    def errors(self): return mi.scan(self.root)
    def exact(self, expected): self.assertEqual(self.errors(),expected)
    def test_exporter_removed(self):
        self.exporter.write_text('const GAUGES: [(&str, &str); 1] = [("fixture_table_gauge", "help")]\n];\n',encoding="utf-8")
        self.write_guide(rows=False)
        self.write_dashboard("shamir_fixture_helper")
        self.write_rules([{"alert":"Fixture","expr":"shamir_fixture_helper > 0","for":"5m","labels":{"severity":"warning"},"annotations":{"summary":"s"}}])
        self.assertEqual(self.errors(),["exported not documented: fixture_table_gauge","exported not documented: shamir_fixture_helper"])
    def test_prose_does_not_document_series(self):
        self.write_guide(rows=False); errs=self.errors()
        self.assertEqual([e for e in errs if e.startswith("exported not documented:")], [f"exported not documented: {n}" for n in ("fixture_counter","fixture_table_gauge","shamir_fixture_helper")])
        self.assertEqual(errs,[f"exported not documented: {n}" for n in ("fixture_counter","fixture_table_gauge","shamir_fixture_helper")])
    def test_unemitted_sum_documented_is_rejected(self):
        self.guide.write_text(self.guide.read_text(encoding="utf-8")+"| `fixture_counter_sum` | gauge |\n",encoding="utf-8")
        self.assertEqual(self.errors(),["documented not exported: fixture_counter_sum"])
    def test_unknown_foreign_metrics_in_dashboard_and_alert(self):
        self.write_dashboard("foreign_vendor_metric")
        self.write_rules([{"alert":"Foreign","expr":"foreign_vendor_metric > 0","for":"5m","labels":{"severity":"warning"},"annotations":{"summary":"s"}}])
        errs=self.errors(); self.assertEqual([e for e in errs if e.startswith("unknown metric")], [f"unknown metric 'foreign_vendor_metric' in {self.rules}",f"unknown metric 'foreign_vendor_metric' in {self.dashboard}"]); self.assertEqual(errs,[f"unknown metric 'foreign_vendor_metric' in {self.rules}",f"unknown metric 'foreign_vendor_metric' in {self.dashboard}"])
    def test_empty_expression_rejected(self):
        self.write_rules([{"alert":"Empty","expr":"","for":"5m","labels":{"severity":"warning"},"annotations":{"summary":"s"}}]); self.assertEqual(self.errors(),[f"empty expr in {self.rules}"])
    def test_invalid_for_rejected(self):
        self.write_rules([{"alert":"Bad","expr":"fixture_counter","for":"5x","labels":{"severity":"warning"},"annotations":{"summary":"s"}}]); self.assertEqual(self.errors(),[f"invalid for in {self.rules}"])
    def test_compound_duration_accepted(self):
        self.write_rules([{"alert":"Compound","expr":"fixture_counter","for":"1h30m","labels":{"severity":"warning"},"annotations":{"summary":"s"}}]); self.assertEqual(self.errors(),[])
    def test_rule_without_severity_rejected(self):
        self.write_rules([{"alert":"NoSeverity","expr":"fixture_counter","for":"5m","labels":{},"annotations":{"summary":"s"}}]); self.assertEqual(self.errors(),[f"missing severity in {self.rules}"])
    def test_same_file_recording_rule_accepted(self):
        self.write_rules([("one",[{"record":"fixture_recorded","expr":"fixture_counter","labels":{}}]),("two",[{"alert":"Recorded","expr":"fixture_recorded","for":"5m","labels":{"severity":"warning"},"annotations":{"summary":"s"}}])]); self.assertEqual(self.errors(),[])
    def test_duplicate_panel_ids_rejected(self):
        self.write_dashboard("fixture_counter",ids=(1,1)); self.assertEqual(self.errors(),[f"duplicate panel id 1 in {self.dashboard}"])
    def test_broken_dashboard_json(self):
        self.write_dashboard("",broken=True); self.assertTrue(self.errors()[0].startswith(f"invalid dashboard JSON {self.dashboard}")); self.assertEqual(len(self.errors()),1)
    def test_broken_rule_json(self):
        self.write_rules(broken=True); self.assertTrue(self.errors()[0].startswith(f"invalid rule JSON {self.rules}")); self.assertEqual(len(self.errors()),1)
    def test_manual_family_missing_child_rejected(self):
        src=self.root/"crates/server/shamir-server-security/src/security/observability/export/engine.rs"; src.parent.mkdir(parents=True)
        src.write_text('"shamir_query_duration_seconds" "_bucket" "_sum"',encoding="utf-8")
        self.assertEqual(self.errors(),[f"manual family not rendered as declared: shamir_query_duration_seconds in {src}"])
    def test_real_tree_contract(self):
        self.dependency_patch.stop(); self.dependency_patch = None
        errors=mi.scan(mi.ROOT); self.assertEqual(errors,[],msg="\n".join(errors))
        samples,types,extract_errors=mi.exported(mi.ROOT); self.assertEqual(extract_errors,[])
        self.assertIn("shamir_query_errors_total",samples); self.assertEqual(types["shamir_query_errors_total"],"counter")
        self.assertIn("shamir_gate_pending_versions",samples)
        for child in ("bucket","count","sum"): self.assertIn("shamir_query_duration_seconds_"+child,samples)
        doc=mi.documented(mi.ROOT/"docs/guide-docs/guide/07-operations.md"); self.assertIn("shamir_login_http_begin_duration_seconds_bucket",doc)
        text=(mi.ROOT/"docs/guide-docs/guide/07-operations.md").read_text(encoding="utf-8"); lt=(mi.ROOT/"crates/server/shamir-server-security/src/security/observability/login_histogram.rs").read_text(encoding="utf-8"); buckets_code=re.findall(r"\d+(?:\.\d+)?",lt.split("const BUCKETS",1)[1].split("= &[",1)[1].split("];",1)[0]); row_guide=next(l for l in text.splitlines() if "shamir_login_http_begin_duration_seconds_bucket{le}" in l); self.assertEqual(sorted(map(float,buckets_code)),sorted(map(float,re.findall(r"\d+(?:\.\d+)?",row_guide.split("границы",1)[1].split("секунд",1)[0]))))
        rules,_=mi.load_rules(mi.ROOT/"deploy/alerts/shamir.rules.yml"); alerts=[r for g in rules["groups"] for r in g["rules"]]
        self.assertEqual(len(alerts),19); self.assertFalse(any("node_filesystem" in r.get("expr","") for r in alerts))

if __name__=="__main__": unittest.main()
