#!/usr/bin/env python3
"""
Bootstrap Superset Machine Investigation dashboard.

Layout
------
Row 0  – Native filter: unit_nr
Row 1  – KPI big-number cards: predicted_rul | risk_score | alert_level | symptom_score | trend_score
Row 2  – RUL History (Line)  |  Risk Score History (Line)
Row 3  – Alert Level Timeline (table, full-width)
Row 4  – Risk Score Components (stacked bar)  |  Symptom Sensor Breakdown (grouped bar)
Row 5  – Reason Breakdown (table, full-width)
"""
import json
import sys

from superset.app import create_app


# ── Layout helpers ─────────────────────────────────────────────────────────────

def _row(row_name: str, chart_ids: list, col_widths: list | None = None) -> dict:
    if col_widths is None:
        w = 24 // len(chart_ids)
        col_widths = [w] * len(chart_ids)
    children = [f"CHART-{cid}" for cid in chart_ids]
    out = {
        row_name: {
            "type": "ROW",
            "id": row_name,
            "children": children,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
    }
    for cid, width in zip(chart_ids, col_widths):
        out[f"CHART-{cid}"] = {
            "type": "CHART",
            "id": f"CHART-{cid}",
            "children": [],
            "meta": {"chartId": cid, "height": 30, "width": width},
        }
    return out


def _build_layout(rows: list) -> str:
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []},
    }
    for row_name, chart_ids, widths in rows:
        layout["GRID_ID"]["children"].append(row_name)
        layout.update(_row(row_name, chart_ids, widths))
    return json.dumps(layout)


# ── Param factories ─────────────────────────────────────────────────────────────

def _big_number(dataset_id: int, col: str, agg: str = "AVG", subheader: str = "", fmt: str = ".2f") -> str:
    return json.dumps({
        "datasource": f"{dataset_id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SIMPLE",
            "column": {"column_name": col},
            "aggregate": agg,
            "label": col,
        },
        "subheader": subheader,
        "y_axis_format": fmt,
        "time_range": "No filter",
        "header_font_size": 0.4,
    })


def _big_number_sql(dataset_id: int, sql_expr: str, label: str, subheader: str = "", fmt: str = "SMART_NUMBER") -> str:
    return json.dumps({
        "datasource": f"{dataset_id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": sql_expr,
            "label": label,
        },
        "subheader": subheader,
        "y_axis_format": fmt,
        "time_range": "No filter",
        "header_font_size": 0.4,
    })


def _table(dataset_id: int, columns: list, row_limit: int = 100, order_col: str = "") -> str:
    p: dict = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "table",
        "all_columns": columns,
        "row_limit": row_limit,
        "order_desc": True,
        "show_cell_bars": False,
        "time_range": "No filter",
    }
    if order_col:
        p["orderby"] = [[order_col, False]]
    return json.dumps(p)


def _line(dataset_id: int, x_axis: str, metrics: list) -> str:
    return json.dumps({
        "datasource": f"{dataset_id}__table",
        "viz_type": "echarts_timeseries_line",
        "x_axis": x_axis,
        "time_grain_sqla": "PT1M",
        "time_range": "No filter",
        "metrics": metrics,
        "groupby": [],
        "row_limit": 5000,
        "rich_tooltip": True,
        "show_legend": True,
        "opacity": 0.8,
    })


def _bar_stacked(dataset_id: int, groupby: list, metrics: list) -> str:
    """Stacked bar chart via dist_bar (supported in all Superset versions)."""
    return json.dumps({
        "datasource": f"{dataset_id}__table",
        "viz_type": "dist_bar",
        "groupby": groupby,
        "columns": [],
        "metrics": metrics,
        "bar_stacked": True,
        "show_legend": True,
        "row_limit": 50,
        "order_desc": True,
        "time_range": "No filter",
        "y_axis_label": "Score",
        "bottom_margin": "auto",
    })


def _bar_grouped(dataset_id: int, groupby: list, metrics: list) -> str:
    """Grouped bar chart via dist_bar (supported in all Superset versions)."""
    return json.dumps({
        "datasource": f"{dataset_id}__table",
        "viz_type": "dist_bar",
        "groupby": groupby,
        "columns": [],
        "metrics": metrics,
        "bar_stacked": False,
        "show_legend": True,
        "row_limit": 50,
        "order_desc": True,
        "time_range": "No filter",
        "y_axis_label": "Score",
        "bottom_margin": "auto",
    })


def _m(col: str, agg: str = "AVG", label: str = "") -> dict:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": agg,
        "label": label or col,
    }


def _m_sql(sql: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


# ── Main ────────────────────────────────────────────────────────────────────────

def main() -> int:
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        db_obj = (
            db.session.query(Database)
            .filter(Database.database_name == "Gold Warehouse")
            .one_or_none()
        )
        if not db_obj:
            raise RuntimeError("Gold Warehouse not found in Superset")

        # ── 1. Ensure datasets ────────────────────────────────────────────────
        table_names = [
            "v_machine_snapshot",
            "gold_prediction_history",
            "gold_alert_history",
            "v_alert_reason_parsed",
            "v_symptom_details_parsed",
        ]
        datasets: dict = {}
        for t in table_names:
            ds = (
                db.session.query(SqlaTable)
                .filter(
                    SqlaTable.database_id == db_obj.id,
                    SqlaTable.schema == "gold",
                    SqlaTable.table_name == t,
                )
                .one_or_none()
            )
            if not ds:
                ds = SqlaTable(table_name=t, schema="gold", database_id=db_obj.id, sql=None)
                db.session.add(ds)
                db.session.commit()
            try:
                ds.fetch_metadata()
                db.session.commit()
            except Exception:
                db.session.rollback()
            datasets[t] = ds

        snap  = datasets["v_machine_snapshot"].id
        phist = datasets["gold_prediction_history"].id
        ahist = datasets["gold_alert_history"].id
        rsn   = datasets["v_alert_reason_parsed"].id
        sym   = datasets["v_symptom_details_parsed"].id

        # ── 2. Chart specs ────────────────────────────────────────────────────
        chart_specs = [
            # ── Row 1: KPI big-number cards ──────────────────────────────────
            ("KPI: Predicted RUL",
             _big_number(snap, "predicted_rul", "AVG", "Predicted RUL (cycles)", ".1f")),
            ("KPI: Risk Score",
             _big_number(snap, "risk_score", "AVG", "Risk Score (0-100)", ".1f")),
            ("KPI: Alert Level",
             _big_number_sql(snap,
                "CASE MAX(alert_level) "
                "WHEN 'Critical' THEN 4 WHEN 'Warning' THEN 3 "
                "WHEN 'Watch' THEN 2 WHEN 'Normal' THEN 1 ELSE 0 END",
                "alert_level_num", "Alert Level (1=Normal…4=Critical)", "d")),
            ("KPI: Symptom Score",
             _big_number(snap, "symptom_score", "AVG", "Symptom Score (0–100)", ".1f")),
            ("KPI: Trend Score",
             _big_number(snap, "trend_score", "AVG", "Trend Score (0–100)", ".1f")),

            # ── Row 2: history lines ──────────────────────────────────────────
            ("RUL History (Line)",
             _line(phist, "prediction_time", [_m("predicted_rul")])),
            ("Risk Score History (Line)",
             _line(ahist, "alert_time", [
                 _m("risk_score"), _m("rul_score"),
                 _m("trend_score"), _m("symptom_score"),
             ])),

            # ── Row 3: alert timeline table ───────────────────────────────────
            ("Alert Level Timeline",
             _table(ahist,
                    ["unit_nr", "alert_time", "alert_level", "risk_score", "rul_score", "trend_score", "symptom_score"],
                    row_limit=200, order_col="alert_time")),

            # ── Row 4a: stacked bar – risk components ─────────────────────────
            ("Risk Score Components (Bar)",
             _bar_stacked(rsn, ["unit_nr"], [
                 _m("rul_score",      label="RUL Score"),
                 _m("trend_score",    label="Trend Score"),
                 _m("symptom_score",  label="Symptom Score"),
             ])),

            # ── Row 4b: grouped bar – sensor breakdown ────────────────────────
            ("Symptom Sensor Breakdown (Bar)",
             _bar_grouped(sym, ["sensor_name"], [
                 _m("deviation",  label="Deviation"),
                 _m("trend",      label="Trend"),
                 _m("volatility", label="Volatility"),
                 _m("score",      label="Score"),
             ])),

            # ── Row 5: reason breakdown table ────────────────────────────────
            ("Reason Breakdown",
             _table(rsn,
                    ["unit_nr", "alert_time", "alert_level", "raw_level",
                     "confirmed_level", "risk_score", "rul_score", "trend_score", "symptom_score"],
                    row_limit=100, order_col="alert_time")),
        ]

        # ── 3. Delete stale slices then recreate ──────────────────────────────
        for name, _ in chart_specs:
            for stale in db.session.query(Slice).filter(Slice.slice_name == name).all():
                db.session.delete(stale)
        db.session.commit()

        charts: dict = {}
        for name, params in chart_specs:
            parsed = json.loads(params)
            viz    = parsed.get("viz_type", "table")
            ds_str = parsed.get("datasource", "")
            ds_id  = int(ds_str.split("__")[0]) if "__" in ds_str else 0
            slc = Slice(slice_name=name, datasource_type="table",
                        datasource_id=ds_id, viz_type=viz, params=params)
            db.session.add(slc)
            db.session.commit()
            charts[name] = slc

        # ── 4. Assign to dashboard ────────────────────────────────────────────
        md_dash = (
            db.session.query(Dashboard)
            .filter(Dashboard.slug == "pdm-machine-detail")
            .one_or_none()
        )
        if not md_dash:
            raise RuntimeError("Machine Investigation dashboard not found – run bootstrap_dashboard.py first")

        ordered = [
            charts["KPI: Predicted RUL"],
            charts["KPI: Risk Score"],
            charts["KPI: Alert Level"],
            charts["KPI: Symptom Score"],
            charts["KPI: Trend Score"],
            charts["RUL History (Line)"],
            charts["Risk Score History (Line)"],
            charts["Alert Level Timeline"],
            charts["Risk Score Components (Bar)"],
            charts["Symptom Sensor Breakdown (Bar)"],
            charts["Reason Breakdown"],
        ]
        ids = [c.id for c in ordered]

        position = _build_layout([
            # Row 1: 5 KPI cards – each width 4 (total = 20, add 4 padding via last w=4)
            ("ROW-1", ids[0:5],  [5, 5, 4, 5, 5]),
            # Row 2: two line charts
            ("ROW-2", ids[5:7],  [12, 12]),
            # Row 3: alert timeline full-width
            ("ROW-3", ids[7:8],  [24]),
            # Row 4: stacked bar + grouped bar
            ("ROW-4", ids[8:10], [12, 12]),
            # Row 5: reason breakdown full-width
            ("ROW-5", ids[10:11],[24]),
        ])

        # ── 5. Native filter for unit_nr ──────────────────────────────────────
        # Uses v_machine_snapshot dataset as filter source (has unit_nr)
        snap_ds_id = datasets["v_machine_snapshot"].id
        native_filter = {
            "id": "NATIVE_FILTER-pdm-unit-nr",
            "name": "Machine (unit_nr)",
            "filterType": "filter_select",
            "targets": [{"datasetId": snap_ds_id, "column": {"name": "unit_nr"}}],
            "defaultDataMask": {"filterState": {"value": None}},
            "controlValues": {
                "multiSelect": False,
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "inverseSelection": False,
            },
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Select a machine to investigate",
        }
        metadata = json.dumps({
            "color_scheme": "supersetColors",
            "expanded_slices": {},
            "label_colors": {},
            "native_filter_configuration": [native_filter],
            "filter_scopes": {},
        })

        md_dash.slices = ordered
        md_dash.position_json = position
        md_dash.json_metadata  = metadata
        md_dash.published = True
        db.session.commit()

        print("Machine Investigation dashboard bootstrapped successfully.")
        print(f"  Charts ({len(ordered)}): {[c.slice_name for c in ordered]}")
        print("  Native filter: unit_nr")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"bootstrap_charts failed: {exc}", file=sys.stderr)
        raise
