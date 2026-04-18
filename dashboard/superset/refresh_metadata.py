#!/usr/bin/env python3
"""
Force-refresh Superset dataset metadata for dashboard datasets.

Safe to run at any time — skips tables that don't exist in the warehouse yet
(e.g. on fresh setup before gold-sync has run for the first time).
"""

from superset.app import create_app


def main() -> int:
    app = create_app()
    with app.app_context():
        from sqlalchemy.exc import NoSuchTableError

        from superset import db
        from superset.connectors.sqla.models import SqlaTable

        table_names = [
            "v_machine_snapshot",
            "gold_prediction_history",
            "gold_alert_history",
            "v_alert_reason_parsed",
            "v_symptom_details_parsed",
        ]

        for name in table_names:
            ds = (
                db.session.query(SqlaTable)
                .filter(SqlaTable.schema == "gold", SqlaTable.table_name == name)
                .one_or_none()
            )
            if not ds:
                print(f"[skip] Dataset not registered yet: gold.{name}")
                continue
            try:
                ds.fetch_metadata()
                db.session.commit()
                print(f"[ok]   Refreshed gold.{name} -> columns={len(ds.columns)}")
            except NoSuchTableError:
                db.session.rollback()
                print(f"[skip] Table not in warehouse yet: gold.{name}")
            except Exception as exc:
                db.session.rollback()
                print(f"[warn] Could not refresh gold.{name}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
