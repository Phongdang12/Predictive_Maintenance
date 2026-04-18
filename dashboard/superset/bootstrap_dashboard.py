#!/usr/bin/env python3
import json
import sys

from superset.app import create_app


def _empty_layout(tab_text: str) -> str:
    return json.dumps(
        {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["TABS_ID"]},
            "TABS_ID": {"type": "TABS", "id": "TABS_ID", "children": ["TAB_ID"]},
            "TAB_ID": {
                "type": "TAB",
                "id": "TAB_ID",
                "children": [],
                "meta": {"text": tab_text},
            },
        }
    )


def ensure_dashboard(title: str, slug: str, Dashboard) -> None:
    from superset import db  

    existing = db.session.query(Dashboard).filter(Dashboard.slug == slug).one_or_none()
    if existing:
        existing.position_json = _empty_layout(title)
        existing.json_metadata = json.dumps(
            {"color_scheme": "", "expanded_slices": {}, "label_colors": {}}
        )
        existing.published = True
        db.session.commit()
        print(f"Dashboard exists: {slug} (id={existing.id})")
        return

    dash = Dashboard(
        dashboard_title=title,
        slug=slug,
        position_json=_empty_layout(title),
        json_metadata=json.dumps(
            {"color_scheme": "", "expanded_slices": {}, "label_colors": {}}
        ),
        published=True,
    )
    db.session.add(dash)
    db.session.commit()
    print(f"Dashboard created: {slug} (id={dash.id})")


def delete_dashboard_if_exists(slug: str, Dashboard) -> None:
    from superset import db

    dash = db.session.query(Dashboard).filter(Dashboard.slug == slug).one_or_none()
    if dash:
        db.session.delete(dash)
        db.session.commit()
        print(f"Dashboard deleted: {slug}")


def main() -> int:
    try:
        app = create_app()
        with app.app_context():
            # Import model inside app context; importing earlier can fail in Superset runtime.
            from superset.models.dashboard import Dashboard  # noqa: WPS433

            # Superset is investigation-only; remove any other dashboards to avoid role overlap.
            delete_dashboard_if_exists("pdm-bi-overview", Dashboard)
            ensure_dashboard("Machine Investigation", "pdm-machine-detail", Dashboard)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to bootstrap dashboards via Superset ORM: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
