"""Export an interactive, Obsidian-style force graph (standalone HTML via vis-network)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _require_duckdb():
	try:
		import duckdb
	except ImportError as e:  # pragma: no cover
		raise ImportError("duckdb required: poetry install --with graph") from e
	return duckdb


def build_vis_dataset(db_path: Path) -> dict[str, Any]:
	"""Return ``{nodes: [...], edges: [...]}`` for vis-network DataSet format."""
	duckdb = _require_duckdb()
	con = duckdb.connect(str(db_path.resolve()), read_only=True)
	node_map: dict[str, dict[str, Any]] = {}
	edges: list[dict[str, Any]] = []
	edge_id = 0

	def add_node(nid: str, label: str, group: str, title: str = "") -> None:
		if nid not in node_map:
			node_map[nid] = {
				"id": nid,
				"label": label[:80] + ("…" if len(label) > 80 else ""),
				"group": group,
				"title": title or label,
			}

	try:
		fn_rows = con.execute(
			"SELECT dotted_path, file_path, line, is_whitelisted, function_name, "
			"cyclomatic, max_ast_depth FROM functions"
		).fetchall()
		fn_paths: set[str] = set()
		for row in fn_rows:
			dp, fpath, line, is_wl, fname, cyc, depth = row
			fn_paths.add(dp)
			nid = f"fn:{dp}"
			group = "whitelist" if is_wl else "function"
			score = (cyc or 0) * 2 + (depth or 0)
			title = (
				f"{dp}\nfile: {fpath}:{line}\n"
				f"whitelisted: {is_wl}\ncyclomatic: {cyc}\nmax depth: {depth}\nscore: {score}"
			)
			add_node(nid, fname or dp.rsplit(".", 1)[-1], group, title)

			fid = f"file:{fpath}"
			add_node(fid, fpath.split("/")[-1][:40], "file", fpath)
			edges.append(
				{
					"id": str(edge_id),
					"from": nid,
					"to": fid,
					"arrows": "to",
					"color": {"color": "#bdc3c7", "opacity": 0.35},
					"physics": True,
				}
			)
			edge_id += 1

		cls_paths: set[str] = set()
		try:
			cls_rows = con.execute(
				"SELECT dotted_path, file_path, line, class_name FROM python_classes"
			).fetchall()
		except Exception:
			cls_rows = []

		for dp, fpath, line, cname in cls_rows:
			cls_paths.add(dp)
			cid = f"cls:{dp}"
			title = f"{dp}\nclass\nfile: {fpath}:{line}"
			add_node(cid, cname or dp.rsplit(".", 1)[-1], "py_class", title)
			fid = f"file:{fpath}"
			add_node(fid, fpath.split("/")[-1][:40], "file", fpath)
			edges.append(
				{
					"id": str(edge_id),
					"from": cid,
					"to": fid,
					"arrows": "to",
					"color": {"color": "#bdc3c7", "opacity": 0.35},
					"physics": True,
				}
			)
			edge_id += 1

		try:
			py_call_rows = con.execute(
				"SELECT caller_dotted_path, callee_dotted_path, file_path, line "
				"FROM python_call_edges"
			).fetchall()
		except Exception:
			py_call_rows = []

		for caller_dp, callee_dp, fpath, ln in py_call_rows:
			cid = f"fn:{caller_dp}"
			if callee_dp in fn_paths:
				tid = f"fn:{callee_dp}"
			elif callee_dp in cls_paths:
				tid = f"cls:{callee_dp}"
			else:
				tid = f"fn:{callee_dp}"
				add_node(
					tid,
					callee_dp.rsplit(".", 1)[-1][:40],
					"external",
					f"{callee_dp}\n(Python call target, not in app scan)",
				)
			edges.append(
				{
					"id": str(edge_id),
					"from": cid,
					"to": tid,
					"arrows": "to",
					"color": {"color": "#f39c12", "opacity": 0.85},
					"title": f"py call {fpath}:{ln}",
					"physics": True,
				}
			)
			edge_id += 1

		try:
			imp_rows = con.execute(
				"SELECT importer_module, target_module, file_path, line, import_kind, names "
				"FROM python_import_edges"
			).fetchall()
		except Exception:
			imp_rows = []

		imp_agg: dict[tuple[str, str], list[tuple[int, str, str]]] = {}
		for _im, tgt, fp, ln, ikind, names in imp_rows:
			nm = names or ""
			imp_agg.setdefault((fp, tgt), []).append((ln, ikind, nm))

		for (fp, tgt), entries in imp_agg.items():
			mid = f"mod:{tgt}"
			short = tgt.split(".")[-1][:40]
			add_node(mid, short, "module", tgt)
			fid = f"file:{fp}"
			add_node(fid, fp.split("/")[-1][:40], "file", fp)
			entries_sorted = sorted(entries, key=lambda t: t[0])
			parts_t = [
				f"L{ln} {k}" + (f" ({nm})" if nm else "") for ln, k, nm in entries_sorted[:10]
			]
			tooltip = "; ".join(parts_t)
			if len(entries_sorted) > 10:
				tooltip += f" … (+{len(entries_sorted) - 10})"
			edges.append(
				{
					"id": str(edge_id),
					"from": fid,
					"to": mid,
					"arrows": "to",
					"color": {"color": "#8e44ad", "opacity": 0.5},
					"title": tooltip,
					"physics": False,
				}
			)
			edge_id += 1

		hook_rows = con.execute(
			"SELECT DISTINCT hook_variable FROM hook_registrations"
		).fetchall()
		for (hv,) in hook_rows:
			hid = f"hook:{hv}"
			add_node(hid, hv[:40], "hook", f"hooks.py key: {hv}")

		for src_file, line, hook_variable, target_path in con.execute(
			"SELECT source_file, line, hook_variable, target_path FROM hook_registrations"
		).fetchall():
			if target_path in fn_paths:
				tid = f"fn:{target_path}"
			elif target_path in cls_paths:
				tid = f"cls:{target_path}"
				short = target_path.rsplit(".", 1)[-1][:40]
				add_node(
					tid,
					short,
					"py_class",
					f"{target_path}\nhook target (class)\n{src_file}:{line}",
				)
			else:
				tid = f"fn:{target_path}"
				add_node(
					tid,
					target_path.rsplit(".", 1)[-1][:40],
					"external",
					f"{target_path}\n(not defined in this app scan)",
				)
			hid = f"hook:{hook_variable}"
			edges.append(
				{
					"id": str(edge_id),
					"from": hid,
					"to": tid,
					"arrows": "to",
					"color": {"color": "#9b59b6", "opacity": 0.7},
					"title": f"{src_file}:{line}",
				}
			)
			edge_id += 1

		for file_path, line, target_path, loop_ctx, loop_type in con.execute(
			"SELECT file_path, line, target_path, loop_context, loop_type FROM js_callsites"
		).fetchall():
			if not target_path:
				continue
			jid = f"js:{file_path}:{line}"
			lt = (
				f" (loop: {loop_type})"
				if loop_ctx and loop_type
				else (" (in loop)" if loop_ctx else "")
			)
			add_node(jid, f"JS L{line}", "js_call", f"{file_path}:{line}{lt}\n→ {target_path}")
			tid = f"fn:{target_path}"
			if target_path not in fn_paths:
				add_node(
					tid,
					target_path.rsplit(".", 1)[-1][:40],
					"external",
					f"{target_path}\n(JS target, not in app scan)",
				)
			fid = f"file:{file_path}"
			add_node(fid, file_path.split("/")[-1][:40], "file", file_path)
			edges.append(
				{
					"id": str(edge_id),
					"from": jid,
					"to": tid,
					"arrows": "to",
					"color": {"color": "#e74c3c" if loop_ctx else "#3498db", "opacity": 0.75},
				}
			)
			edge_id += 1
			edges.append(
				{
					"id": str(edge_id),
					"from": jid,
					"to": fid,
					"arrows": "to",
					"color": {"color": "#95a5a6", "opacity": 0.25},
					"physics": False,
				}
			)
			edge_id += 1

	finally:
		con.close()

	return {"nodes": list(node_map.values()), "edges": edges}


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Code graph</title>
  <script src="https://unpkg.com/vis-network@9.1.9/standalone/umd/vis-network.min.js" crossorigin="anonymous"></script>
  <style>
    * { box-sizing: border-box; }
    html, body { margin: 0; height: 100%; font-family: system-ui, sans-serif; }
    #wrap { display: flex; flex-direction: column; height: 100%; }
    #toolbar {
      padding: 8px 12px; background: #1e1e1e; color: #ccc; font-size: 13px;
      border-bottom: 1px solid #333;
    }
    #toolbar label { margin-right: 16px; cursor: pointer; }
    #graph { flex: 1; min-height: 0; background: #121212; }
    .legend { opacity: 0.85; }
    .legend span { display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 4px; }
  </style>
</head>
<body>
  <div id="wrap">
    <div id="toolbar">
      <strong style="color:#fff">Code graph</strong>
      <span class="legend" style="margin-left:20px">
        <span style="background:#2ecc71"></span>whitelist
        <span style="background:#3498db"></span>function
        <span style="background:#f4d03f"></span>class
        <span style="background:#e67e22"></span>external
        <span style="background:#9b59b6"></span>hook
        <span style="background:#1abc9c"></span>JS call
        <span style="background:#7f8c8d"></span>file
        <span style="background:#8e44ad"></span>module
        <span style="color:#f39c12">━</span>Python call
        <span style="color:#8e44ad">━</span>import
      </span>
      <label><input type="checkbox" id="physics" /> Physics</label>
      <span id="stats"></span>
    </div>
    <div id="graph"></div>
  </div>
  <script type="application/json" id="graph-json">__PAYLOAD__</script>
  <script>
    const raw = document.getElementById("graph-json").textContent;
    const graphPayload = JSON.parse(raw);
    const groups = {
      whitelist: { color: { background: "#2ecc71", border: "#27ae60" }, font: { color: "#111" } },
      function: { color: { background: "#3498db", border: "#2980b9" }, font: { color: "#fff" } },
      external: { color: { background: "#e67e22", border: "#d35400" }, font: { color: "#fff" } },
      hook: { color: { background: "#9b59b6", border: "#8e44ad" }, font: { color: "#fff" }, shape: "box" },
      js_call: { color: { background: "#1abc9c", border: "#16a085" }, font: { color: "#111" }, shape: "dot", size: 12 },
      file: { color: { background: "#34495e", border: "#2c3e50" }, font: { color: "#ecf0f1", size: 11 }, shape: "ellipse" },
      module: { color: { background: "#8e44ad", border: "#6c3483" }, font: { color: "#fff", size: 12 }, shape: "box" },
      py_class: { color: { background: "#f4d03f", border: "#d4ac0d" }, font: { color: "#111", size: 13 }, shape: "box" }
    };
    const nodes = new vis.DataSet(graphPayload.nodes);
    const edges = new vis.DataSet(graphPayload.edges);
    const container = document.getElementById("graph");
    const data = { nodes: nodes, edges: edges };
    const options = {
      nodes: { font: { size: 14 }, borderWidth: 1, shadow: true },
      edges: { smooth: { type: "continuous" } },
      physics: {
        enabled: false,
        maxVelocity: 24,
        barnesHut: { gravitationalConstant: -9000, centralGravity: 0.1, springLength: 155, springConstant: 0.045 },
        stabilization: { iterations: 280, updateInterval: 25 }
      },
      interaction: { hover: true, tooltipDelay: 120, navigationButtons: true, keyboard: true },
      groups: groups
    };
    const network = new vis.Network(container, data, options);
    document.getElementById("stats").textContent =
      graphPayload.nodes.length + " nodes · " + graphPayload.edges.length + " edges";
    const physicsChk = document.getElementById("physics");
    if (physicsChk) {
      physicsChk.addEventListener("change", function () {
        network.setOptions({ physics: { enabled: this.checked } });
      });
    }
  </script>
</body>
</html>
"""


def write_interactive_html(db_path: Path, output_path: Path) -> Path:
	"""Write a single self-contained HTML file (CDN vis-network + embedded JSON)."""
	data = build_vis_dataset(db_path)
	payload = json.dumps(data, ensure_ascii=False)
	# JSON inside script tag: escape </ to avoid premature close
	payload_safe = payload.replace("</", "<\\/")
	html = _HTML_TEMPLATE.replace("__PAYLOAD__", payload_safe)
	output_path = Path(output_path).resolve()
	output_path.parent.mkdir(parents=True, exist_ok=True)
	output_path.write_text(html, encoding="utf-8")
	return output_path


def write_graph_json(db_path: Path, output_path: Path) -> Path:
	"""Write ``graph.json`` (nodes + edges) for other tools."""
	data = build_vis_dataset(db_path)
	output_path = Path(output_path).resolve()
	output_path.parent.mkdir(parents=True, exist_ok=True)
	output_path.write_text(
		json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
	)
	return output_path
