#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — Interfaz web del Sistema Experto K8s (dashboard en vivo)

No reimplementa el motor de diagnóstico: importa k8s_expert.py (debe estar en
la misma carpeta) y expone su colector + motor de reglas (experta) como una
API REST y un WebSocket que alimentan el dashboard en static/.

    static/index.html  ──WS/REST──▶  app.py (FastAPI)  ──import──▶  k8s_expert.py
                                                                       │
                                                                       ▼
                                                              API de Kubernetes

Uso:
    python app.py                      # http://localhost:8000
    uvicorn app:app --reload           # modo desarrollo

Requiere: fastapi, uvicorn[standard], y las mismas dependencias que k8s_expert.py
(kubernetes, experta, rich) — ver requirements.txt.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# k8s_expert.py debe vivir junto a este archivo.
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    import k8s_expert as k8s
except ImportError as exc:  # k8s_expert.py ya valida sus propias dependencias
    sys.exit(f"No se pudo importar k8s_expert.py: {exc}")

try:
    from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
    from fastapi.staticfiles import StaticFiles
except ImportError as exc:
    sys.exit(f"Falta una dependencia ({exc.name}). Ejecuta: pip install -r requirements.txt")


STATIC_DIR = Path(__file__).resolve().parent / "static"
DEFAULT_INTERVAL = 2.0   # segundos entre lecturas del dashboard (la CLI usa la Watch API; aquí se sondea)

app = FastAPI(title="Sistema Experto K8s — API")


# ══════════════════════════════════════════════════════════════════════════
# Serialización: estructuras de k8s_expert.py -> JSON
# ══════════════════════════════════════════════════════════════════════════
def container_to_dict(c: "k8s.ContainerInfo") -> dict:
    return {
        "name": c.name, "image": c.image, "init": c.init, "state": c.state,
        "reason": c.reason, "message": c.message, "exit_code": c.exit_code,
        "ready": c.ready, "restarts": c.restarts,
        "last_reason": c.last_reason, "last_exit_code": c.last_exit_code,
        "mem_limit": c.mem_limit, "mem_request": c.mem_request,
        "log_tail": c.log_tail,
        "display_state": k8s.describe_state(c),
        "problem": c.reason in k8s.PROBLEM_REASONS,
    }


def pod_to_dict(pod: "k8s.PodInfo") -> dict:
    return {
        "name": pod.name, "namespace": pod.namespace, "phase": pod.phase,
        "reason": pod.reason, "message": pod.message, "node": pod.node, "qos": pod.qos,
        "containers": [container_to_dict(c) for c in pod.containers],
        "conditions": [c._asdict() for c in pod.conditions],
        "events": [e._asdict() for e in pod.events],
    }


def finding_to_dict(f: "k8s.Finding") -> dict:
    return {
        "rule_id": f.rule_id, "key": f.key, "container": f.container,
        "severity": f.severity, "category": f.category, "cause": f.cause,
        "steps": [{"description": d, "command": cmd} for d, cmd in f.steps],
        "evidence": f.evidence,
    }


def result_to_dict(result: "k8s.DiagnosisResult") -> dict:
    findings = [finding_to_dict(f) for f in result.findings]
    worst = max((f["severity"] for f in findings), key=lambda s: k8s.SEVERITY_ORDER[s], default=None)
    return {
        "findings": findings,
        "symptoms": [{"kind": kind, "container": c, "rule": r} for kind, c, r in result.symptoms],
        "fact_counts": dict(result.fact_counts),
        "worst_severity": worst,
    }


def report_payload(pod: "k8s.PodInfo", result: "k8s.DiagnosisResult", ctx_name: str) -> dict:
    return {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "context": ctx_name,
        "pod": pod_to_dict(pod),
        "result": result_to_dict(result),
    }


def connection_error_message(exc: Exception) -> str:
    """Mismo texto de ayuda que usa la CLI, reutilizado en la API."""
    if isinstance(exc, k8s.ClusterError):
        return str(exc)
    if isinstance(exc, k8s.HTTPError):
        return f"No se pudo conectar al clúster: {exc}  ¿Está corriendo Minikube? → minikube status"
    return str(exc)


# ══════════════════════════════════════════════════════════════════════════
# API REST
# ══════════════════════════════════════════════════════════════════════════
@app.get("/api/namespaces")
def list_namespaces(context: Optional[str] = None):
    try:
        v1, ctx_name = k8s.connect(context)
        items = v1.list_namespace().items
    except (k8s.ClusterError, k8s.HTTPError) as exc:
        raise HTTPException(status_code=503, detail=connection_error_message(exc)) from exc
    return {"context": ctx_name, "namespaces": [n.metadata.name for n in items]}


@app.get("/api/pods")
def list_pods(namespace: str = "default", context: Optional[str] = None):
    try:
        v1, ctx_name = k8s.connect(context)
        items = v1.list_namespaced_pod(namespace).items
    except (k8s.ClusterError, k8s.HTTPError) as exc:
        raise HTTPException(status_code=503, detail=connection_error_message(exc)) from exc
    pods = [{"name": p.metadata.name, "phase": p.status.phase or "Unknown"} for p in items]
    return {"context": ctx_name, "namespace": namespace, "pods": pods}


@app.get("/api/diagnose")
def diagnose_once(pod: str, namespace: str = "default", context: Optional[str] = None):
    """Diagnóstico puntual (equivalente a `python k8s_expert.py <pod>` sin --watch)."""
    try:
        v1, ctx_name = k8s.connect(context)
        raw = v1.read_namespaced_pod(pod, namespace)
    except k8s.ApiException as exc:
        if exc.status == 404:
            raise HTTPException(status_code=404, detail=f"Pod '{pod}' no encontrado en '{namespace}'.") from exc
        raise HTTPException(status_code=exc.status, detail=str(exc.reason)) from exc
    except (k8s.ClusterError, k8s.HTTPError) as exc:
        raise HTTPException(status_code=503, detail=connection_error_message(exc)) from exc

    podinfo = k8s.build_snapshot(v1, raw, with_logs=True)
    result = k8s.diagnose(podinfo)
    return report_payload(podinfo, result, ctx_name)


# ══════════════════════════════════════════════════════════════════════════
# WebSocket — dashboard en vivo
#
# La CLI (`--watch`) usa la Watch API de Kubernetes (push). Aquí, por
# simplicidad y robustez frente a cortes de conexión durante una demo, se
# sondea el Pod cada `interval` segundos: se reutilizan las mismas funciones
# `build_snapshot`, `signature` y `diagnose` de k8s_expert.py, así que el
# diagnóstico es idéntico al de la CLI; solo cambia cómo se detectan los
# cambios de estado.
# ══════════════════════════════════════════════════════════════════════════
@app.websocket("/ws/watch")
async def ws_watch(
    websocket: WebSocket,
    pod: str,
    namespace: str = "default",
    context: Optional[str] = None,
    interval: float = DEFAULT_INTERVAL,
):
    await websocket.accept()
    loop = asyncio.get_event_loop()

    try:
        v1, ctx_name = await loop.run_in_executor(None, k8s.connect, context)
    except (k8s.ClusterError, k8s.HTTPError) as exc:
        await websocket.send_json({"type": "error", "message": connection_error_message(exc)})
        await websocket.close()
        return

    last_sig = object()   # distinto de cualquier signature() real -> fuerza el primer reporte
    try:
        while True:
            try:
                raw = await loop.run_in_executor(None, v1.read_namespaced_pod, pod, namespace)
            except k8s.ApiException as exc:
                if exc.status == 404:
                    # No existe (aún). Se avisa y se sigue sondeando: si se aplica el
                    # manifiesto después de conectar el dashboard, aparece solo.
                    await websocket.send_json({
                        "type": "not_found", "pod": pod, "namespace": namespace,
                        "message": f"Pod '{pod}' no encontrado en el namespace '{namespace}'.",
                        "timestamp": datetime.now().isoformat(timespec="seconds"),
                    })
                    last_sig = object()
                    await asyncio.sleep(interval)
                    continue
                await websocket.send_json({"type": "error", "message": f"Error de la API ({exc.status}): {exc.reason}"})
                await asyncio.sleep(interval)
                continue

            podinfo = await loop.run_in_executor(None, k8s.build_snapshot, v1, raw, True)
            sig = k8s.signature(podinfo)
            if sig != last_sig:
                last_sig = sig
                result = await loop.run_in_executor(None, k8s.diagnose, podinfo)
                await websocket.send_json({"type": "report", **report_payload(podinfo, result, ctx_name)})
            else:
                await websocket.send_json({
                    "type": "heartbeat",
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                })
            await asyncio.sleep(interval)
    except WebSocketDisconnect:
        return
    except k8s.HTTPError as exc:
        try:
            await websocket.send_json({"type": "error", "message": connection_error_message(exc)})
        except Exception:
            pass
    except Exception as exc:  # cualquier otro fallo: no tumbar el servidor, avisar al cliente
        try:
            await websocket.send_json({"type": "error", "message": f"Error inesperado: {exc}"})
        except Exception:
            pass


# Rutas de API primero; el mount de estáticos va al final para no taparlas.
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


if __name__ == "__main__":
    try:
        import uvicorn
    except ImportError:
        sys.exit("Falta uvicorn. Ejecuta: pip install -r requirements.txt")
    print("Sistema Experto K8s — dashboard en http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
