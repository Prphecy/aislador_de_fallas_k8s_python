#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
k8s_expert.py — Sistema Experto de diagnóstico de fallas en Kubernetes (prototipo)

    API de K8s ──▶ PodInfo ──▶ Hechos ──▶ Reglas (experta) ──▶ Diagnóstico (rich)
    (kubernetes)  (colector)  (memoria    capa 1: síntomas
                               de trabajo) capa 2: diagnósticos

Uso:
    python k8s_expert.py oom-demo                  # diagnóstico puntual
    python k8s_expert.py oom-demo --watch          # tiempo real (Watch API)
    python k8s_expert.py oom-demo -n default --context minikube

Código de salida: 0 = sin fallas relevantes · 1 = falla detectada · 2 = error de ejecución
"""
from __future__ import annotations

import argparse
import collections
import collections.abc
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import NamedTuple, Optional


# ══════════════════════════════════════════════════════════════════════════
# 0. COMPATIBILIDAD
# ══════════════════════════════════════════════════════════════════════════
def _restore_collections_aliases() -> None:
    """experta 1.9.x depende de frozendict==1.2, que usa `collections.Mapping`
    (eliminado en Python 3.10). Se restauran los alias ANTES de importar experta."""
    for name in ("Mapping", "MutableMapping", "Sequence", "MutableSequence",
                 "Set", "MutableSet", "Iterable", "Callable", "Hashable"):
        if not hasattr(collections, name):
            setattr(collections, name, getattr(collections.abc, name))


_restore_collections_aliases()

try:
    from experta import MATCH, NOT, TEST, Fact, KnowledgeEngine, Rule, W
    from kubernetes import client, config, watch
    from kubernetes.client.rest import ApiException
    from rich import box
    from rich.console import Console
    from rich.markup import escape
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich.tree import Tree
    from urllib3.exceptions import HTTPError
except ImportError as exc:  # pragma: no cover
    sys.exit(f"Falta una dependencia ({exc.name}). Ejecuta: pip install -r requirements.txt")


# ══════════════════════════════════════════════════════════════════════════
# 1. MODELO DE DATOS (lo leído de la API, ya normalizado)
# ══════════════════════════════════════════════════════════════════════════
RECURRENT_RESTARTS = 3      # a partir de N reinicios un OOMKilled se considera "recurrente"

CRASH_REASONS = {"CrashLoopBackOff", "Error"}
IMAGE_PULL_REASONS = {"ErrImagePull", "ImagePullBackOff", "InvalidImageName", "ErrImageNeverPull"}
CONFIG_ERROR_REASONS = {"CreateContainerConfigError", "CreateContainerError"}
PROBLEM_REASONS = {"OOMKilled"} | CRASH_REASONS | IMAGE_PULL_REASONS | CONFIG_ERROR_REASONS


class Condition(NamedTuple):
    type: str
    status: str
    reason: str
    message: str


class EventInfo(NamedTuple):
    type: str
    reason: str
    message: str
    count: int


@dataclass
class ContainerInfo:
    name: str
    image: str = ""
    init: bool = False
    state: str = "unknown"          # waiting | running | terminated | unknown
    reason: str = ""                # motivo del estado ACTUAL (CrashLoopBackOff, OOMKilled…)
    message: str = ""
    exit_code: int = -1
    ready: bool = False
    restarts: int = 0
    last_reason: str = ""           # motivo de la ÚLTIMA terminación (status.lastState)
    last_exit_code: int = -1
    last_finished: Optional[datetime] = None
    mem_limit: str = ""
    mem_request: str = ""
    log_tail: list[str] = field(default_factory=list)


@dataclass
class PodInfo:
    name: str
    namespace: str
    phase: str
    reason: str = ""
    message: str = ""
    node: str = ""
    qos: str = ""
    containers: list[ContainerInfo] = field(default_factory=list)
    conditions: list[Condition] = field(default_factory=list)
    events: list[EventInfo] = field(default_factory=list)

    def container(self, name: str) -> Optional[ContainerInfo]:
        return next((c for c in self.containers if c.name == name), None)


# ══════════════════════════════════════════════════════════════════════════
# 2. COLECTOR — lectura del estado real vía la API oficial de Kubernetes
# ══════════════════════════════════════════════════════════════════════════
def _container_info(spec, status, init: bool) -> ContainerInfo:
    """Combina spec (límites) y status (estado en vivo) de un contenedor."""
    res = spec.resources
    limits = (res.limits or {}) if res else {}
    requests = (res.requests or {}) if res else {}
    info = ContainerInfo(name=spec.name, image=spec.image or "", init=init,
                         mem_limit=limits.get("memory", ""), mem_request=requests.get("memory", ""))
    if status is None:                       # p. ej. Pod Pending: aún sin status
        return info

    info.ready = bool(status.ready)
    info.restarts = status.restart_count or 0
    st = status.state
    if st is not None:
        if st.waiting:
            info.state, info.reason, info.message = "waiting", st.waiting.reason or "", st.waiting.message or ""
        elif st.running:
            info.state = "running"
        elif st.terminated:
            t = st.terminated
            info.state, info.reason, info.message = "terminated", t.reason or "", t.message or ""
            info.exit_code = t.exit_code if t.exit_code is not None else -1
    last = status.last_state.terminated if status.last_state else None
    if last:
        info.last_reason = last.reason or ""
        info.last_exit_code = last.exit_code if last.exit_code is not None else -1
        info.last_finished = last.finished_at
    return info


def fetch_events(v1, pod, limit: int = 6) -> list[EventInfo]:
    """Últimos eventos del Pod (filtrados por UID para no mezclar Pods homónimos)."""
    selector = (f"involvedObject.kind=Pod,involvedObject.name={pod.metadata.name},"
                f"involvedObject.uid={pod.metadata.uid}")
    try:
        items = v1.list_namespaced_event(pod.metadata.namespace, field_selector=selector).items
    except ApiException:
        return []
    items.sort(key=lambda e: e.last_timestamp or e.event_time or e.metadata.creation_timestamp)
    return [EventInfo(e.type or "", e.reason or "", (e.message or "").strip(), e.count or 1)
            for e in items[-limit:]]


def fetch_log_tail(v1, namespace: str, pod: str, container: str, previous: bool, lines: int = 6) -> list[str]:
    try:
        text = v1.read_namespaced_pod_log(name=pod, namespace=namespace, container=container,
                                          previous=previous, tail_lines=lines)
    except ApiException:                     # p. ej. 400: el contenedor aún no tiene logs
        return []
    return [ln for ln in (text or "").splitlines() if ln.strip()][-lines:]


def build_snapshot(v1, pod, with_logs: bool = True) -> PodInfo:
    """V1Pod (API de K8s) -> PodInfo normalizado, incluyendo eventos y logs previos."""
    meta, spec, status = pod.metadata, pod.spec, pod.status
    main_st = {s.name: s for s in (status.container_statuses or [])}
    init_st = {s.name: s for s in (status.init_container_statuses or [])}

    containers = [_container_info(c, init_st.get(c.name), True) for c in (spec.init_containers or [])]
    containers += [_container_info(c, main_st.get(c.name), False) for c in spec.containers]

    if with_logs:
        for c in containers:
            if c.restarts > 0 or (c.state == "terminated" and c.reason != "Completed"):
                # con reinicios, los logs útiles son los de la instancia que murió (--previous)
                c.log_tail = fetch_log_tail(v1, meta.namespace, meta.name, c.name, previous=c.restarts > 0)

    return PodInfo(
        name=meta.name, namespace=meta.namespace, phase=status.phase or "Unknown",
        reason=status.reason or "", message=status.message or "",
        node=spec.node_name or "", qos=status.qos_class or "",
        containers=containers,
        conditions=[Condition(c.type, c.status, c.reason or "", c.message or "")
                    for c in (status.conditions or [])],
        events=fetch_events(v1, pod),
    )


# ══════════════════════════════════════════════════════════════════════════
# 3. BASE DE CONOCIMIENTO — texto experto separado de las condiciones (reglas)
# ══════════════════════════════════════════════════════════════════════════
SEVERITY_ORDER = {"CRÍTICA": 4, "ALTA": 3, "MEDIA": 2, "BAJA": 1, "INFO": 0}


@dataclass(frozen=True)
class Knowledge:
    severity: str
    category: str
    cause: str                                   # plantilla .format(**ctx)
    steps: tuple                                 # ((descripción, comando | None), ...)


@dataclass
class Finding:
    rule_id: str
    key: str
    container: str
    severity: str
    category: str
    cause: str
    steps: list
    evidence: list


EXIT_HINTS = {
    0: "salió con éxito: el proceso principal terminó; un servicio debe quedarse en primer plano (o usa un Job).",
    1: "error genérico de la aplicación (excepción no controlada, configuración inválida). Revisa los logs.",
    2: "uso incorrecto del comando o shell (argumentos inválidos).",
    126: "el comando existe pero no es ejecutable (permisos).",
    127: "comando no encontrado (command/args incorrectos o binario ausente en la imagen).",
    137: "SIGKILL (128+9): OOMKilled, kill -9 o liveness probe fallida.",
    139: "SIGSEGV (128+11): fallo de segmentación en la app o en una librería nativa.",
    143: "SIGTERM (128+15): terminación ordenada (rolling update, desalojo, probe).",
}

PULL_HINTS = [
    (("not found", "manifest unknown", "invalid reference"), "El nombre o el tag de la imagen no existe en el registro."),
    (("unauthorized", "denied", "authentication required"), "El registro exige credenciales (imagen privada) o faltan permisos."),
    (("no such host", "timeout", "connection refused", "tls", "dial tcp"), "Problema de red/DNS entre el nodo y el registro."),
]
SCHED_HINTS = [
    (("insufficient memory",), "Ningún nodo tiene memoria libre suficiente para los requests del Pod."),
    (("insufficient cpu",), "Ningún nodo tiene CPU libre suficiente para los requests del Pod."),
    (("taint",), "Los nodos tienen taints que el Pod no tolera."),
    (("node affinity", "node selector", "nodeselector"), "El nodeSelector/affinity del Pod no coincide con ningún nodo."),
    (("persistentvolumeclaim", "unbound"), "El Pod espera un PVC que no está enlazado."),
]

_OOM_STEPS = (
    ("Confirma el motivo de la última terminación (Reason: OOMKilled, Exit Code: 137).",
     "kubectl describe pod {pod} -n {ns} | grep -A6 'Last State'"),
    ("Mide el consumo real (requiere metrics-server: `minikube addons enable metrics-server`).",
     "kubectl top pod {pod} -n {ns} --containers"),
    ("Si el límite está subdimensionado, súbelo (propuesta inicial: {suggest}) y alinea requests.memory. "
     "Los recursos de un Pod suelto son inmutables: elimínalo y vuelve a aplicarlo con el manifiesto corregido.",
     "kubectl delete pod {pod} -n {ns} && kubectl apply -f <manifiesto-corregido>.yaml"),
    ("Si lo gestiona un Deployment/StatefulSet, ajusta el límite en el controlador.",
     "kubectl set resources deployment/<nombre> -c {container} --limits=memory={suggest}"),
    ("Si el consumo crece sin techo (fuga de memoria), perfila la app y alinea el runtime con el límite "
     "(JVM -Xmx, Node --max-old-space-size, caches y pools acotados).", None),
    ("Prevención: alerta cuando working_set/limit supere ~85 % y evalúa un VPA.", None),
)

KB: dict[str, Knowledge] = {
    "OOM_KILLED_RECURRENT": Knowledge(
        "CRÍTICA", "Recursos · Memoria (OOMKilled recurrente)",
        "El contenedor «{container}» excede su límite de memoria ({limit}) una y otra vez: el OOM killer del "
        "kernel (cgroup) lo termina con SIGKILL (exit code {exit_code}) y el kubelet lo reinicia en bucle "
        "({restarts} reinicios → CrashLoopBackOff con backoff creciente). El servicio está caído o degradado.",
        _OOM_STEPS),
    "OOM_KILLED": Knowledge(
        "ALTA", "Recursos · Memoria (OOMKilled)",
        "El contenedor «{container}» superó su límite de memoria ({limit}) y el OOM killer del kernel lo "
        "terminó con SIGKILL (exit code {exit_code}). Reinicios acumulados: {restarts}. "
        "Si se repite, derivará en CrashLoopBackOff.",
        _OOM_STEPS),
    "OOM_NODE_PRESSURE": Knowledge(
        "ALTA", "Recursos · Memoria del nodo (sin límite definido)",
        "«{container}» fue terminado por OOM pero NO tiene límite de memoria: el kernel del nodo se quedó sin "
        "memoria y eligió víctimas (típico de pods BestEffort). Reinicios: {restarts}.",
        (("Revisa las condiciones del nodo (MemoryPressure).",
          "kubectl describe node {node} | grep -A8 Conditions"),
         ("Identifica qué pods consumen más memoria.", "kubectl top pods -A --sort-by=memory"),
         ("Define requests y limits de memoria para que el Pod sea Burstable/Guaranteed y no el primer "
          "candidato a morir.", None),
         ("En Minikube, si el nodo es pequeño, recrea el cluster con más RAM (destructivo).",
          "minikube delete && minikube start --memory=4096"))),
    "CONTAINER_CRASH": Knowledge(
        "ALTA", "Aplicación · Contenedor en falla",
        "El contenedor «{container}» termina de forma inesperada y el kubelet lo reinicia "
        "({restarts} reinicios; estado actual: {state}). Última salida: {exit_hint}",
        (("Lee los logs de la instancia que falló.", "kubectl logs {pod} -n {ns} -c {container} --previous"),
         ("Revisa eventos y motivo de terminación.", "kubectl describe pod {pod} -n {ns}"),
         ("Verifica command/args, variables de entorno y dependencias (BD, servicios) necesarias al arrancar.", None),
         ("Depura con un contenedor efímero.",
          "kubectl debug -it {pod} -n {ns} --image=busybox --target={container}"))),
    "IMAGE_PULL_FAIL": Knowledge(
        "ALTA", "Imagen · Registro",
        "No se puede descargar la imagen «{image}» del contenedor «{container}» ({reason}). {detail}",
        (("Inspecciona el error exacto en los eventos.",
          "kubectl describe pod {pod} -n {ns} | grep -iE 'failed|pull|backoff'"),
         ("Corrige el nombre/tag de la imagen (typo, tag inexistente).", None),
         ("Si el registro es privado, crea un imagePullSecret y referéncialo en el Pod.",
          "kubectl create secret docker-registry regcred --docker-server=<registry> "
          "--docker-username=<user> --docker-password=<pass> -n {ns}"),
         ("Si es red/DNS, verifica la salida a Internet desde Minikube.",
          "minikube ssh -- curl -sI https://registry-1.docker.io/v2/"),
         ("Para imágenes locales usa `minikube image load <imagen>` e imagePullPolicy: IfNotPresent.", None))),
    "CONFIG_ERROR": Knowledge(
        "ALTA", "Configuración · ConfigMap/Secret",
        "Kubernetes no pudo crear el contenedor «{container}» ({reason}). Detalle: {detail}",
        (("Comprueba que existan los ConfigMaps/Secrets referenciados.",
          "kubectl get configmap,secret -n {ns}"),
         ("Crea el recurso faltante o corrige el nombre/clave en env.valueFrom, envFrom o volumes.", None),
         ("Confirma el mensaje exacto en los eventos.", "kubectl describe pod {pod} -n {ns}"))),
    "UNSCHEDULABLE": Knowledge(
        "ALTA", "Scheduling · Capacidad",
        "El scheduler no encontró un nodo donde ubicar el Pod (PodScheduled=False). {detail}",
        (("Lee el motivo exacto en los eventos.",
          "kubectl describe pod {pod} -n {ns} | grep -A6 Events"),
         ("Compara lo solicitado con lo asignable en los nodos.",
          "kubectl describe nodes | grep -A8 'Allocated resources'"),
         ("Reduce requests o añade capacidad (Minikube: `minikube start --cpus=4 --memory=4096` en un cluster nuevo, "
          "o `minikube node add`).", None),
         ("Revisa taints/tolerations, nodeSelector/affinity y PVCs pendientes.", None))),
    "EVICTED": Knowledge(
        "MEDIA", "Recursos · Desalojo por el nodo",
        "El kubelet desalojó el Pod por presión de recursos en el nodo «{node}». {detail}",
        (("Revisa las condiciones del nodo.", "kubectl describe node {node} | grep -A8 Conditions"),
         ("Define requests/limits adecuados (memoria o ephemeral-storage según el mensaje).", None),
         ("Un Pod Evicted no se recrea solo si no hay controlador: elimínalo y vuelve a crearlo.",
          "kubectl delete pod {pod} -n {ns}"))),
    "NOT_READY": Knowledge(
        "MEDIA", "Salud de la aplicación · Readiness",
        "El contenedor «{container}» está en ejecución pero no pasa a Ready: la readiness probe falla o la app "
        "aún no acepta tráfico. Mientras tanto, los Services no le enviarán tráfico.",
        (("Busca «Readiness probe failed» en los eventos.", "kubectl describe pod {pod} -n {ns}"),
         ("Revisa los logs de arranque.", "kubectl logs {pod} -n {ns} -c {container}"),
         ("Verifica path/puerto/timeouts de readinessProbe; sube initialDelaySeconds si la app tarda en arrancar.",
          None))),
    "HEALTHY": Knowledge(
        "INFO", "Sin fallas",
        "Todos los contenedores están en ejecución y Ready (fase {phase}). No se detectaron síntomas de falla "
        "conocidos.",
        (("No se requiere acción.", None),)),
    "UNKNOWN": Knowledge(
        "BAJA", "Indeterminado · Posible estado transitorio",
        "El Pod está en fase {phase} y ninguna regla de la base de conocimiento coincide (puede ser un estado "
        "transitorio como ContainerCreating o PodInitializing).",
        (("Espera unos segundos y vuelve a ejecutar (o usa --watch).", None),
         ("Si persiste, revisa los eventos.", "kubectl describe pod {pod} -n {ns}"))),
}


# ── utilidades de interpretación ───────────────────────────────────────────
def parse_quantity_bytes(q: str) -> Optional[int]:
    """'100Mi' -> 104857600 (bytes). None si no se puede interpretar."""
    if not q:
        return None
    units = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40,
             "k": 10**3, "K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12}
    for suffix, mult in units.items():
        if q.endswith(suffix):
            try:
                return int(float(q[:-len(suffix)]) * mult)
            except ValueError:
                return None
    try:
        return int(float(q))
    except ValueError:
        return None


def suggest_limit(limit_bytes: Optional[int]) -> str:
    """Propuesta INICIAL: duplicar el límite, redondeado a múltiplos de 64Mi (mín. 256Mi)."""
    mi = ((limit_bytes or 128 * 2**20) * 2) // 2**20
    return f"{max(256, ((mi + 63) // 64) * 64)}Mi"


def exit_hint(code: int) -> str:
    if code < 0:
        return "desconocida."
    if code in EXIT_HINTS:
        return f"{code} = {EXIT_HINTS[code]}"
    if code > 128:
        return f"{code} = terminado por la señal {code - 128}."
    return f"{code} = código de salida propio de la aplicación."


def exit_code_of(c: ContainerInfo) -> int:
    return c.exit_code if (c.state == "terminated" and c.exit_code >= 0) else c.last_exit_code


def describe_state(c: ContainerInfo) -> str:
    if c.state == "waiting":
        return f"Waiting({c.reason or '?'})"
    if c.state == "terminated":
        return f"Terminated({c.reason or '?'}, exit {c.exit_code})"
    return "Running" if c.state == "running" else "Unknown"


def age_seconds(ts: Optional[datetime]) -> Optional[int]:
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0, int((datetime.now(timezone.utc) - ts).total_seconds()))


def human_age(s: int) -> str:
    if s < 90:
        return f"{s}s"
    if s < 5400:
        return f"{s // 60}m"
    return f"{s // 3600}h" if s < 172800 else f"{s // 86400}d"


def _clip(text: str, n: int = 300) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= n else text[: n - 1] + "…"


def match_hints(text: str, table) -> list[str]:
    low = (text or "").lower()
    return [hint for keys, hint in table if any(k in low for k in keys)]


def interpret(key: str, pod: PodInfo, c: Optional[ContainerInfo]) -> str:
    """Detalle contextual que se inserta en la causa raíz."""
    if key == "IMAGE_PULL_FAIL" and c:
        text = " ".join([c.message] + [e.message for e in pod.events if e.type == "Warning"])
        return " ".join(match_hints(text, PULL_HINTS)) or _clip(c.message)
    if key == "UNSCHEDULABLE":
        msg = next((x.message for x in pod.conditions if x.type == "PodScheduled"), "")
        return " ".join(match_hints(msg, SCHED_HINTS)) or _clip(msg)
    if key == "CONFIG_ERROR" and c:
        return _clip(c.message)
    if key == "EVICTED":
        return _clip(pod.message)
    return ""


def build_context(key: str, pod: PodInfo, c: Optional[ContainerInfo]) -> dict:
    code = exit_code_of(c) if c else -1
    return {
        "pod": pod.name, "ns": pod.namespace, "node": pod.node or "<nodo>", "phase": pod.phase,
        "container": c.name if c else "", "image": c.image if c else "",
        "reason": (c.reason if c else pod.reason) or "?",
        "restarts": c.restarts if c else 0,
        "state": describe_state(c) if c else pod.phase,
        "limit": c.mem_limit if (c and c.mem_limit) else "sin límite",
        "suggest": suggest_limit(parse_quantity_bytes(c.mem_limit) if c else None),
        "exit_code": code if code >= 0 else "?",
        "exit_hint": exit_hint(code),
        "detail": interpret(key, pod, c),
    }


def build_evidence(pod: PodInfo, c: Optional[ContainerInfo]) -> list[str]:
    ev: list[str] = []
    if c is not None:
        ev.append(f"Contenedor «{c.name}»: {describe_state(c)} · reinicios={c.restarts} · "
                  f"ready={'sí' if c.ready else 'no'}")
        if c.last_reason:
            age = age_seconds(c.last_finished)
            ev.append(f"Última terminación: {c.last_reason} (exit code {c.last_exit_code})"
                      + (f" hace {human_age(age)}" if age is not None else ""))
        if c.mem_limit or c.mem_request:
            ev.append(f"Memoria: request={c.mem_request or '—'} · limit={c.mem_limit or '—'} · "
                      f"QoS={pod.qos or '—'}")
        if c.message:
            ev.append(f"Mensaje: {_clip(c.message)}")
        ev.extend(f"log ▸ {_clip(line, 160)}" for line in c.log_tail[-3:])
    else:
        ev.append(f"Pod en fase {pod.phase}" + (f" (motivo: {pod.reason})" if pod.reason else ""))
        for cond in pod.conditions:
            if cond.status == "False":
                ev.append(f"Condición {cond.type}=False"
                          + (f" ({cond.reason})" if cond.reason else "")
                          + (f": {_clip(cond.message)}" if cond.message else ""))
        if pod.message:
            ev.append(f"Mensaje: {_clip(pod.message)}")
    return ev


def make_finding(rule_id: str, key: str, pod: PodInfo, c: Optional[ContainerInfo]) -> Finding:
    kb, ctx = KB[key], build_context(key, pod, c)
    return Finding(
        rule_id=rule_id, key=key, container=c.name if c else "",
        severity=kb.severity, category=kb.category,
        cause=kb.cause.format(**ctx),
        steps=[(d.format(**ctx), cmd.format(**ctx) if cmd else None) for d, cmd in kb.steps],
        evidence=build_evidence(pod, c),
    )


# ══════════════════════════════════════════════════════════════════════════
# 4. MOTOR DE INFERENCIA (experta) — encadenamiento hacia adelante en 2 capas
#       hechos observados ──▶ síntomas (S..) ──▶ diagnósticos (D..)
#    Conflictos resueltos por salience: lo más específico dispara primero y
#    marca `Concluded`, lo que bloquea (NOT) a las reglas más genéricas.
# ══════════════════════════════════════════════════════════════════════════
class PodFact(Fact):
    """Estado agregado del Pod: name, phase, reason, ready."""


class ContainerFact(Fact):
    """Estado observado de un contenedor: name, role, state, reason, restarts, last_reason, mem_limit…"""


class ConditionFact(Fact):
    """Condición del Pod: cond, status, reason."""


class Symptom(Fact):
    """Hecho derivado (capa 1): kind + container ('' si es del Pod completo)."""


class Concluded(Fact):
    """Marca que ya existe diagnóstico para un contenedor (o para el Pod, container='')."""


@dataclass
class DiagnosisResult:
    findings: list[Finding]
    symptoms: list[tuple[str, str, str]]     # (síntoma, contenedor, regla)
    fact_counts: Counter


class K8sExpertEngine(KnowledgeEngine):

    def __init__(self, pod: PodInfo):
        super().__init__()
        self.pod = pod
        self.findings: list[Finding] = []
        self.symptoms: list[tuple[str, str, str]] = []
        self.fact_counts: Counter = Counter()
        self._concluded: set[str] = set()

    # ── memoria de trabajo: PodInfo -> hechos ─────────────────────────────
    def load_facts(self) -> None:
        pod = self.pod
        main = [c for c in pod.containers if not c.init]
        self._add(PodFact(name=pod.name, phase=pod.phase, reason=pod.reason,
                          ready=bool(main) and all(c.ready for c in main)))
        for c in pod.containers:
            self._add(ContainerFact(
                name=c.name, role="init" if c.init else "main", state=c.state, reason=c.reason,
                exit_code=c.exit_code, ready=c.ready, restarts=c.restarts,
                last_reason=c.last_reason, last_exit_code=c.last_exit_code, mem_limit=c.mem_limit))
        for cond in pod.conditions:
            self._add(ConditionFact(cond=cond.type, status=cond.status, reason=cond.reason))

    def _add(self, fact: Fact) -> None:
        self.fact_counts[type(fact).__name__] += 1
        self.declare(fact)

    # ── acciones (RHS) reutilizables ──────────────────────────────────────
    def _symptom(self, kind: str, container: str, rule_id: str) -> None:
        if any(k == kind and c == container for k, c, _ in self.symptoms):
            return                                   # varias reglas pueden detectar el mismo síntoma
        self.symptoms.append((kind, container, rule_id))
        self.declare(Symptom(kind=kind, container=container))

    def _conclude(self, rule_id: str, key: str, container: str = "") -> None:
        # 1 diagnóstico por contenedor; HEALTHY/UNKNOWN nunca conviven con otro hallazgo
        if container in self._concluded or (key in ("HEALTHY", "UNKNOWN") and self.findings):
            return
        self._concluded.add(container)
        self.findings.append(make_finding(rule_id, key, self.pod, self.pod.container(container)))
        self.declare(Concluded(container=container))

    # ══ CAPA 1 · SÍNTOMAS (salience 1000: se derivan antes de diagnosticar) ═
    @Rule(ContainerFact(name=MATCH.name, reason="OOMKilled"), salience=1000)
    def s01_oom_estado_actual(self, name, **_):
        self._symptom("OOM_KILLED", name, "S01")

    @Rule(ContainerFact(name=MATCH.name, last_reason="OOMKilled"), salience=1000)
    def s02_oom_ultima_terminacion(self, name, **_):
        self._symptom("OOM_KILLED", name, "S02")

    @Rule(ContainerFact(name=MATCH.name, reason=MATCH.reason),
          TEST(lambda reason: reason in CRASH_REASONS), salience=1000)
    def s03_contenedor_en_falla(self, name, **_):
        self._symptom("CONTAINER_CRASH", name, "S03")

    @Rule(ContainerFact(name=MATCH.name, reason=MATCH.reason),
          TEST(lambda reason: reason in IMAGE_PULL_REASONS), salience=1000)
    def s04_imagen_no_descargable(self, name, **_):
        self._symptom("IMAGE_PULL_FAIL", name, "S04")

    @Rule(ContainerFact(name=MATCH.name, reason=MATCH.reason),
          TEST(lambda reason: reason in CONFIG_ERROR_REASONS), salience=1000)
    def s05_error_de_configuracion(self, name, **_):
        self._symptom("CONFIG_ERROR", name, "S05")

    @Rule(PodFact(phase="Pending"),
          ConditionFact(cond="PodScheduled", status="False", reason="Unschedulable"), salience=1000)
    def s06_pod_no_planificable(self, **_):
        self._symptom("UNSCHEDULABLE", "", "S06")

    @Rule(PodFact(reason="Evicted"), salience=1000)
    def s07_pod_desalojado(self, **_):
        self._symptom("EVICTED", "", "S07")

    @Rule(PodFact(phase="Running"),
          ContainerFact(name=MATCH.name, role="main", state="running", ready=MATCH.ready),
          TEST(lambda ready: not ready), salience=1000)
    def s08_contenedor_no_ready(self, name, **_):
        self._symptom("NOT_READY", name, "S08")

    # ══ CAPA 2 · DIAGNÓSTICOS (de más específico a más genérico) ═══════════
    @Rule(Symptom(kind="OOM_KILLED", container=MATCH.name),
          ContainerFact(name=MATCH.name, mem_limit=MATCH.limit),
          TEST(lambda limit: limit == ""),
          NOT(Concluded(container=MATCH.name)), salience=110)
    def d01_oom_sin_limite(self, name, **_):
        self._conclude("D01", "OOM_NODE_PRESSURE", name)

    @Rule(Symptom(kind="OOM_KILLED", container=MATCH.name),
          ContainerFact(name=MATCH.name, restarts=MATCH.restarts),
          TEST(lambda restarts: restarts >= RECURRENT_RESTARTS),
          NOT(Concluded(container=MATCH.name)), salience=100)
    def d02_oom_recurrente(self, name, **_):
        self._conclude("D02", "OOM_KILLED_RECURRENT", name)

    @Rule(Symptom(kind="OOM_KILLED", container=MATCH.name),
          NOT(Concluded(container=MATCH.name)), salience=90)
    def d03_oom_killed(self, name, **_):
        self._conclude("D03", "OOM_KILLED", name)

    @Rule(Symptom(kind="IMAGE_PULL_FAIL", container=MATCH.name),
          NOT(Concluded(container=MATCH.name)), salience=80)
    def d04_imagen_no_descargable(self, name, **_):
        self._conclude("D04", "IMAGE_PULL_FAIL", name)

    @Rule(Symptom(kind="CONFIG_ERROR", container=MATCH.name),
          NOT(Concluded(container=MATCH.name)), salience=80)
    def d05_error_de_configuracion(self, name, **_):
        self._conclude("D05", "CONFIG_ERROR", name)

    @Rule(Symptom(kind="CONTAINER_CRASH", container=MATCH.name),
          NOT(Concluded(container=MATCH.name)),
          NOT(Symptom(kind="EVICTED", container=W())),     # un desalojo explica el "Error" del contenedor
          salience=50)
    def d06_contenedor_en_falla(self, name, **_):
        self._conclude("D06", "CONTAINER_CRASH", name)

    @Rule(Symptom(kind="UNSCHEDULABLE", container=MATCH.c),
          NOT(Concluded(container=MATCH.c)), salience=60)
    def d07_no_planificable(self, **_):
        self._conclude("D07", "UNSCHEDULABLE", "")

    @Rule(Symptom(kind="EVICTED", container=MATCH.c),
          NOT(Concluded(container=MATCH.c)), salience=60)
    def d08_desalojado(self, **_):
        self._conclude("D08", "EVICTED", "")

    @Rule(Symptom(kind="NOT_READY", container=MATCH.name),
          NOT(Concluded(container=MATCH.name)), salience=40)
    def d09_no_ready(self, name, **_):
        self._conclude("D09", "NOT_READY", name)

    @Rule(PodFact(phase=MATCH.phase, ready=MATCH.ready),
          TEST(lambda phase, ready: phase == "Succeeded" or (phase == "Running" and ready)),
          NOT(Symptom(container=W())), salience=10)
    def d10_saludable(self, **_):
        self._conclude("D10", "HEALTHY", "")

    @Rule(PodFact(name=MATCH.pod), NOT(Concluded(container=W())), salience=-100)
    def d99_sin_patron_conocido(self, **_):
        self._conclude("D99", "UNKNOWN", "")


def diagnose(pod: PodInfo) -> DiagnosisResult:
    engine = K8sExpertEngine(pod)
    engine.reset()
    engine.load_facts()
    engine.run()
    findings = sorted(engine.findings, key=lambda f: -SEVERITY_ORDER[f.severity])
    return DiagnosisResult(findings, engine.symptoms, engine.fact_counts)


# ══════════════════════════════════════════════════════════════════════════
# 5. PRESENTACIÓN (rich)
# ══════════════════════════════════════════════════════════════════════════
SEVERITY_STYLE = {   # etiqueta, color de borde, icono
    "CRÍTICA": ("bold white on red", "red", "🔴"),
    "ALTA": ("bold black on dark_orange", "dark_orange", "🟠"),
    "MEDIA": ("bold black on yellow", "yellow", "🟡"),
    "BAJA": ("bold black on cyan", "cyan", "🔵"),
    "INFO": ("bold black on green", "green", "🟢"),
}


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def steps_text(steps) -> Text:
    out = Text()
    for i, (desc, cmd) in enumerate(steps, 1):
        out.append(f"{i}. ", style="bold")
        out.append(desc + "\n")
        if cmd:
            out.append(f"   $ {cmd}\n", style="bold cyan")
    out.rstrip()
    return out


def diagnosis_panel(f: Finding, primary: bool) -> Panel:
    label_style, border, icon = SEVERITY_STYLE[f.severity]
    grid = Table.grid(padding=(0, 2, 1, 0), expand=True)
    grid.add_column(justify="right", style="bold", no_wrap=True)
    grid.add_column(ratio=1)
    grid.add_row("Severidad", Text.assemble(f"{icon} ", (f" {f.severity} ", label_style)))
    grid.add_row("Categoría", Text(f.category, style="bold"))
    grid.add_row("Causa raíz", Text(f.cause))
    grid.add_row("Remediación", steps_text(f.steps))
    if f.evidence:
        grid.add_row("Evidencia", Text("\n".join(f"• {e}" for e in f.evidence), style="dim"))
    scope = f" · {f.container}" if f.container else ""
    return Panel(grid, title=f"[bold]{'DIAGNÓSTICO' if primary else 'HALLAZGO ADICIONAL'}{escape(scope)}[/bold]",
                 subtitle=f"regla {f.rule_id} · {f.key}", border_style=border,
                 box=box.HEAVY if primary else box.ROUNDED, padding=(1, 2))


def containers_table(pod: PodInfo) -> Table:
    t = Table(title="Contenedores (estado leído de la API)", title_justify="left", title_style="bold",
              box=box.SIMPLE_HEAD, header_style="bold")
    for col in ("Contenedor", "Estado", "Motivo", "Últ. terminación", "Reinicios", "Ready", "Mem limit"):
        t.add_column(col, overflow="fold")
    for c in pod.containers:
        last = f"{c.last_reason} ({c.last_exit_code})" if c.last_reason else "—"
        t.add_row(
            Text(c.name + (" (init)" if c.init else ""), style="bold"),
            Text(c.state),
            Text(c.reason or "—", style="bold red" if c.reason in PROBLEM_REASONS else ""),
            Text(last, style="bold red" if c.last_reason == "OOMKilled" else ""),
            Text(str(c.restarts), style="bold yellow" if c.restarts >= RECURRENT_RESTARTS else ""),
            Text("✔" if c.ready else "✘", style="green" if c.ready else "red"),
            Text(c.mem_limit or "—"),
        )
    return t


def events_table(pod: PodInfo) -> Table:
    t = Table(title="Últimos eventos del Pod", title_justify="left", title_style="bold",
              box=box.SIMPLE_HEAD, header_style="bold")
    t.add_column("Tipo")
    t.add_column("Motivo")
    t.add_column("Mensaje", ratio=1, overflow="fold")
    t.add_column("×", justify="right")
    for e in pod.events:
        t.add_row(Text(e.type, style="yellow" if e.type == "Warning" else "dim"), Text(e.reason),
                  Text(_clip(e.message, 200)), Text(str(e.count)))
    return t


def inference_tree(result: DiagnosisResult) -> Tree:
    tree = Tree("[dim]Cadena de inferencia (forward chaining)[/dim]")
    facts = ", ".join(f"{k}×{v}" for k, v in result.fact_counts.items())
    node = tree.add(f"[dim]Hechos declarados: {escape(facts)}[/dim]")
    for kind, container, rule in result.symptoms:
        node.add(f"[dim]{rule} → síntoma {kind}" + (f" @ {escape(container)}" if container else "") + "[/dim]")
    for f in result.findings:
        node.add(f"[dim]{f.rule_id} → diagnóstico {f.key} ({f.severity})[/dim]")
    return tree


def render_report(console: Console, pod: PodInfo, result: DiagnosisResult, ctx_name: str) -> None:
    head = Table.grid(padding=(0, 3))
    head.add_row(*(Text.assemble((f"{k} ", "dim"), (v or "—", "bold")) for k, v in (
        ("Pod", f"{pod.namespace}/{pod.name}"), ("Fase", pod.phase), ("Nodo", pod.node),
        ("QoS", pod.qos), ("Contexto", ctx_name))))
    console.print(Panel(head, title="🩺 Sistema Experto K8s", border_style="blue", box=box.ROUNDED))
    console.print(containers_table(pod))
    if pod.events:
        console.print(events_table(pod))
    for i, f in enumerate(result.findings):
        console.print(diagnosis_panel(f, primary=(i == 0)))
    console.print(inference_tree(result))
    console.print()


def status_line(pod: PodInfo) -> Text:
    line = Text(f"[{_now()}] ", style="dim")
    line.append(pod.phase, style="bold")
    for c in pod.containers:
        line.append(f" · {c.name}: {describe_state(c)} ↻{c.restarts}")
    return line


# ══════════════════════════════════════════════════════════════════════════
# 6. CLI — conexión al cluster, modo puntual y modo tiempo real
# ══════════════════════════════════════════════════════════════════════════
class ClusterError(Exception):
    pass


def connect(context: Optional[str]):
    try:
        config.load_kube_config(context=context)          # Minikube escribe en ~/.kube/config
        _, active = config.list_kube_config_contexts()
    except (config.ConfigException, FileNotFoundError) as exc:
        raise ClusterError(f"No se pudo cargar el kubeconfig: {exc}\n  ¿Está corriendo Minikube? → minikube status")
    return client.CoreV1Api(), context or (active or {}).get("name", "")


def signature(pod: PodInfo) -> tuple:
    """Huella del estado: solo re-diagnosticamos cuando algo relevante cambia."""
    return (pod.phase, pod.reason, tuple(
        (c.name, c.state, c.reason, c.ready, c.restarts, c.last_reason) for c in pod.containers))


def watch_pod(v1, args, console: Console, ctx_name: str) -> None:
    console.print(f"[dim]Observando {escape(args.namespace)}/{escape(args.pod)} en tiempo real · Ctrl+C para salir[/dim]")
    deadline = time.monotonic() + args.timeout if args.timeout else None
    last_sig = last_key = None
    while True:
        remaining = None
        if deadline is not None:
            remaining = int(deadline - time.monotonic())
            if remaining <= 0:
                console.print("[dim]Tiempo de observación agotado.[/dim]")
                return
        stream = watch.Watch()
        try:
            for event in stream.stream(v1.list_namespaced_pod, namespace=args.namespace,
                                       field_selector=f"metadata.name={args.pod}",
                                       timeout_seconds=remaining or 300):
                if event["type"] == "DELETED":
                    console.print(f"[dim]\\[{_now()}][/dim] [yellow]Pod eliminado.[/yellow]")
                    last_sig = last_key = None
                    continue
                if event["type"] not in ("ADDED", "MODIFIED"):
                    continue
                pod = build_snapshot(v1, event["object"], with_logs=not args.no_logs)
                sig = signature(pod)
                if sig == last_sig:
                    continue
                last_sig = sig
                result = diagnose(pod)
                console.print(status_line(pod))
                key = tuple(sorted(f.rule_id for f in result.findings))
                if key != last_key:                       # el diagnóstico cambió (p. ej. ALTA → CRÍTICA)
                    last_key = key
                    render_report(console, pod, result, ctx_name)
        except ApiException as exc:
            if exc.status != 410:                         # 410 Gone: resourceVersion caducado → reabrir watch
                raise
        finally:
            stream.stop()


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sistema Experto de diagnóstico de Pods de Kubernetes")
    p.add_argument("pod", nargs="?", default="oom-demo", help="nombre del Pod (default: oom-demo)")
    p.add_argument("-n", "--namespace", default="default")
    p.add_argument("--context", help="contexto de kubeconfig (default: el activo, p. ej. minikube)")
    p.add_argument("-w", "--watch", action="store_true", help="tiempo real: re-diagnostica en cada cambio de estado")
    p.add_argument("--timeout", type=int, default=0, help="segundos máximos en --watch (0 = hasta Ctrl+C)")
    p.add_argument("--no-logs", action="store_true", help="no leer logs de los contenedores")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    console = Console()
    try:
        v1, ctx_name = connect(args.context)
        if args.watch:
            watch_pod(v1, args, console, ctx_name)
            return 0
        raw = v1.read_namespaced_pod(args.pod, args.namespace)
        pod = build_snapshot(v1, raw, with_logs=not args.no_logs)
        result = diagnose(pod)
        render_report(console, pod, result, ctx_name)
        worst = max((SEVERITY_ORDER[f.severity] for f in result.findings), default=0)
        return 1 if worst >= SEVERITY_ORDER["MEDIA"] else 0
    except ApiException as exc:
        if exc.status == 404:
            console.print(f"[bold red]✘ Pod '{escape(args.pod)}' no encontrado en el namespace '{escape(args.namespace)}'.[/]"
                          "\n  ¿Aplicaste el manifiesto? → kubectl apply -f oom-pod.yaml  (o usa --watch y aplícalo después)")
        else:
            console.print(f"[bold red]✘ Error de la API de Kubernetes ({exc.status}):[/] {escape(str(exc.reason))}")
        return 2
    except ClusterError as exc:
        console.print(f"[bold red]✘ {escape(str(exc))}[/]")
        return 2
    except HTTPError as exc:
        console.print(f"[bold red]✘ No se pudo conectar al cluster:[/] {escape(str(exc))}\n  ¿Está corriendo Minikube? → minikube status")
        return 2
    except KeyboardInterrupt:
        console.print("\n[dim]Observación detenida.[/dim]")
        return 0


if __name__ == "__main__":
    sys.exit(main())
