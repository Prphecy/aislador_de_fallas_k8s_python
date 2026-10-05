# Sistema Experto de Diagnóstico de Fallas en Kubernetes

Prototipo funcional que se conecta a un clúster real (Minikube), lee el estado de un Pod en tiempo real y, mediante un **motor de reglas de inferencia**, entrega un diagnóstico con **Severidad, Categoría, Causa raíz y Remediación**.

El caso de demostración es un Pod que falla a propósito por **OOMKilled** (memoria excedida), pero el sistema no está atado a ese error: razona a partir del estado real del Pod y por eso el diagnóstico cambia según la falla.

---

## 1. Contenido del proyecto

| Archivo | Función |
|---|---|
| `oom-pod.yaml` | Manifiesto de un Pod que falla a propósito por OOMKilled |
| `k8s_expert.py` | Sistema experto (colector + motor de reglas + salida en consola con `rich`) |
| `app.py` | Backend web (FastAPI): expone el mismo sistema experto como API REST + WebSocket |
| `static/index.html`, `static/style.css`, `static/app.js` | Dashboard en vivo que consume esa API |
| `requirements.txt` | Dependencias de Python |
| `README.md` | Este documento |

## 2. Tecnologías

| Componente | Librería | Rol |
|---|---|---|
| Acceso al clúster | `kubernetes` (cliente oficial) | Lee Pods, eventos y logs; observa cambios (Watch API) |
| Motor de inferencia | `experta` | Encadenamiento hacia adelante (forward chaining) sobre hechos y reglas |
| Salida por consola | `rich` | Paneles, tablas y árbol de inferencia (modo CLI) |
| Interfaz web | `FastAPI` + `uvicorn` | API REST + WebSocket que alimentan el dashboard en el navegador |
| Entorno | Minikube + `kubectl` | Clúster local de pruebas |

Hay **dos formas de usar el mismo sistema experto**: por consola (`k8s_expert.py`) o por navegador (`app.py` + `static/`). Ambas llaman exactamente a las mismas funciones de colección y a las mismas reglas de `experta`; lo único que cambia es cómo se presenta el resultado.

---

## 3. Arquitectura

El programa es una tubería (pipeline) de cinco etapas:

```
                                                            ┌─▶ Informe (rich, consola)
 API de K8s ──▶ PodInfo ──▶ Hechos ──▶ Reglas (experta) ───┤
 (kubernetes)  (colector)  (memoria    capa 1: síntomas     └─▶ JSON (FastAPI) ──▶ Dashboard (navegador)
                            de         capa 2: diagnósticos
                            trabajo)   Severidad · Categoría · Causa raíz · Remediación
```

Las primeras tres etapas (colector, hechos, reglas) viven íntegramente en `k8s_expert.py` y son **las mismas** para ambos modos. `app.py` no reimplementa ninguna regla: importa `k8s_expert.py`, llama a sus mismas funciones (`build_snapshot`, `diagnose`, `signature`) y traduce el resultado a JSON en vez de a un panel de `rich`.

1. **Colector.** Lee el Pod con `read_namespaced_pod`, sus eventos con `list_namespaced_event` (filtrados por UID del Pod, para no mezclar Pods con el mismo nombre) y el log con `read_namespaced_pod_log`. Si el contenedor ya se reinició, pide el log de la instancia anterior (`previous=True`), que es la que murió.
2. **Modelo de datos.** Convierte el objeto crudo de la API en estructuras simples (`PodInfo`, `ContainerInfo`, `EventInfo`, `Condition`) que combinan la especificación (límites y solicitudes de memoria) con el estado vivo (razón, código de salida, reinicios, último estado).
3. **Hechos.** Cada dato relevante se inserta en la memoria de trabajo de `experta` como un `Fact` (`ContainerFact`, `PodFact`, `ConditionFact`).
4. **Motor de reglas.** Dos capas de reglas (sección 4).
5. **Presentación.** `rich` dibuja el informe: cabecera del Pod, tabla de contenedores, eventos, un panel por diagnóstico y el árbol de inferencia.

---

## 4. El motor de reglas

Usa **encadenamiento hacia adelante**: parte de los hechos observados y dispara reglas hasta llegar a conclusiones. Se diseñó en dos capas para separar *qué se observa* de *qué significa*.

### Capa 1: Síntomas (salience 1000)

Se ejecutan primero y traducen hechos crudos en síntomas.

| Regla | Detecta |
|---|---|
| `s01` | OOMKilled en el estado actual del contenedor |
| `s02` | OOMKilled en la última terminación (ya se reinició) |
| `s03` | CrashLoopBackOff o Error |
| `s04` | Imagen no descargable (ErrImagePull, ImagePullBackOff, etc.) |
| `s05` | Error de configuración (CreateContainerConfigError, etc.) |
| `s06` | Pod no planificable (`PodScheduled=False`, Unschedulable) |
| `s07` | Pod desalojado (Evicted) |
| `s08` | Contenedor en ejecución pero no Ready |

### Capa 2: Diagnósticos (salience de 110 a -100)

Convierten síntomas en conclusiones. La prioridad (*salience*) va de lo más específico a lo más genérico, y un hecho `Concluded` evita que un mismo contenedor reciba dos diagnósticos.

| Regla | Prioridad | Diagnóstico | Severidad |
|---|---|---|---|
| `d01` | 110 | OOM **sin** límite de memoria (presión del nodo) | ALTA |
| `d02` | 100 | OOM recurrente (3 o más reinicios) | **CRÍTICA** |
| `d03` | 90 | OOMKilled simple | ALTA |
| `d04` | 80 | Imagen no descargable | ALTA |
| `d05` | 80 | Error de configuración | ALTA |
| `d07` | 60 | Pod no planificable | ALTA |
| `d08` | 60 | Pod desalojado | MEDIA |
| `d06` | 50 | Contenedor en falla (crash genérico) | ALTA |
| `d09` | 40 | Contenedor no Ready | MEDIA |
| `d10` | 10 | Pod saludable | INFO |
| `d99` | -100 | Sin patrón conocido (comodín) | BAJA |

Detalles de diseño:
- **Un diagnóstico por contenedor.** Un Pod con dos contenedores puede recibir dos diagnósticos distintos.
- **Saludable y Desconocido nunca conviven** con otros diagnósticos.
- **`d06` excluye los Pods desalojados**, para que una evicción no genere además un "crash" redundante.

### Interpretación adicional

- **Códigos de salida:** 0, 1, 2, 126, 127, 137 (SIGKILL/OOM), 139 (SIGSEGV) y 143 (SIGTERM) se traducen a una explicación legible.
- **Mensajes de eventos:** se buscan palabras clave (por ejemplo `not found`, `Insufficient memory`) para afinar la causa raíz.
- **Límite sugerido:** ante un OOM, la remediación propone un límite de memoria del doble del actual, redondeado a múltiplos de 64Mi.

---

## 5. Requisitos e instalación

**Requisitos:** Python 3.9 o superior, Minikube, `kubectl` y Docker (o el driver de Minikube que uses).

```bash
pip install -r requirements.txt
minikube start
```

> **Nota de compatibilidad:** `experta` 1.9.x depende de `frozendict==1.2`, que no funciona en Python 3.10 o superior porque usa `collections.Mapping`. El script restaura esos alias antes de importar `experta`, así que no hace falta ningún ajuste manual.

---

## 6. Uso

### Paso a paso

```bash
# 1. Desplegar el Pod que falla a propósito
kubectl apply -f oom-pod.yaml

# 2. (Opcional) Ver cómo falla
kubectl get pod oom-demo -w

# 3. Diagnosticar
python k8s_expert.py oom-demo            # diagnóstico puntual
python k8s_expert.py oom-demo --watch    # tiempo real

# 4. Limpiar
kubectl delete -f oom-pod.yaml
```

### Opciones de la línea de comandos

| Opción | Descripción |
|---|---|
| `pod` | Nombre del Pod (por defecto `oom-demo`) |
| `-n`, `--namespace` | Namespace (por defecto `default`) |
| `--context` | Contexto de kubeconfig (por defecto el activo, p. ej. `minikube`) |
| `-w`, `--watch` | Modo tiempo real: vuelve a diagnosticar solo cuando cambia el estado |
| `--timeout N` | Segundos máximos en `--watch` (0 = hasta Ctrl+C) |
| `--no-logs` | No leer los logs de los contenedores |

### Códigos de salida

| Código | Significado |
|---|---|
| `0` | Sin fallas relevantes (severidad menor a MEDIA) |
| `1` | Falla detectada (severidad MEDIA o superior) |
| `2` | Error de ejecución (Pod inexistente, Minikube apagado, etc.) |

Esto permite usar el script dentro de otros scripts o pipelines (`python k8s_expert.py oom-demo || echo "hay falla"`).

### Modo tiempo real

Con `--watch` el script usa la Watch API de Kubernetes. Calcula una huella del estado del Pod y **solo re-diagnostica cuando esa huella cambia**, sin saturar la consola. Si la conexión expira (HTTP 410 Gone), reabre el watch automáticamente.

---

## 7. Interfaz web (dashboard en vivo)

Además de la CLI, el mismo sistema experto se puede usar desde el navegador: un dashboard que se actualiza solo cada vez que cambia el estado del Pod, sin recargar la página.

### 7.1 Arquitectura de la capa web

```
 Navegador                          Servidor (uvicorn)
┌─────────────────────┐            ┌──────────────────────────────┐
│ static/index.html    │  GET /    │                                │
│ static/style.css      │◀─────────│  app.py (FastAPI)              │
│ static/app.js         │           │   GET  /api/namespaces         │
│                        │  REST    │   GET  /api/pods               │
│  <select>/<input> Pod ─┼─────────▶│   GET  /api/diagnose            │
│                        │           │   WS   /ws/watch   ────────────┼──▶ k8s_expert.py
│  Panel de diagnóstico ◀┼───WS─────│     (sondea el Pod cada 2s,     │      (connect, build_snapshot,
│  Tabla de contenedores │  JSON    │      reusa signature()/diagnose)│       signature, diagnose)
│  Historial en vivo     │           │                                │
└─────────────────────┘            └──────────────────────────────┘
```

- **`GET /api/namespaces`** y **`GET /api/pods?namespace=...`** alimentan los campos de Namespace y Pod (son `<input>` con `<datalist>`: se puede escribir el nombre a mano o elegirlo de la lista, igual que pedía la consigna).
- **`GET /api/diagnose?pod=...&namespace=...`** hace un diagnóstico puntual y devuelve JSON (útil para probar la API con `curl` o Postman, sin pasar por el dashboard).
- **`WS /ws/watch?pod=...&namespace=...`** es lo que usa el dashboard: al conectarse, el servidor lee el Pod cada 2 segundos y solo envía un mensaje nuevo cuando algo relevante cambia (mismo criterio que `signature()` en la CLI). Si el Pod todavía no existe, avisa y sigue intentando: si aplicas el manifiesto después de conectar el dashboard, el diagnóstico aparece solo.

> **Nota:** la CLI (`--watch`) usa la *Watch API* de Kubernetes (el clúster empuja los cambios). El dashboard, por simplicidad y para tolerar mejor los cortes de red durante una demo en vivo, **sondea** el Pod cada 2 segundos en vez de suscribirse a esa API. El diagnóstico es idéntico en ambos casos porque usan las mismas funciones; solo cambia cómo se detecta que algo cambió.

### 7.2 Instalación y ejecución

```bash
pip install -r requirements.txt   # agrega fastapi y uvicorn
python app.py                     # sirve el dashboard en http://localhost:8000
```

`app.py` debe estar en la misma carpeta que `k8s_expert.py` (lo importa directamente; no duplica ninguna regla) y la carpeta `static/` debe estar junto a `app.py`.

### 7.3 Qué muestra el dashboard

| Elemento | Contenido |
|---|---|
| Franja superior (hero) | Severidad actual, Pod, fase, nodo y hora de la última lectura. Cambia de color según la severidad |
| Tarjetas de diagnóstico | Una por hallazgo: Severidad, Categoría, Causa raíz y Remediación (los comandos `kubectl` se pueden copiar con un clic) |
| Cadena de inferencia | Los mismos hechos → síntomas → diagnóstico que muestra el árbol de la CLI |
| Contenedores / Eventos | Las mismas tablas que imprime `render_report()` en consola |
| Historial en vivo | Línea de tiempo con cada cambio de diagnóstico detectado (por ejemplo ALTA → CRÍTICA), con hora |

### 7.4 Para la demo

1. Abre `http://localhost:8000`, escribe o elige `oom-demo` y pulsa **Conectar** *antes* de aplicar el manifiesto: verás el aviso "Pod no encontrado" en vivo.
2. En otra terminal: `kubectl apply -f oom-pod.yaml`. El dashboard detecta el Pod solo, sin recargar la página.
3. Observa la franja superior pasar de verde (saludable, mientras arranca) a roja (CRÍTICA) a medida que se acumulan los reinicios, y cómo el historial en vivo va quedando como registro de esos cambios.
4. Sube el límite de memoria y recrea el Pod (sección 8): el dashboard vuelve solo a INFO.

## 8. Cómo funciona el caso OOMKilled

1. `oom-pod.yaml` lanza `stress`, que intenta reservar **250 MiB** de RAM.
2. El contenedor tiene un límite de **100 MiB** (`resources.limits.memory`).
3. El *OOM killer* del kernel lo mata con SIGKILL (código de salida **137**).
4. El kubelet lo reinicia con espera creciente (10 s, 20 s, 40 s... hasta 5 min): el Pod queda en **CrashLoopBackOff**.
5. El sistema experto observa `lastState.terminated.reason = OOMKilled`, código 137 y varios reinicios, y concluye **`d02` OOM recurrente, severidad CRÍTICA**.

Para "arreglarlo" y ver al sistema reportar un Pod sano: sube `limits.memory` a `300Mi` en el YAML, y luego `kubectl delete pod oom-demo && kubectl apply -f oom-pod.yaml` (los recursos de un Pod suelto son inmutables, por eso hay que recrearlo).

---

## 9. Salida del sistema

El informe incluye, en orden:

1. **Cabecera del Pod:** nombre, namespace, fase y nodo.
2. **Tabla de contenedores:** estado, reinicios, código de salida y límites.
3. **Tabla de eventos:** los más recientes del Pod.
4. **Panel de diagnóstico** (uno por hallazgo, con color según severidad):
   - **Severidad**
   - **Categoría**
   - **Causa raíz**
   - **Remediación** (pasos con comandos `kubectl` concretos)
   - **Evidencia** que sustenta la conclusión
5. **Árbol de inferencia:** la cadena hechos → síntomas → diagnóstico que siguió el motor.

Severidades: `CRÍTICA` > `ALTA` > `MEDIA` > `BAJA` > `INFO`.

---

## 10. Pruebas realizadas

Durante el desarrollo se validaron 16 escenarios de fallo, todos con el resultado esperado:

| Escenario | Diagnóstico |
|---|---|
| OOM recurrente (CrashLoopBackOff, 5 reinicios) | `d02` · CRÍTICA |
| Primer OOMKilled | `d03` · ALTA |
| OOM ya reiniciado y actualmente Running | `d03` · ALTA |
| OOM sin límite de memoria | `d01` · ALTA |
| CrashLoopBackOff con código 1 y con código 127 | `d06` · ALTA |
| Imagen inexistente (ErrImagePull) | `d04` · ALTA |
| ConfigMap faltante | `d05` · ALTA |
| Memoria insuficiente en el clúster | `d07` · ALTA |
| Pod desalojado | `d08` · MEDIA |
| En ejecución pero no Ready | `d09` · MEDIA |
| Pod sano y Job completado | `d10` · INFO |
| ContainerCreating transitorio | `d99` · BAJA |
| Dos contenedores, uno con OOM y otro sano | `d02` · CRÍTICA |
| Dos contenedores, uno con OOM y otro con imagen inválida | `d03` + `d04` |
| Init container fallando | `d06` · ALTA |

También se probó el flujo completo de `main()` con respuestas de API simuladas: Pod con OOM (código 1), Pod inexistente (404, código 2), clúster apagado (código 2) y Pod sano (código 0).

**Capa web.** Con el mismo enfoque (dobles de la API de Kubernetes, nunca un clúster real), se verificó que:
- los tres endpoints REST (`/api/namespaces`, `/api/pods`, `/api/diagnose`) devuelven el JSON esperado y traducen los errores de conexión al mismo código/mensaje que usa la CLI;
- el reporte completo (Pod + hallazgos + evidencia + árbol de inferencia) se serializa a JSON sin pérdidas para varios escenarios de falla distintos;
- el bucle del WebSocket, ejecutado de principio a fin de forma asíncrona, pasa correctamente por las cuatro transiciones: Pod no encontrado → aparece sano → sin cambios (heartbeat) → pasa a OOM recurrente (CRÍTICA) → el cliente se desconecta y el servidor lo maneja sin caerse.

No se probó contra un Minikube real (el entorno donde se construyó no tiene red para instalar `fastapi`/`uvicorn`), así que vale la misma advertencia que para la CLI: ejecuta `python app.py` contra tu clúster antes de la presentación para confirmar que todo corre igual que en las pruebas.

---

## 11. Limitaciones conocidas

- Analiza **un Pod a la vez**, tanto en la CLI como en el dashboard (una conexión WebSocket = un Pod).
- El dashboard **sondea** el Pod cada 2 segundos en vez de usar la Watch API (ver sección 7.1); con un intervalo tan corto no debería notarse en una demo, pero es menos eficiente que el modo `--watch` de la CLI si se deja abierto mucho tiempo.
- Las reglas actuales cubren fallas de memoria, imagen, configuración, planificación, evicción y readiness. No cubre aún probes fallando, volúmenes pendientes (PVC) ni problemas de red.
- Se basa en el **estado y los eventos** del Pod; no consulta métricas de uso real (CPU/memoria en vivo).
- La base de conocimiento vive dentro del script; agregar reglas implica editar el código.

## 12. Mejoras propuestas

1. Manifiestos de demo adicionales: imagen inexistente, ConfigMap faltante y recursos imposibles de planificar.
2. Modo/dashboard multi-Pod: filtro por etiqueta (`-l app=x`) y una vista con todos los Pods de un namespace a la vez, ordenados por severidad.
3. Nuevas reglas: probes de liveness/readiness, PVC pendiente, presión de disco o memoria en el nodo.
4. Comparar uso real contra el límite con `metrics-server` para anticipar OOMs.
5. Diagnóstico a nivel de Deployment/ReplicaSet.
6. Exportar el informe a JSON o Markdown (la API ya lo hace vía `/api/diagnose`; falta un botón de descarga en el dashboard).
7. Pruebas automatizadas con `pytest`.
8. Cambiar el sondeo del dashboard por la Watch API (como ya hace la CLI), para que sea push en vez de polling.

---

## 13. Glosario

| Término | Significado |
|---|---|
| **OOMKilled** | El kernel mató el proceso por superar el límite de memoria de su cgroup |
| **CrashLoopBackOff** | El contenedor se reinicia una y otra vez con espera creciente |
| **Exit code 137** | 128 + 9 (SIGKILL); típico de OOMKilled |
| **Forward chaining** | Técnica de inferencia que parte de los hechos y aplica reglas hasta concluir |
| **Salience** | Prioridad de una regla cuando varias pueden dispararse a la vez |
| **Hecho (Fact)** | Dato almacenado en la memoria de trabajo del motor |
| **Watch API** | Mecanismo de Kubernetes que notifica cambios de estado en tiempo real |
| **WebSocket** | Conexión persistente entre navegador y servidor que permite enviar mensajes en ambas direcciones sin recargar la página; es lo que usa el dashboard para actualizarse solo |
| **Polling (sondeo)** | Preguntar por el estado a intervalos fijos (aquí, cada 2s), en vez de que el servidor avise por su cuenta (push) |
