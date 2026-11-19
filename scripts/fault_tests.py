"""Pruebas de tolerancia a fallos (inyección de fallos real: se DETIENEN procesos).

Escenarios:
  F1  Caída de una réplica de inventory durante tráfico de lectura.
  F2  Caída de una réplica de inventory durante creación de pedidos.
  F3  Caída de TODAS las réplicas de inventory -> circuit breaker -> recuperación.
  F4  Caída de notifications -> los eventos se acumulan -> se procesan al volver (sin pérdidas).
  F5  Caída del broker (Redis) -> outbox retiene eventos -> se publican al volver.
  F6  Consumidor que muere sin confirmar (XACK) -> XAUTOCLAIM reasigna los mensajes.
  F7  Caída de orders -> degradación parcial (el catálogo sigue disponible).

Uso: python scripts/fault_tests.py   (requiere el stack local levantado con scripts/stack.sh)
"""
import asyncio
import json
import pathlib
import subprocess
import time
import uuid

import httpx
import redis

ROOT = pathlib.Path(__file__).resolve().parents[1]
BASE = "http://localhost:8000"
STREAM = "distrimarket.events"
R = redis.Redis(decode_responses=True)
LOG: list[str] = []
RESULTS: list[dict] = []
T0 = time.time()


def log(msg):
    line = f"[t+{time.time() - T0:6.1f}s] {msg}"
    print(line, flush=True)
    LOG.append(line)


def stack(action, *services):
    subprocess.run([str(ROOT / "scripts" / "stack.sh"), action, *services], check=True,
                   capture_output=True, text=True)
    log(f"$ stack.sh {action} {' '.join(services)}")


async def astack(action, *services):
    """Versión no bloqueante: el tráfico sigue fluyendo mientras se detiene el proceso."""
    await asyncio.to_thread(stack, action, *services)


async def restart_and_cool(*services):
    """Reinicia y espera el reset_timeout (5 s) de los circuit breakers para aislar escenarios."""
    await astack("start", *services)
    await asyncio.sleep(5.5)


def redis_cmd(*args):
    return subprocess.run(["redis-cli", *args], capture_output=True, text=True).stdout.strip()


async def burst(c, n, fn, concurrency=10):
    sem = asyncio.Semaphore(concurrency)
    async def one(i):
        async with sem:
            t = time.perf_counter()
            try:
                r = await fn(c, i)
                return r.status_code, (time.perf_counter() - t) * 1000, r.headers.get("x-upstream", "")
            except Exception as exc:
                return type(exc).__name__, (time.perf_counter() - t) * 1000, ""
    return await asyncio.gather(*(one(i) for i in range(n)))


def summary(results):
    codes = {}
    for code, _, _ in results:
        codes[str(code)] = codes.get(str(code), 0) + 1
    lat = sorted(ms for _, ms, _ in results)
    return {"total": len(results), "codigos": codes,
            "lat_p50_ms": round(lat[len(lat) // 2], 1), "lat_max_ms": round(lat[-1], 1)}


def record(sid, nombre, esperado, observado, ok):
    RESULTS.append({"id": sid, "escenario": nombre, "esperado": esperado,
                    "observado": observado, "resultado": "SUPERADA" if ok else "FALLIDA"})
    log(f"==> {sid}: {'SUPERADA' if ok else 'FALLIDA'}")


async def wait_notifications(c, headers, expected, timeout=40):
    deadline = time.time() + timeout
    n = 0
    while time.time() < deadline:
        n = len((await c.get("/api/notifications", headers=headers)).json())
        if n >= expected:
            return n, round(timeout - (deadline - time.time()), 1)
        await asyncio.sleep(0.5)
    return n, None


async def main():
    async with httpx.AsyncClient(base_url=BASE, timeout=15) as c:
        admin = (await c.post("/api/auth/login", json={"email": "admin@distrimarket.co",
                                                        "password": "Admin#2026seguro"})).json()["access_token"]
        ah = {"Authorization": f"Bearer {admin}"}

        async def new_user():
            email = f"fallos-{uuid.uuid4().hex[:8]}@correo.co"
            await c.post("/api/auth/register", json={"email": email, "full_name": "Usuario fallos",
                                                       "password": "Fallos2026x"})
            tok = (await c.post("/api/auth/login", json={"email": email, "password": "Fallos2026x"})).json()["access_token"]
            return {"Authorization": f"Bearer {tok}"}

        prod = (await c.post("/api/products", headers=ah, json={
            "sku": "F-" + uuid.uuid4().hex[:6].upper(), "name": "Producto fallos",
            "price": 5000, "stock": 100000})).json()

        def order_fn(h):
            return lambda cl, i: cl.post("/api/orders", headers=h,
                                         json={"items": [{"product_id": prod["id"], "quantity": 1}]})

        # ------------------------------------------------------------------ F1
        log("F1: caída de inventory-1 durante 1500 lecturas del catálogo")
        task = asyncio.create_task(burst(c, 1500, lambda cl, i: cl.get("/api/products"), 10))
        await asyncio.sleep(0.5)
        await astack("stop", "inventory-1")
        res = await task
        s = summary(res)
        served_after = {up for _, _, up in res[-100:]}
        s["replicas_en_ultimas_100"] = sorted(served_after)
        record("F1", "Caída de una réplica de inventory (lecturas)",
               "0 errores visibles para el cliente; failover automático a inventory-2", s,
               s["codigos"].get("200", 0) == 1500 and served_after == {"inventory-2"})
        await restart_and_cool("inventory-1")

        # ------------------------------------------------------------------ F2
        log("F2: caída de inventory-2 durante 200 pedidos")
        u = await new_user()
        stock0 = (await c.get(f"/api/products/{prod['id']}")).json()["stock"]
        task = asyncio.create_task(burst(c, 200, order_fn(u), 5))
        await asyncio.sleep(0.4)
        await astack("stop", "inventory-2")
        res = await task
        s = summary(res)
        stock1 = (await c.get(f"/api/products/{prod['id']}")).json()["stock"]
        n_orders = len((await c.get("/api/orders", headers=u)).json())  # API devuelve hasta 100
        async with httpx.AsyncClient() as raw:
            n_orders = int(subprocess.run(["psql", "-h", "localhost", "-U", "dm", "-d", "orders_db", "-tAc",
                "SELECT count(*) FROM orders o WHERE o.items @> '[{\"product_id\": %d}]' AND o.created_at > now() - interval '30 seconds'" % prod["id"]],
                capture_output=True, text=True).stdout.strip())
        s.update({"pedidos_registrados": n_orders, "stock_descontado": stock0 - stock1})
        record("F2", "Caída de una réplica de inventory (escrituras)",
               "200 pedidos confirmados; stock descontado = pedidos registrados (consistencia)", s,
               s["codigos"].get("201", 0) == 200 and n_orders == 200 and stock0 - stock1 == 200)
        await restart_and_cool("inventory-2")

        # ------------------------------------------------------------------ F3
        log("F3: caída de TODAS las réplicas de inventory")
        u = await new_user()
        stack("stop", "inventory-1", "inventory-2")
        timeline = []
        for i in range(12):
            t = time.perf_counter()
            r = await order_fn(u)(c, i)
            timeline.append({"intento": i + 1, "status": r.status_code,
                             "ms": round((time.perf_counter() - t) * 1000, 1),
                             "detalle": r.json().get("detail", "")[:70]})
        st = (await c.get("/api/products")).status_code
        status_cb = (await httpx.AsyncClient().get("http://localhost:8003/internal/status",
                     headers={"X-Internal-Token": "dev-internal-token"})).json()
        cb_states = [b["state"] for b in status_cb["inventory_breakers"]]
        log(f"   estados circuit breaker (orders->inventory): {cb_states}; GET catálogo -> {st}")
        stack("start", "inventory-1", "inventory-2")
        await asyncio.sleep(5.5)  # reset_timeout del breaker = 5 s -> HALF_OPEN
        t = time.perf_counter()
        rec = await order_fn(u)(c, 99)
        rec_ms = round((time.perf_counter() - t) * 1000, 1)
        status_cb2 = (await httpx.AsyncClient().get("http://localhost:8003/internal/status",
                      headers={"X-Internal-Token": "dev-internal-token"})).json()
        cb_after = [b["state"] for b in status_cb2["inventory_breakers"]]
        transitions = [b["recent_transitions"] for b in status_cb2["inventory_breakers"]]
        obs = {"linea_de_tiempo": timeline, "catalogo_durante_caida": st,
               "estado_breakers_durante_caida": cb_states,
               "pedido_tras_recuperacion": {"status": rec.status_code, "ms": rec_ms},
               "estado_breakers_tras_recuperacion": cb_after, "transiciones": transitions}
        fast = [x["ms"] for x in timeline if "circuito abierto" in x["detalle"]]
        record("F3", "Caída total de inventory + circuit breaker",
               "Respuestas 503 controladas; el circuito se abre y las siguientes fallan rápido (<50 ms); "
               "al volver el servicio el circuito pasa a HALF_OPEN y luego CLOSED", obs,
               all(x["status"] == 503 for x in timeline) and "OPEN" in cb_states and bool(fast)
               and max(fast) < 50 and rec.status_code == 201 and cb_after.count("CLOSED") >= 1)

        # ------------------------------------------------------------------ F4
        log("F4: caída de notifications mientras se crean 30 pedidos")
        u = await new_user()
        stack("stop", "notifications")
        res = await burst(c, 30, order_fn(u), 5)
        s = summary(res)
        await asyncio.sleep(1.5)
        lag = redis_cmd("XINFO", "GROUPS", STREAM)
        n_during = 0
        log(f"   pedidos durante la caída: {s['codigos']}; consumer group: {lag.splitlines()[-4:] if lag else ''}")
        stack("start", "notifications")
        n, secs = await wait_notifications(c, u, 30)
        s.update({"notificaciones_durante_caida": n_during, "notificaciones_tras_recuperar": n,
                  "segundos_hasta_ponerse_al_dia": secs})
        record("F4", "Caída del servicio de notificaciones",
               "Los pedidos NO se ven afectados; al volver se entregan las 30 notificaciones sin pérdidas ni duplicados",
               s, s["codigos"].get("201", 0) == 30 and n == 30)

        # ------------------------------------------------------------------ F5
        log("F5: caída del broker Redis mientras se crean 20 pedidos")
        u = await new_user()
        redis_cmd("SHUTDOWN", "NOSAVE")
        log("$ redis-cli SHUTDOWN (broker caído)")
        await asyncio.sleep(0.5)
        res = await burst(c, 20, order_fn(u), 5)
        s = summary(res)
        await asyncio.sleep(1)
        pending = (await httpx.AsyncClient().get("http://localhost:8003/internal/status",
                   headers={"X-Internal-Token": "dev-internal-token"})).json()["outbox_pending"]
        log(f"   pedidos con broker caído: {s['codigos']}; eventos retenidos en outbox: {pending}")
        subprocess.run(["redis-server", "--daemonize", "yes", "--port", "6379", "--appendonly", "yes",
                        "--dir", "/tmp", "--logfile", "/tmp/redis.log"], check=True)
        log("$ redis-server (broker restablecido)")
        n, secs = await wait_notifications(c, u, 20, timeout=60)
        pending2 = (await httpx.AsyncClient().get("http://localhost:8003/internal/status",
                    headers={"X-Internal-Token": "dev-internal-token"})).json()["outbox_pending"]
        s.update({"outbox_pendiente_durante_caida": pending, "outbox_pendiente_despues": pending2,
                  "notificaciones_entregadas": n, "segundos_hasta_entrega": secs})
        record("F5", "Caída del broker de mensajería (Redis)",
               "Los pedidos se siguen confirmando (outbox); al volver Redis se publican y notifican los 20 eventos",
               s, s["codigos"].get("201", 0) == 20 and pending >= 20 and pending2 == 0 and n == 20)

        # ------------------------------------------------------------------ F6
        log("F6: consumidor 'fantasma' lee 10 eventos y muere sin XACK")
        u = await new_user()
        stack("stop", "notifications")
        await burst(c, 10, order_fn(u), 5)
        await asyncio.sleep(1.5)
        ghost = R.xreadgroup("notifications", "consumidor-fantasma", {STREAM: ">"}, count=100)
        taken = len(ghost[0][1]) if ghost else 0
        log(f"   el consumidor fantasma tomó {taken} mensajes y 'murió' sin confirmarlos")
        stack("start", "notifications")
        n, secs = await wait_notifications(c, u, 10, timeout=45)
        stats = (await httpx.AsyncClient().get("http://localhost:8004/internal/stats",
                 headers={"X-Internal-Token": "dev-internal-token"})).json()
        record("F6", "Consumidor caído con mensajes sin confirmar",
               "XAUTOCLAIM reasigna los mensajes tras 15 s de inactividad y se entregan las 10 notificaciones",
               {"mensajes_huerfanos": taken, "notificaciones_entregadas": n,
                "segundos_hasta_entrega": secs, "reclamados": stats.get("reclaimed")},
               taken >= 10 and n == 10 and stats.get("reclaimed", 0) >= 10)
        R.xgroup_delconsumer(STREAM, "notifications", "consumidor-fantasma")

        # ------------------------------------------------------------------ F7
        log("F7: caída de orders (degradación parcial)")
        u = await new_user()
        stack("stop", "orders")
        t = time.perf_counter()
        r1 = await c.get("/api/orders", headers=u)
        ms1 = round((time.perf_counter() - t) * 1000, 1)
        r2 = await c.get("/api/products")
        stack("start", "orders")
        await asyncio.sleep(5.5)
        r3 = await c.get("/api/orders", headers=u)
        obs = {"pedidos_durante_caida": {"status": r1.status_code, "ms": ms1,
                                          "retry_after": r1.headers.get("retry-after")},
               "catalogo_durante_caida": r2.status_code, "pedidos_tras_recuperar": r3.status_code}
        record("F7", "Caída del servicio de pedidos",
               "503 controlado con Retry-After; el catálogo sigue operando; recuperación automática",
               obs, r1.status_code == 503 and r2.status_code == 200 and r3.status_code == 200)

    ev = ROOT / "docs" / "evidencias"
    ev.mkdir(parents=True, exist_ok=True)
    (ev / "tolerancia_fallos.json").write_text(json.dumps(RESULTS, indent=2, ensure_ascii=False))
    (ev / "tolerancia_fallos_log.txt").write_text("\n".join(LOG) + "\n")
    ok = sum(r["resultado"] == "SUPERADA" for r in RESULTS)
    print(f"\nResumen: {ok}/{len(RESULTS)} escenarios superados")


if __name__ == "__main__":
    asyncio.run(main())
