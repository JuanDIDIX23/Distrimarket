"""Captura evidencias visuales del cliente web (Playwright + Chrome headless)."""
import pathlib
import subprocess
import time

from playwright.sync_api import sync_playwright

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "evidencias" / "capturas"
OUT.mkdir(parents=True, exist_ok=True)
BASE = "http://localhost:8000"

with sync_playwright() as p:
    b = p.chromium.launch(executable_path="/usr/bin/google-chrome", args=["--no-sandbox"])
    page = b.new_page(viewport={"width": 1280, "height": 900}, device_scale_factor=1.5)
    page.goto(BASE)
    page.wait_for_selector("#status table")
    page.screenshot(path=OUT / "01_login_y_estado.png", full_page=True)

    page.click("#login-form button")
    page.wait_for_selector("#products tbody tr")
    page.fill('#products input[data-id="1"]', "2")
    page.fill('#products input[data-id="5"]', "1")
    page.dispatch_event('#products input[data-id="5"]', "input")
    page.screenshot(path=OUT / "02_catalogo_carrito.png", full_page=True)

    page.click("#order-btn")
    page.wait_for_selector("#order-msg.ok")
    time.sleep(1.5)
    page.evaluate("loadNotifications()")
    time.sleep(0.5)
    page.screenshot(path=OUT / "03_pedido_confirmado_notificacion.png", full_page=True)

    # Caída de una réplica: el panel lo refleja y el catálogo sigue funcionando.
    subprocess.run([str(ROOT / "scripts/stack.sh"), "stop", "inventory-2"], check=True, capture_output=True)
    for _ in range(4):
        page.evaluate("loadProducts()")
        time.sleep(0.2)
    page.evaluate("loadStatus()")
    time.sleep(1.2)
    page.screenshot(path=OUT / "04_replica_caida_failover.png", full_page=True)
    subprocess.run([str(ROOT / "scripts/stack.sh"), "start", "inventory-2"], check=True, capture_output=True)
    b.close()
print("capturas en", OUT)
