// Cliente web de DistriMarket: consume exclusivamente el API Gateway.
const $ = (s) => document.querySelector(s);
let token = sessionStorage.getItem("dm_token");
let user = JSON.parse(sessionStorage.getItem("dm_user") || "null");
let products = [];
const money = (v) => "$" + Number(v).toLocaleString("es-CO", { maximumFractionDigits: 0 });
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

async function api(path, opts = {}) {
  const headers = { "Content-Type": "application/json", ...(opts.headers || {}) };
  if (token) headers.Authorization = "Bearer " + token;
  const res = await fetch(path, { ...opts, headers });
  const data = await res.json().catch(() => ({}));
  if (res.status === 401 && token) logout();
  if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail || data));
  return { data, res };
}

function render() {
  $("#auth-view").hidden = !!token;
  $("#app-view").hidden = !token;
  $("#session").innerHTML = token
    ? `${esc(user.full_name)} <small>(${esc(user.role)})</small> <button class="ghost" id="logout">salir</button>` : "";
  if (token) { $("#logout").onclick = logout; loadAll(); }
}
function logout() { token = null; user = null; sessionStorage.clear(); render(); }

$("#login-form").onsubmit = async (e) => {
  e.preventDefault();
  const f = new FormData(e.target);
  try {
    const { data } = await api("/api/auth/login", { method: "POST", body: JSON.stringify(Object.fromEntries(f)) });
    token = data.access_token; user = data.user;
    sessionStorage.setItem("dm_token", token); sessionStorage.setItem("dm_user", JSON.stringify(user));
    render();
  } catch (err) { $("#auth-msg").className = "msg err"; $("#auth-msg").textContent = err.message; }
};
$("#register-form").onsubmit = async (e) => {
  e.preventDefault();
  try {
    await api("/api/auth/register", { method: "POST", body: JSON.stringify(Object.fromEntries(new FormData(e.target))) });
    $("#auth-msg").className = "msg ok"; $("#auth-msg").textContent = "Cuenta creada. Ya puede ingresar.";
  } catch (err) { $("#auth-msg").className = "msg err"; $("#auth-msg").textContent = err.message; }
};

async function loadProducts() {
  const { data, res } = await api("/api/products");
  products = data;
  $("#served-by").textContent = "atendido por réplica: " + (res.headers.get("X-Upstream") || "?");
  $("#products tbody").innerHTML = data.map((p) => `<tr><td>${esc(p.name)}</td><td class="num">${money(p.price)}</td>
    <td class="num">${p.stock}</td><td><input type="number" min="0" max="${p.stock}" value="0" data-id="${p.id}"></td></tr>`).join("");
  document.querySelectorAll("#products input").forEach((i) => (i.oninput = updateTotal));
  updateTotal();
}
function cart() {
  return [...document.querySelectorAll("#products input")]
    .map((i) => ({ product_id: +i.dataset.id, quantity: +i.value })).filter((i) => i.quantity > 0);
}
function updateTotal() {
  const t = cart().reduce((s, i) => s + products.find((p) => p.id === i.product_id).price * i.quantity, 0);
  $("#cart-total").textContent = money(t);
}
$("#order-btn").onclick = async () => {
  const items = cart();
  const msg = $("#order-msg");
  if (!items.length) { msg.className = "msg err"; msg.textContent = "Seleccione al menos un producto."; return; }
  try {
    const key = crypto.randomUUID();  // Idempotency-Key: un doble clic no duplica el pedido
    const { data } = await api("/api/orders", { method: "POST", body: JSON.stringify({ items }), headers: { "Idempotency-Key": key } });
    msg.className = "msg ok"; msg.textContent = `Pedido #${data.id} confirmado por ${money(data.total)}.`;
    await loadAll();
    setTimeout(loadNotifications, 800);
  } catch (err) { msg.className = "msg err"; msg.textContent = err.message; }
};
async function loadOrders() {
  const { data } = await api("/api/orders");
  $("#orders").innerHTML = data.map((o) => `<li><span class="pill ${o.status === "CONFIRMED" ? "up" : "down"}">${o.status}</span>
    #${o.id} · ${money(o.total)} · ${o.items.length} ítem(s)</li>`).join("") || "<li>Sin pedidos</li>";
}
async function loadNotifications() {
  const { data } = await api("/api/notifications");
  $("#notifications").innerHTML = data.map((n) => `<li><strong>${esc(n.subject)}</strong><br>${esc(n.body)}</li>`).join("") || "<li>Sin notificaciones</li>";
}
async function loadAll() { await Promise.allSettled([loadProducts(), loadOrders(), loadNotifications(), loadStatus()]); }

async function loadStatus() {
  const { data } = await api("/api-status");
  const rows = Object.entries(data.services).flatMap(([svc, list]) => list.map((i) =>
    `<tr><td>${svc}</td><td>${esc(i.url)}</td><td><span class="pill ${i.up ? "up" : "down"}">${i.up ? "activo" : "caído"}</span></td><td class="num">${i.ms ?? "-"} ms</td>
     <td><span class="pill ${data.circuit_breakers.find((c) => c.name === i.url).state}">${data.circuit_breakers.find((c) => c.name === i.url).state}</span></td></tr>`));
  $("#status").innerHTML = `<table><thead><tr><th>Servicio</th><th>Instancia</th><th>Salud</th><th>Latencia</th><th>Circuit breaker</th></tr></thead>
    <tbody>${rows.join("")}<tr><td>broker</td><td>Redis Streams</td><td><span class="pill ${data.redis ? "up" : "down"}">${data.redis ? "activo" : "caído"}</span></td><td></td><td></td></tr></tbody></table>`;
}
$("#refresh-status").onclick = loadStatus;
render(); loadStatus();
