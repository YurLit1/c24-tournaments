const API = "http://localhost:8080";

const players = Array.from({ length: 10 }, (_, i) => ({
  id: `00000000-0000-0000-0000-${String(i + 1).padStart(12, "0")}`,
  name: `player${i + 1}`,
}));

let selectedTournament = null;

async function api(path, { method = "GET", body, userId } = {}) {
  const headers = {};
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (userId) headers["X-User-Id"] = userId;
  let r;
  try {
    r = await fetch(API + path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  } catch {
    throw { status: 0, title: "Сервер недоступний", detail: `Немає відповіді від ${API}` };
  }
  const isJson = (r.headers.get("Content-Type") || "").includes("json");
  const data = isJson ? await r.json() : null;
  if (!r.ok) {
    throw { status: r.status, title: data?.title ?? r.statusText, detail: data?.detail ?? "" };
  }
  return { data, location: r.headers.get("Location"), requestId: r.headers.get("X-Request-ID") };
}

function money(minor, currency) {
  return `${(minor / 100).toFixed(2)} ${currency}`;
}

function when(iso, zone) {
  return new Date(iso).toLocaleString("uk-UA", { timeZone: zone || "Europe/Kyiv" });
}

function show(kind, text) {
  const el = document.getElementById("result");
  el.className = kind;
  el.textContent = text;
}

async function loadTournaments() {
  const body = document.getElementById("tournaments");
  try {
    const { data } = await api("/v1/tournaments");
    const items = data?.items ?? [];
    body.innerHTML = "";
    for (const t of items) {
      const tr = document.createElement("tr");
      tr.className = "pick" + (t.id === selectedTournament ? " selected" : "");
      for (const text of [t.game, when(t.starts_at, t.time_zone), `${t.available} / ${t.slots}`, money(t.fee_minor, t.currency)]) {
        const td = document.createElement("td");
        td.textContent = text;
        tr.appendChild(td);
      }
      tr.addEventListener("click", () => {
        selectedTournament = t.id;
        for (const row of body.querySelectorAll("tr")) row.classList.remove("selected");
        tr.classList.add("selected");
      });
      body.appendChild(tr);
    }
    if (!items.length) body.innerHTML = '<tr><td colspan="4">Турнірів немає</td></tr>';
  } catch (p) {
    body.innerHTML = '<tr><td colspan="4">Не вдалося завантажити</td></tr>';
    show("err", `Помилка ${p.status}: ${p.title}\n${p.detail}`);
  }
}

async function register() {
  if (!selectedTournament) {
    show("err", "Оберіть турнір у таблиці");
    return;
  }
  const userId = document.getElementById("player").value;
  const nickname = document.getElementById("nickname").value;
  try {
    const { data, location, requestId } = await api("/v1/registrations", {
      method: "POST",
      userId,
      body: { tournament_id: selectedTournament, nickname },
    });
    show("ok",
      `Реєстрацію створено (201)\n` +
      `id: ${data.id}\n` +
      `статус: ${data.status}\n` +
      `сплатити внесок до: ${when(data.expires_at)}\n` +
      `Location: ${location}\n` +
      `X-Request-ID: ${requestId}`);
  } catch (p) {
    show("err", `Помилка ${p.status}: ${p.title}\n${p.detail}`);
  }
  await loadTournaments();
}

function init() {
  const select = document.getElementById("player");
  for (const p of players) {
    const opt = document.createElement("option");
    opt.value = p.id;
    opt.textContent = `${p.name} (${p.id})`;
    select.appendChild(opt);
  }
  document.getElementById("register").addEventListener("click", register);
  document.getElementById("reload").addEventListener("click", loadTournaments);
  loadTournaments();
}

init();
