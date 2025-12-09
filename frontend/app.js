/* =========================
   CleanMind AI – Frontend JS
   ========================= */

/* ====== SETTINGS ====== */
const API_BASE_URL = "http://127.0.0.1:8000"; // FastAPI backend
let datasetId = null;
let cleanedId = null;

/* Allowed file extensions for this tool (tabular data only) */
const ALLOWED_EXT_REGEX = /\.(csv|tsv|xlsx|xls|json|parquet)$/i;

/* ====== Toast / Alert ====== */
function alertBox(msg, type = "success") {
  const t = document.getElementById("toast");
  if (!t) return;

  t.style.borderLeftColor = type === "error" ? "#ef4444" : "#22c55e";
  t.textContent = msg;
  t.classList.remove("hidden");
  t.classList.add("flex");

  clearTimeout(t._hideTimer);
  t._hideTimer = setTimeout(() => {
    t.classList.add("hidden");
    t.classList.remove("flex");
  }, 3000);
}

/* ====== Generic API helper ====== */
async function api(path, options = {}) {
  const opts = { ...options };
  opts.headers = opts.headers || {};
  opts.headers["Accept"] = "application/json";

  // If "json" key provided, send as JSON
  if (opts.json) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(opts.json);
    delete opts.json;
  }

  const res = await fetch(API_BASE_URL + path, opts);

  if (!res.ok) {
    let msg = `Request failed: ${res.status}`;
    try {
      const err = await res.json();
      if (err?.detail) {
        msg =
          typeof err.detail === "string"
            ? err.detail
            : JSON.stringify(err.detail);
      }
    } catch (_) {
      // ignore JSON parse error
    }
    throw new Error(msg);
  }

  // Some endpoints (like download) may not return JSON
  const contentType = res.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    return res;
  }
  return res.json();
}

/* ====== NAVBAR RENDER ====== */
function renderNavbar() {
  const nav = document.getElementById("navbar");
  if (!nav) return;

  nav.innerHTML = `
    <div class="max-w-6xl mx-auto py-4 flex justify-between items-center px-6">
      <div class="flex items-center gap-2">
        <div
          class="h-7 w-7 rounded-xl bg-gradient-to-tr from-emerald-400 to-cyan-500
                 flex items-center justify-center text-slate-950 text-lg font-black">
          C
        </div>
        <div>
          <p class="text-sm sm:text-base font-semibold text-slate-100">CleanMind AI</p>
          <p class="text-[11px] text-slate-400 -mt-1">Smart Data Cleaning Studio</p>
        </div>
      </div>

      <div class="hidden sm:flex items-center gap-2 text-[11px] text-slate-400">
        <span class="px-3 py-1 rounded-full bg-slate-800/70 border border-slate-700">
          Tabular Data Cleaner
        </span>
      </div>
    </div>
  `;
}

/* ====== DASHBOARD UI ====== */
function renderDashboard() {
  const app = document.getElementById("app-section");
  if (!app) return;

  app.innerHTML = `
    <div class="max-w-6xl mx-auto px-6 pb-10 pt-4 flex flex-col gap-6">

      <!-- Upload card -->
      <div class="glass p-6">
        <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h2 class="text-sm sm:text-base font-semibold text-slate-100">
              Upload & Clean Dataset
            </h2>
            <p class="text-xs text-slate-400 mt-1 max-w-xl">
              Upload CSV, Excel, JSON or Parquet. CleanMind AI will profile, score quality,
              run cleaning and let you download a cleaned file.
            </p>
          </div>

          <label class="inline-flex items-center gap-2 cursor-pointer">
            <input
              id="file-input"
              type="file"
              class="hidden"
              accept=".csv,.tsv,.xlsx,.xls,.json,.parquet"
              onchange="uploadFile(event)"
            />
            <span
              class="px-4 py-2 rounded-lg bg-emerald-500 hover:bg-emerald-400
                     text-xs sm:text-sm font-semibold text-slate-950 shadow-sm">
              Browse data file
            </span>
          </label>
        </div>

        <p class="mt-3 text-[11px] text-slate-500">
          Supported: <span class="text-slate-300">CSV, TSV, XLSX/XLS, JSON, Parquet</span>.
          For Word/PDF documents, use a separate document processing tool.
        </p>
      </div>

      <!-- Three column layout -->
      <div class="grid md:grid-cols-3 gap-4">
        <!-- Profile -->
        <div class="glass p-4 md:col-span-1">
          <h3 class="text-xs font-semibold text-slate-200 mb-2">Dataset Profile</h3>
          <div id="profile" class="text-[11px] text-slate-300 space-y-1">
            <p class="text-slate-500 text-[11px]">No dataset loaded yet. Upload a file to begin.</p>
          </div>
        </div>

        <!-- Quality -->
        <div class="glass p-4 md:col-span-1">
          <h3 class="text-xs font-semibold text-slate-200 mb-2">Quality Score</h3>
          <div id="quality" class="text-[11px] text-slate-300 space-y-1">
            <p class="text-slate-500 text-[11px]">Upload a file to see data quality metrics.</p>
          </div>
        </div>

        <!-- Cleaning summary -->
        <div class="glass p-4 md:col-span-1">
          <h3 class="text-xs font-semibold text-slate-200 mb-2">Cleaning Summary</h3>
          <div id="result" class="text-[11px] text-slate-300 space-y-1">
            <p class="text-slate-500 text-[11px]">Cleaned dataset details will appear here.</p>
          </div>
        </div>
      </div>

      <!-- Download section -->
      <div class="glass p-4 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3">
        <div class="text-xs text-slate-300">
          <p class="font-medium">Download Cleaned Dataset</p>
          <p class="text-[11px] text-slate-400">
            After the cleaning pipeline completes, export the cleaned dataset as CSV.
          </p>
        </div>
        <button
          id="download"
          onclick="downloadCleaned()"
          class="px-4 py-2 rounded-lg bg-slate-800 text-xs sm:text-sm text-slate-400
                 border border-slate-600 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed"
          disabled>
          Download CSV
        </button>
      </div>
    </div>
  `;
}

/* ====== FILE UPLOAD FLOW ====== */
async function uploadFile(e) {
  const file = e.target.files[0];
  if (!file) return;

  // Guard: only allow supported tabular files
  if (!ALLOWED_EXT_REGEX.test(file.name)) {
    alertBox(
      "Unsupported file type. This tool only cleans CSV, Excel, JSON & Parquet tabular data.",
      "error"
    );
    e.target.value = "";
    return;
  }

  try {
    const form = new FormData();
    form.append("file", file);

    alertBox("Uploading and registering dataset...");

    const res = await api("/datasets/upload", {
      method: "POST",
      body: form,
    });

    datasetId = res.dataset_id;
    cleanedId = null;

    await loadProfile();
    await loadQuality();
    await cleanNow();
  } catch (err) {
    console.error(err);
    alertBox(err.message || "Upload failed", "error");
  } finally {
    e.target.value = ""; // Reset file input
  }
}

/* ====== PROFILE ====== */
async function loadProfile() {
  if (!datasetId) return;

  const p = await api(`/datasets/${datasetId}/profile`);
  const profileDiv = document.getElementById("profile");
  if (!profileDiv) return;

  let colsHtml = Object.entries(p.columns || {})
    .slice(0, 6)
    .map(
      ([name, c]) => `
        <div class="flex justify-between gap-2">
          <span class="text-[11px] text-slate-300 truncate max-w-[140px]">${name}</span>
          <span class="text-[11px] text-slate-400">${c.dtype}</span>
        </div>
      `
    )
    .join("");

  if (!colsHtml) {
    colsHtml = `<p class="text-[11px] text-slate-400">No column info parsed.</p>`;
  }

  profileDiv.innerHTML = `
    <p class="text-[11px] text-slate-200 mb-1">
      ID: <span class="text-slate-400">${datasetId}</span>
    </p>
    <p class="text-[11px]">
      Rows: <span class="text-emerald-400">${p.n_rows}</span>
      · Columns: <span class="text-emerald-400">${p.n_cols}</span>
    </p>
    <div class="mt-2 border-t border-slate-700/70 pt-2">
      <p class="text-[11px] text-slate-400 mb-1">Sample columns:</p>
      <div class="space-y-1">${colsHtml}</div>
    </div>
  `;
}

/* ====== QUALITY SCORE ====== */
async function loadQuality() {
  if (!datasetId) return;

  const q = await api(`/datasets/${datasetId}/quality_score`);
  const div = document.getElementById("quality");
  if (!div) return;

  const score = q.quality_score ?? 0;
  const badgeColor =
    score >= 95
      ? "bg-emerald-500/20 text-emerald-300"
      : score >= 80
      ? "bg-sky-500/20 text-sky-300"
      : "bg-amber-500/20 text-amber-300";

  div.innerHTML = `
    <div class="flex items-center gap-2">
      <span class="text-[28px] font-semibold text-slate-100">${score.toFixed(1)}</span>
      <span class="text-[11px] text-slate-400">/ 100</span>
      <span class="ml-2 px-2 py-0.5 rounded-full text-[10px] ${badgeColor}">
        ${score >= 95 ? "Excellent" : score >= 80 ? "Good" : "Needs Attention"}
      </span>
    </div>
    <div class="mt-2 space-y-1 text-[11px] text-slate-300">
      <p>Missing ratio:
        <span class="text-emerald-300">
          ${(q.metrics?.missing_ratio * 100).toFixed(2)}%
        </span>
      </p>
      <p>Duplicate ratio:
        <span class="text-emerald-300">
          ${(q.metrics?.duplicate_ratio * 100).toFixed(2)}%
        </span>
      </p>
      <p>Constant cols:
        <span class="text-emerald-300">
          ${(q.metrics?.constant_cols_ratio * 100).toFixed(2)}%
        </span>
      </p>
    </div>
  `;
}

/* ====== CLEANING ====== */
async function cleanNow() {
  if (!datasetId) return;

  alertBox("Running cleaning pipeline...");

  const c = await api(`/datasets/${datasetId}/clean`, {
    method: "POST",
  });

  cleanedId = c.cleaned_dataset_id;

  const div = document.getElementById("result");
  if (!div) return;

  div.innerHTML = `
    <p class="text-[11px] text-slate-200 mb-1">
      Source dataset:
      <span class="text-slate-400">${c.source_dataset_id}</span>
    </p>
    <p class="text-[11px] text-slate-300">
      Rows:
      <span class="text-emerald-400">${c.n_rows_before}</span>
      → <span class="text-emerald-400">${c.n_rows_after}</span>
    </p>
    <p class="text-[11px] text-slate-300">
      Missing values:
      <span class="text-emerald-400">${c.n_missing_before}</span>
      → <span class="text-emerald-400">${c.n_missing_after}</span>
    </p>
    <p class="text-[11px] text-slate-300">
      Duplicate rows removed:
      <span class="text-emerald-400">${c.duplicate_rows_removed}</span>
      · Outlier rows removed:
      <span class="text-emerald-400">${c.outlier_rows_removed}</span>
    </p>
  `;

  const dlBtn = document.getElementById("download");
  if (dlBtn) {
    dlBtn.disabled = false;
    dlBtn.classList.remove("bg-slate-800", "text-slate-400");
    dlBtn.classList.add(
      "bg-emerald-500",
      "hover:bg-emerald-400",
      "text-slate-950"
    );
  }

  alertBox("Cleaning complete. You can download the cleaned CSV now.");
}

/* ====== DOWNLOAD CLEANED DATA ====== */
function downloadCleaned() {
  if (!cleanedId) {
    alertBox("No cleaned dataset available to download.", "error");
    return;
  }
  // Let browser download the file
  window.open(`${API_BASE_URL}/datasets/${cleanedId}/download`, "_blank");
}

/* ====== INIT ====== */
function initCleanMind() {
  renderNavbar();
  renderDashboard();
}

document.addEventListener("DOMContentLoaded", initCleanMind);
