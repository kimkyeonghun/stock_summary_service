(function () {
  "use strict";

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function showToast(message, type) {
    const root = document.getElementById("toast-root");
    if (!root) return;
    const tone = type === "success" ? "border-emerald-200 bg-emerald-50 text-emerald-700" : "border-red-200 bg-red-50 text-red-700";
    const node = document.createElement("div");
    node.className = `pointer-events-auto rounded-xl border px-3 py-2 text-sm shadow-sm ${tone}`;
    node.textContent = message || "요청 처리 중 오류가 발생했습니다.";
    root.appendChild(node);
    window.setTimeout(() => {
      node.classList.add("opacity-0");
      window.setTimeout(() => node.remove(), 180);
    }, 2200);
  }

  async function fetchJson(url, options) {
    const res = await fetch(url, options || {});
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const message = data && data.message ? String(data.message) : "요청 실패";
      throw new Error(message);
    }
    return data;
  }

  function setUrlQuery(nextQuery, options) {
    const opts = options || {};
    const mode = opts.mode || "push";
    const hash = opts.hash || "";
    const url = new URL(window.location.href);
    Object.keys(nextQuery).forEach((key) => {
      const value = nextQuery[key];
      if (value === null || value === undefined || value === "") {
        url.searchParams.delete(key);
      } else {
        url.searchParams.set(key, String(value));
      }
    });
    url.hash = hash;
    const next = `${url.pathname}${url.search}${url.hash}`;
    if (mode === "replace") {
      history.replaceState({}, "", next);
    } else {
      history.pushState({}, "", next);
    }
  }

  function normalizeChoice(value, allowed, fallback) {
    const token = String(value || "").trim().toLowerCase();
    return allowed.includes(token) ? token : fallback;
  }

  function sourceBadge(kind, label) {
    const safeLabel = escapeHtml(label || "뉴스");
    const icon = kind === "report" ? "📄" : kind === "filing" ? "🏛" : "📰";
    return `<span class="inline-flex items-center gap-1 rounded-full border border-gray-200 bg-white px-2 py-0.5 text-xs text-gray-500"><span>${icon}</span><span>${safeLabel}</span></span>`;
  }

  function initTimelineController() {
    const root = document.querySelector("[data-ajax-timeline-root]");
    if (!root) return;

    const market = String(root.dataset.market || "").toLowerCase();
    const code = String(root.dataset.stockCode || "").toUpperCase();
    if (!market || !code) return;

    const activeClass = String(root.dataset.chipActiveClass || "border-indigo-600 bg-indigo-600 text-white");
    const inactiveClass = String(root.dataset.chipInactiveClass || "border-gray-200 bg-white text-gray-900");
    const docSort = String(root.dataset.docSort || "recent").toLowerCase();
    const countEl = root.querySelector("[data-role='timeline-count']");
    const sourceWrap = root.querySelector("[data-role='timeline-source-filters']");
    const windowWrap = root.querySelector("[data-role='timeline-window-filters']");

    let state = {
      source: normalizeChoice(root.dataset.tlSource, ["all", "news", "report", "filing"], "all"),
      window: normalizeChoice(root.dataset.tlWindow, ["7d", "14d", "30d", "all"], "14d"),
    };
    let requestSeq = 0;
    let abortController = null;

    function timelineCardHtml(event, marketCode) {
      const impactHtml = event.show_impact ? `<span class="text-xs ${escapeHtml(event.impact_css || "text-gray-500")}">${escapeHtml(event.impact_emoji || "")} ${escapeHtml(event.impact_text || "")}</span>` : "";
      const similarHtml = Number(event.similar_count || 0) > 0 ? `<span class="rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-500">유사 기사 +${Number(event.similar_count || 0)}</span>` : "";

      let detailsHtml = "";
      if (event.has_more_details) {
        const summaryLines = []
          .concat(Array.isArray(event.summary_preview_lines) ? event.summary_preview_lines : [])
          .concat(Array.isArray(event.summary_more_lines) ? event.summary_more_lines : [])
          .map((line) => `<li class=\"text-sm text-gray-900\">${escapeHtml(line)}</li>`)
          .join("");
        const refs = (Array.isArray(event.evidence_top) ? event.evidence_top : [])
          .map((ref) => {
            const title = escapeHtml(ref && ref.title ? ref.title : "출처");
            const url = ref && ref.url ? String(ref.url) : "";
            if (!url) return `<li class=\"text-sm text-gray-900\">${title}</li>`;
            return `<li><a href=\"${escapeHtml(url)}\" target=\"_blank\" rel=\"noreferrer\" class=\"text-sm text-indigo-600 hover:text-indigo-500\">${title}</a></li>`;
          })
          .join("");
        const moreRefCount = Array.isArray(event.evidence_more) ? event.evidence_more.length : 0;
        const sourceUrl = event.url ? `<a href=\"${escapeHtml(event.url)}\" target=\"_blank\" rel=\"noreferrer\" class=\"inline-flex rounded-lg border border-gray-200 bg-white px-2 py-1 text-xs font-medium text-gray-900 hover:border-indigo-500 hover:text-indigo-500\">원문 보기</a>` : "";
        const itemUrl = `/${marketCode}/item/${Number(event.item_id || 0)}`;

        detailsHtml = `
          <details class="mt-3 rounded-xl border border-gray-200 p-3">
            <summary class="cursor-pointer text-xs font-medium text-gray-500">더보기</summary>
            <div class="mt-3 space-y-3">
              ${summaryLines ? `<div><p class=\"text-xs font-semibold tracking-wide text-gray-500\">상세 요약</p><ul class=\"mt-2 space-y-2\">${summaryLines}</ul></div>` : ""}
              ${refs ? `<div><p class=\"text-xs font-semibold tracking-wide text-gray-500\">근거 출처</p><ul class=\"mt-2 space-y-2\">${refs}</ul>${moreRefCount > 0 ? `<p class=\"mt-2 text-xs text-gray-500\">외 ${moreRefCount}건</p>` : ""}</div>` : ""}
              <div class="flex flex-wrap gap-2">${sourceUrl}<a href="${itemUrl}" class="inline-flex rounded-lg border border-gray-200 bg-white px-2 py-1 text-xs font-medium text-gray-900 hover:border-indigo-500 hover:text-indigo-500">요약 상세 보기</a></div>
            </div>
          </details>
        `;
      }

      return `
        <li class="rounded-xl border border-gray-200 p-4">
          <div class="flex flex-wrap items-center gap-2">
            <span class="text-xs text-gray-500">${escapeHtml(event.published_at || "")}</span>
            ${sourceBadge(event.source_kind, event.source_kind_label)}
            ${impactHtml}
            ${similarHtml}
          </div>
          <p class="mt-2 text-sm text-gray-900">${escapeHtml(event.one_liner || "")}</p>
          ${detailsHtml}
        </li>
      `;
    }

    function renderTimeline(payload) {
      const events = Array.isArray(payload.events) ? payload.events : [];
      const filters = payload.filters || {};
      const sourceOptions = Array.isArray(filters.source) ? filters.source : [];
      const windowOptions = Array.isArray(filters.window) ? filters.window : [];

      if (countEl) countEl.textContent = String(events.length);

      if (sourceWrap) {
        const html = ["<p class=\"text-xs font-semibold text-gray-500\">출처</p>"]
          .concat(
            sourceOptions.map((opt) => {
              const active = !!opt.is_active;
              return `<a href=\"${escapeHtml(opt.url || "#") }\" data-ajax-timeline-filter data-filter-kind=\"source\" data-filter-value=\"${escapeHtml(opt.value || "all")}\" class=\"rounded-full border px-3 py-1 text-xs font-medium ${active ? activeClass : inactiveClass}\">${escapeHtml(opt.label || "")}</a>`;
            })
          )
          .join("");
        sourceWrap.innerHTML = html;
      }

      if (windowWrap) {
        const html = ["<p class=\"text-xs font-semibold text-gray-500\">기간</p>"]
          .concat(
            windowOptions.map((opt) => {
              const active = !!opt.is_active;
              return `<a href=\"${escapeHtml(opt.url || "#")}\" data-ajax-timeline-filter data-filter-kind=\"window\" data-filter-value=\"${escapeHtml(opt.value || "14d")}\" class=\"rounded-full border px-3 py-1 text-xs font-medium ${active ? activeClass : inactiveClass}\">${escapeHtml(opt.label || "")}</a>`;
            })
          )
          .join("");
        windowWrap.innerHTML = html;
      }

      const existingList = root.querySelector("[data-role='timeline-list']");
      const existingEmpty = root.querySelector("[data-role='timeline-empty']");
      if (existingList) existingList.remove();
      if (existingEmpty) existingEmpty.remove();

      if (events.length > 0) {
        const ul = document.createElement("ul");
        ul.className = "mt-4 space-y-3";
        ul.setAttribute("data-role", "timeline-list");
        ul.innerHTML = events.map((event) => timelineCardHtml(event, market)).join("");
        root.appendChild(ul);
      } else {
        const p = document.createElement("p");
        p.className = "mt-4 text-sm text-gray-500";
        p.setAttribute("data-role", "timeline-empty");
        p.textContent = "조건에 맞는 이벤트가 없습니다.";
        root.appendChild(p);
      }
    }

    async function loadTimeline(options) {
      const opts = options || {};
      if (abortController) abortController.abort();
      abortController = new AbortController();
      const seq = ++requestSeq;
      const params = new URLSearchParams({
        tl_source: state.source,
        tl_window: state.window,
        doc_sort: docSort,
      });
      try {
        const payload = await fetchJson(`/api/${market}/stock/${encodeURIComponent(code)}/timeline?${params.toString()}`, {
          signal: abortController.signal,
          headers: { Accept: "application/json" },
        });
        if (seq !== requestSeq) return;
        renderTimeline(payload);
        if (opts.syncUrl !== false) {
          setUrlQuery({ tl_source: state.source, tl_window: state.window }, { mode: opts.urlMode || "push", hash: "event-timeline" });
        }
      } catch (error) {
        if (error && error.name === "AbortError") return;
        showToast(error && error.message ? error.message : "타임라인을 불러오지 못했습니다.", "error");
      }
    }

    root.addEventListener("click", (event) => {
      const target = event.target.closest("[data-ajax-timeline-filter]");
      if (!target || !root.contains(target)) return;
      event.preventDefault();
      const kind = String(target.dataset.filterKind || "");
      const value = String(target.dataset.filterValue || "");
      if (kind === "source") {
        state.source = normalizeChoice(value, ["all", "news", "report", "filing"], state.source);
      } else if (kind === "window") {
        state.window = normalizeChoice(value, ["7d", "14d", "30d", "all"], state.window);
      }
      loadTimeline({ syncUrl: true, urlMode: "push" });
    });

    window.addEventListener("popstate", () => {
      const params = new URLSearchParams(window.location.search);
      const nextSource = normalizeChoice(params.get("tl_source") || state.source, ["all", "news", "report", "filing"], "all");
      const nextWindow = normalizeChoice(params.get("tl_window") || state.window, ["7d", "14d", "30d", "all"], "14d");
      if (nextSource === state.source && nextWindow === state.window) return;
      state.source = nextSource;
      state.window = nextWindow;
      loadTimeline({ syncUrl: false });
    });
  }

  function initFeedController() {
    const root = document.querySelector("[data-ajax-feed-root]");
    if (!root) return;
    const market = String(root.dataset.market || "").toLowerCase();
    if (!market) return;

    const filtersEl = root.querySelector("[data-role='feed-filters']");
    const listEl = root.querySelector("[data-role='feed-list']");
    const countEl = root.querySelector("[data-role='feed-count']");
    let moreBtn = root.querySelector("[data-role='feed-load-more']");
    let emptyEl = root.querySelector("[data-role='feed-empty']");
    const limit = Math.max(1, Number(root.dataset.feedLimit || "10") || 10);
    const activeClass = String(root.dataset.chipActiveClass || "border-indigo-600 bg-indigo-600 text-white");
    const inactiveClass = String(root.dataset.chipInactiveClass || "border-gray-200 bg-white text-gray-900 hover:text-indigo-500");

    let selected = String(root.dataset.selectedFeedStock || "").toUpperCase();
    let offset = Number(moreBtn ? moreBtn.dataset.nextOffset || "0" : "0") || (listEl ? listEl.children.length : 0);
    let isLoading = false;

    function ensureMoreButton() {
      if (moreBtn) return moreBtn;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.id = "load-more-feed";
      btn.setAttribute("data-role", "feed-load-more");
      btn.className = "mt-3 rounded-xl border border-gray-200 bg-white px-3 py-2 text-xs font-medium text-gray-900 hover:text-indigo-500";
      btn.textContent = "더보기";
      root.appendChild(btn);
      moreBtn = btn;
      return btn;
    }

    function renderFilters(options) {
      if (!filtersEl) return;
      const rows = Array.isArray(options) ? options : [];
      filtersEl.innerHTML = rows
        .map((opt) => {
          const active = !!opt.is_active;
          const classes = active ? activeClass : inactiveClass;
          return `<a href="${escapeHtml(opt.url || "#")}" data-ajax-feed-filter data-feed-stock-code="${escapeHtml(opt.code || "")}" class="rounded-full border px-3 py-1 text-xs font-medium ${classes}">${escapeHtml(opt.label || "")}</a>`;
        })
        .join("");
    }

    function feedCardHtml(item) {
      const impact = item.show_impact ? `<span class="text-sm ${escapeHtml(item.impact_css || "text-gray-500")}">${escapeHtml(item.impact_emoji || "")} ${escapeHtml(item.impact_text || "")}</span>` : "";
      const similar = Number(item.similar_count || 0) > 0 ? `<span class="rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-500">유사 기사 +${Number(item.similar_count || 0)}</span>` : "";
      const basket = `<button type="button" data-ajax-basket-add data-market="${escapeHtml(market)}" data-stock-code="${escapeHtml(item.stock_code || "")}" class="rounded-full border border-gray-200 bg-white px-2 py-0.5 text-xs font-medium text-gray-900 hover:text-indigo-500">백테스트 담기</button>`;
      return `
        <li class="feed-item rounded-xl border border-gray-200 p-4">
          <a href="/${market}/item/${Number(item.item_id || 0)}" class="block">
            <div class="min-w-0">
              <div class="flex items-center gap-2">
                <p class="text-sm font-medium text-gray-900">${escapeHtml(item.stock_name || "")} (${escapeHtml(item.stock_code || "")})</p>
                ${impact}
                <span class="text-xs text-gray-500">${escapeHtml(item.published_at || "")}</span>
              </div>
              <p class="mt-2 text-sm text-gray-900">${escapeHtml(item.one_liner || "")}</p>
              <div class="mt-2 flex flex-wrap items-center gap-2">
                ${sourceBadge(item.source_kind, item.source_kind_label)}
                ${similar}
              </div>
            </div>
          </a>
          <div class="mt-2">${basket}</div>
        </li>
      `;
    }

    function renderItems(items, append) {
      if (!listEl) return;
      const rows = Array.isArray(items) ? items : [];
      if (!append) listEl.innerHTML = "";
      if (rows.length > 0) {
        listEl.insertAdjacentHTML("beforeend", rows.map((item) => feedCardHtml(item)).join(""));
      }
      const currentCount = listEl.children.length;
      if (countEl) countEl.textContent = String(currentCount);
      if (!emptyEl) {
        emptyEl = document.createElement("p");
        emptyEl.className = "mt-4 text-sm text-gray-500";
        emptyEl.setAttribute("data-role", "feed-empty");
        emptyEl.textContent = "표시할 피드가 없습니다.";
        root.appendChild(emptyEl);
      }
      emptyEl.classList.toggle("hidden", currentCount > 0);
    }

    function renderMeta(meta) {
      const hasMore = !!(meta && meta.has_more);
      const nextOffset = meta && meta.next_offset != null ? Number(meta.next_offset) : null;
      const btn = ensureMoreButton();
      if (hasMore && nextOffset != null) {
        btn.dataset.nextOffset = String(nextOffset);
        btn.classList.remove("hidden");
      } else {
        btn.classList.add("hidden");
      }
    }

    async function loadFeed(options) {
      const opts = options || {};
      if (isLoading) return;
      isLoading = true;
      const append = !!opts.append;
      const nextOffset = append ? Number(moreBtn ? moreBtn.dataset.nextOffset || "0" : offset) : 0;
      const params = new URLSearchParams({ limit: String(limit), offset: String(nextOffset) });
      if (selected) params.set("feed_stock", selected);

      try {
        const payload = await fetchJson(`/api/${market}/feed?${params.toString()}`, {
          headers: { Accept: "application/json" },
        });
        renderFilters(payload.filters && payload.filters.options ? payload.filters.options : []);
        renderItems(payload.items || [], append);
        renderMeta(payload.meta || {});
        offset = payload.meta && payload.meta.next_offset != null ? Number(payload.meta.next_offset) : (listEl ? listEl.children.length : 0);
        if (opts.syncUrl !== false) {
          setUrlQuery({ feed_stock: selected || null }, { mode: opts.urlMode || "push" });
        }
      } catch (error) {
        showToast(error && error.message ? error.message : "피드를 불러오지 못했습니다.", "error");
      } finally {
        isLoading = false;
      }
    }

    root.addEventListener("click", (event) => {
      const filter = event.target.closest("[data-ajax-feed-filter]");
      if (filter && root.contains(filter)) {
        event.preventDefault();
        selected = String(filter.dataset.feedStockCode || "").toUpperCase();
        loadFeed({ append: false, syncUrl: true, urlMode: "push" });
        return;
      }
      const more = event.target.closest("[data-role='feed-load-more']");
      if (more && root.contains(more)) {
        event.preventDefault();
        loadFeed({ append: true, syncUrl: false });
      }
    });

    window.addEventListener("popstate", () => {
      const params = new URLSearchParams(window.location.search);
      const nextFeedStock = String(params.get("feed_stock") || "").toUpperCase();
      if (nextFeedStock === selected) return;
      selected = nextFeedStock;
      loadFeed({ append: false, syncUrl: false });
    });
  }

  function updateWatchlistButton(form, subscribed) {
    const button = form.querySelector("[data-role='watchlist-toggle-button']") || form.querySelector("button[type='submit']");
    if (!button) return;
    form.dataset.subscribed = subscribed ? "1" : "0";
    const subscribeLabel = String(form.dataset.btnSubscribeLabel || "Subscribe");
    const unsubscribeLabel = String(form.dataset.btnUnsubscribeLabel || "Unsubscribe");
    const subscribeClass = String(form.dataset.btnSubscribeClass || button.className);
    const unsubscribeClass = String(form.dataset.btnUnsubscribeClass || button.className);
    button.textContent = subscribed ? unsubscribeLabel : subscribeLabel;
    button.className = subscribed ? unsubscribeClass : subscribeClass;

    const item = form.closest("[data-item]");
    if (item) item.dataset.subscribed = subscribed ? "1" : "0";
  }

  function initWatchlistController() {
    const forms = Array.from(document.querySelectorAll("form[data-ajax-watchlist-form]"));
    if (!forms.length) return;

    forms.forEach((form) => {
      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        const market = String(form.dataset.market || "").toLowerCase();
        const kind = String(form.dataset.kind || "").toLowerCase();
        const value = String(form.dataset.value || "").toUpperCase();
        if (!market || !kind || !value) {
          form.submit();
          return;
        }
        if (form.dataset.busy === "1") return;

        const prevSubscribed = String(form.dataset.subscribed || "0") === "1";
        const desired = !prevSubscribed;
        form.dataset.busy = "1";
        updateWatchlistButton(form, desired);
        window.dispatchEvent(new Event("briefalpha:watchlist-change"));

        try {
          const payload = await fetchJson(`/api/${market}/watchlist`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accept: "application/json",
            },
            body: JSON.stringify({ kind, value, desired }),
          });
          updateWatchlistButton(form, !!payload.subscribed);
          window.dispatchEvent(new Event("briefalpha:watchlist-change"));
        } catch (error) {
          updateWatchlistButton(form, prevSubscribed);
          window.dispatchEvent(new Event("briefalpha:watchlist-change"));
          showToast(error && error.message ? error.message : "구독 상태를 변경하지 못했습니다.", "error");
        } finally {
          form.dataset.busy = "0";
        }
      });
    });
  }

  function initBasketController() {
    document.addEventListener("click", async (event) => {
      const btn = event.target.closest("[data-ajax-basket-add]");
      if (!btn) return;
      event.preventDefault();
      if (btn.dataset.busy === "1") return;

      const market = String(btn.dataset.market || "").toLowerCase();
      const stockCode = String(btn.dataset.stockCode || "").toUpperCase();
      if (!market || !stockCode) return;

      btn.dataset.busy = "1";
      try {
        await fetchJson(`/api/${market}/backtest/basket`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
          },
          body: JSON.stringify({ code: stockCode }),
        });
        showToast("백테스트 장바구니에 추가했습니다.", "success");
      } catch (error) {
        showToast(error && error.message ? error.message : "장바구니 추가에 실패했습니다.", "error");
      } finally {
        btn.dataset.busy = "0";
      }
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    initTimelineController();
    initFeedController();
    initWatchlistController();
    initBasketController();
  });
})();
