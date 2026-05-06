(function () {
  const chartDom = document.getElementById("chart-main");
  const searchInput = document.getElementById("stock-search");
  const statusEl = document.getElementById("chart-status");
  const manualCode = document.getElementById("manual-code");
  const btnManual = document.getElementById("btn-load-manual");

  const sidebarNormal = document.getElementById("sidebar-normal");
  const sidebarSearch = document.getElementById("sidebar-search");
  const searchPanelTitle = document.getElementById("search-panel-title");
  const listPullup = document.getElementById("list-pullup");
  const listStartup = document.getElementById("list-startup");
  const listSearch = document.getElementById("list-search");

  const pagerPullupPrev = document.getElementById("pager-pullup-prev");
  const pagerPullupNext = document.getElementById("pager-pullup-next");
  const pagerPullupInfo = document.getElementById("pager-pullup-info");
  const pagerPullupSize = document.getElementById("pager-pullup-size");

  const pagerStartupPrev = document.getElementById("pager-startup-prev");
  const pagerStartupNext = document.getElementById("pager-startup-next");
  const pagerStartupInfo = document.getElementById("pager-startup-info");
  const pagerStartupSize = document.getElementById("pager-startup-size");

  const pagerSearchPrev = document.getElementById("pager-search-prev");
  const pagerSearchNext = document.getElementById("pager-search-next");
  const pagerSearchInfo = document.getElementById("pager-search-info");
  const pagerSearchSize = document.getElementById("pager-search-size");

  let chart = echarts.init(chartDom, null, { renderer: "canvas" });
  let activeCode = null;
  let searchTimer = null;

  const pullupState = {
    page: 1,
    pageSize: parseInt(pagerPullupSize.value, 10) || 20,
    total: 0,
    loading: false,
  };
  const startupState = {
    page: 1,
    pageSize: parseInt(pagerStartupSize.value, 10) || 20,
    total: 0,
    loading: false,
  };
  const searchState = {
    page: 1,
    pageSize: parseInt(pagerSearchSize.value, 10) || 20,
    total: 0,
    loading: false,
    query: "",
  };

  function setStatus(msg, isErr) {
    statusEl.textContent = msg || "";
    statusEl.classList.toggle("error", !!isErr);
  }

  function scatterPairs(markList) {
    if (!markList || !markList.length) return [];
    return markList.map((m) => [m.date, m.value]);
  }

  function buildOption(payload) {
    const dates = payload.dates;
    const kline = payload.kline;
    const volume = payload.volume;
    const turnover = payload.turnover || [];
    const pctChg = payload.pct_chg || [];
    const ma = payload.ma || {};
    const rsi = payload.rsi14 || [];
    const marks = payload.marks || {};

    const volColors = kline.map((row) => {
      const o = row[0];
      const c = row[1];
      return c >= o ? "#ef232a" : "#14b143";
    });

    const axisLineDark = { lineStyle: { color: "#30363d" } };
    const axisLabelDark = { color: "#8b949e", fontSize: 10 };
    const splitLineDark = { lineStyle: { color: "#21262d" } };

    return {
      backgroundColor: "#0d1117",
      animation: false,
      legend: {
        top: 6,
        left: "center",
        textStyle: { color: "#c9d1d9", fontSize: 11 },
        data: [
          "K线",
          "MA5",
          "MA10",
          "MA20",
          "MA60",
          "成交量",
          "RSI14",
          "买入",
          "试盘日",
          "突破",
          "放量",
        ],
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross", link: [{ xAxisIndex: "all" }] },
        backgroundColor: "rgba(22,27,34,0.94)",
        borderColor: "#30363d",
        textStyle: { color: "#e6edf3", fontSize: 11 },
        formatter: function (params) {
          if (!params || !params.length) return "";
          const idx = params[0].dataIndex;
          const d = dates[idx];
          const kl = kline[idx];
          const vol = volume[idx];
          const tn = turnover[idx];
          const pc = pctChg[idx];
          if (!kl) return d;
          const open = kl[0],
            close = kl[1],
            low = kl[2],
            high = kl[3];
          const pct =
            pc != null && !Number.isNaN(pc)
              ? pc.toFixed(2) + "%"
              : "-";
          let html =
            '<div style="font-weight:600;margin-bottom:6px">' +
            d +
            "</div>";
          html +=
            "<div>开 <span style='float:right;margin-left:12px'>" +
            open +
            "</span></div>";
          html +=
            "<div>高 <span style='float:right'>" +
            high +
            "</span></div>";
          html +=
            "<div>低 <span style='float:right'>" +
            low +
            "</span></div>";
          html +=
            "<div>收 <span style='float:right'>" +
            close +
            "</span></div>";
          html +=
            "<div>涨跌 <span style='float:right'>" +
            pct +
            "</span></div>";
          html +=
            "<div>成交量 <span style='float:right'>" +
            (vol != null ? Number(vol).toLocaleString() : "-") +
            "</span></div>";
          if (tn != null && !Number.isNaN(tn)) {
            html +=
              "<div>换手率 <span style='float:right'>" +
              Number(tn).toFixed(2) +
              "%</span></div>";
          }
          return html;
        },
      },
      axisPointer: {
        link: [{ xAxisIndex: "all" }],
        label: { backgroundColor: "#30363d" },
      },
      grid: [
        { left: 56, right: 52, top: 56, height: "48%" },
        { left: 56, right: 52, top: "62%", height: "14%" },
        { left: 56, right: 52, top: "80%", height: "14%" },
      ],
      xAxis: [
        {
          type: "category",
          data: dates,
          boundaryGap: true,
          axisLine: axisLineDark,
          axisLabel: { show: false },
          axisTick: { show: false },
          gridIndex: 0,
        },
        {
          type: "category",
          data: dates,
          boundaryGap: true,
          axisLine: axisLineDark,
          axisLabel: { show: false },
          axisTick: { show: false },
          gridIndex: 1,
        },
        {
          type: "category",
          data: dates,
          boundaryGap: true,
          axisLine: axisLineDark,
          axisLabel: axisLabelDark,
          axisTick: { show: false },
          gridIndex: 2,
        },
      ],
      yAxis: [
        {
          scale: true,
          gridIndex: 0,
          axisLine: axisLineDark,
          axisLabel: axisLabelDark,
          splitLine: { show: true, ...splitLineDark },
        },
        {
          scale: true,
          gridIndex: 1,
          axisLine: axisLineDark,
          axisLabel: axisLabelDark,
          splitLine: { show: false },
        },
        {
          scale: true,
          gridIndex: 2,
          min: 0,
          max: 100,
          axisLine: axisLineDark,
          axisLabel: axisLabelDark,
          splitLine: { show: true, ...splitLineDark },
        },
      ],
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: [0, 1, 2],
          start: 55,
          end: 100,
        },
        {
          type: "slider",
          xAxisIndex: [0, 1, 2],
          bottom: 4,
          height: 18,
          start: 55,
          end: 100,
          textStyle: { color: "#8b949e" },
          borderColor: "#30363d",
          fillerColor: "rgba(56,139,253,0.15)",
          handleStyle: { color: "#388bfd" },
        },
      ],
      series: (function () {
        const maDefs = [
          ["ma5", "MA5", "#f1c40f"],
          ["ma10", "MA10", "#3498db"],
          ["ma20", "MA20", "#9b59b6"],
          ["ma60", "MA60", "#95a5a6"],
        ];
        const maSeries = maDefs
          .filter(([key]) => ma[key] && ma[key].length)
          .map(([key, label, color]) => ({
            name: label,
            type: "line",
            xAxisIndex: 0,
            yAxisIndex: 0,
            data: ma[key],
            smooth: true,
            showSymbol: false,
            lineStyle: { width: 1.2, color },
          }));
        return [
          {
            name: "K线",
            type: "candlestick",
            xAxisIndex: 0,
            yAxisIndex: 0,
            data: kline,
            itemStyle: {
              color: "#ef232a",
              color0: "#14b143",
              borderColor: "#ef232a",
              borderColor0: "#14b143",
            },
          },
        ]
          .concat(maSeries)
          .concat([
            {
              name: "成交量",
              type: "bar",
              xAxisIndex: 1,
              yAxisIndex: 1,
              data: volume.map((v, i) => ({
                value: v,
                itemStyle: { color: volColors[i] },
              })),
            },
            {
              name: "RSI14",
              type: "line",
              xAxisIndex: 2,
              yAxisIndex: 2,
              data: rsi,
              smooth: true,
              showSymbol: false,
              lineStyle: { width: 1.2, color: "#58a6ff" },
              markLine: {
                silent: true,
                symbol: "none",
                lineStyle: { color: "#484f58", type: "dashed" },
                data: [{ yAxis: 30 }, { yAxis: 70 }],
              },
            },
            {
              name: "买入",
              type: "scatter",
              xAxisIndex: 0,
              yAxisIndex: 0,
              data: scatterPairs(marks.buy_signal),
              symbol: "diamond",
              symbolSize: 11,
              itemStyle: { color: "#a855f7" },
              z: 10,
            },
            {
              name: "试盘日",
              type: "scatter",
              xAxisIndex: 0,
              yAxisIndex: 0,
              data: scatterPairs(marks.is_spike_day),
              symbol: "triangle",
              symbolSize: 10,
              itemStyle: { color: "#f97316" },
              z: 10,
            },
            {
              name: "突破",
              type: "scatter",
              xAxisIndex: 0,
              yAxisIndex: 0,
              data: scatterPairs(marks.breakout),
              symbol: "pin",
              symbolSize: 10,
              itemStyle: { color: "#00b894" },
              z: 10,
            },
            {
              name: "放量",
              type: "scatter",
              xAxisIndex: 0,
              yAxisIndex: 0,
              data: scatterPairs(marks.vol_expand),
              symbol: "rect",
              symbolSize: 8,
              itemStyle: { color: "#3b82f6" },
              z: 10,
            },
          ]);
      })(),
    };
  }

  async function loadKline(code) {
    if (!code) return;
    activeCode = code;
    setStatus("加载 K 线…");
    try {
      const url =
        "/api/strategy/kline/" +
        encodeURIComponent(code) +
        "?bars=600";
      const res = await fetch(url);
      const json = await res.json();
      if (!json.ok) {
        setStatus(json.error || "加载失败", true);
        return;
      }
      const titleEl = document.getElementById("chart-stock-title");
      const d = json.data;
      titleEl.textContent =
        (d.name || "") + " (" + d.code + ")";
      chart.setOption(buildOption(d), true);
      setStatus("");
      document.querySelectorAll(".stock-item").forEach((el) => {
        el.classList.toggle("active", el.dataset.code === code);
      });
    } catch (e) {
      setStatus("网络错误: " + e.message, true);
    }
  }

  function turnoverLabel(it) {
    const v =
      it.turnover_pct_display != null
        ? it.turnover_pct_display
        : it.turnover;
    if (v == null || Number.isNaN(Number(v))) return "";
    return " · 换手 " + Number(v).toFixed(2) + "%";
  }

  function totalPages(total, pageSize) {
    if (total <= 0) return 1;
    return Math.ceil(total / pageSize);
  }

  function applyPagerUi(state, infoEl, prevEl, nextEl) {
    const tp = totalPages(state.total, state.pageSize);
    let p = state.page;
    if (p > tp) p = tp;
    if (p < 1) p = 1;
    state.page = p;
    infoEl.textContent =
      "第 " +
      state.page +
      " / " +
      tp +
      " 页 · 共 " +
      state.total +
      " 条";
    prevEl.disabled = state.page <= 1 || state.total === 0 || state.loading;
    nextEl.disabled =
      state.page >= tp || state.total === 0 || state.loading;
  }

  function makeStockItemEl(it, mode, paletteIdx) {
    const div = document.createElement("div");
    div.className = "stock-item palette-" + (paletteIdx % 6);
    div.dataset.code = it.code;
    div.setAttribute("role", "button");
    div.tabIndex = 0;
    let meta;
    if (mode === "search") {
      meta = it.code + " · 点击切换K线图";
    } else if (mode === "startup") {
      const hint = it.is_abnormal_type || it.warning_info || "";
      meta =
        it.code +
        " · 日期 " +
        (it.date || "-") +
        (hint ? " · " + hint : "");
    } else {
      meta =
        it.code +
        " · 信号日 " +
        (it.date || "-") +
        turnoverLabel(it);
    }
    div.innerHTML =
      '<div class="name">' +
      (it.name || it.code) +
      '</div><div class="meta">' +
      meta +
      "</div>";
    div.addEventListener("click", () => loadKline(it.code));
    div.addEventListener("keydown", function (ev) {
      if (ev.key === "Enter" || ev.key === " ") {
        ev.preventDefault();
        loadKline(it.code);
      }
    });
    return div;
  }

  function renderEmptyHint(container, mode) {
    container.innerHTML =
      '<div class="status-bar">' +
      (mode === "search"
        ? "未找到匹配股票，请换关键词或输入 sh600000 形式代码"
        : mode === "startup"
          ? "暂无 need_to_analysis=1 的记录"
          : "近期暂无买入信号；可用上方检索任意标的查看K线") +
      "</div>";
  }

  async function fetchPullupList() {
    if (pullupState.loading) return;
    pullupState.loading = true;
    pullupState.pageSize =
      parseInt(pagerPullupSize.value, 10) || 20;
    applyPagerUi(pullupState, pagerPullupInfo, pagerPullupPrev, pagerPullupNext);
    try {
      const params = new URLSearchParams({
        turnover_min: "0",
        recent_days: "120",
        page: String(pullupState.page),
        page_size: String(pullupState.pageSize),
      });
      const res = await fetch("/api/strategy/buy-signals?" + params.toString());
      const json = await res.json();
      if (!json.ok) {
        pullupState.total = 0;
        listPullup.innerHTML =
          '<div class="status-bar error">' +
          (json.error || "列表加载失败") +
          "</div>";
        applyPagerUi(pullupState, pagerPullupInfo, pagerPullupPrev, pagerPullupNext);
        return;
      }
      const items = json.items || [];
      pullupState.total = json.total != null ? json.total : 0;
      const tp = totalPages(pullupState.total, pullupState.pageSize);
      if (pullupState.page > tp) {
        pullupState.page = Math.max(1, tp);
        pullupState.loading = false;
        await fetchPullupList();
        return;
      }
      listPullup.innerHTML = "";
      if (!items.length) {
        renderEmptyHint(listPullup, "signal");
      } else {
        items.forEach((it, i) => {
          listPullup.appendChild(makeStockItemEl(it, "signal", i));
        });
        if (pullupState.page === 1 && !activeCode) {
          loadKline(items[0].code);
        }
      }
      refreshActiveHighlight();
    } catch (e) {
      pullupState.total = 0;
      listPullup.innerHTML =
        '<div class="status-bar error">请求失败</div>';
    } finally {
      pullupState.loading = false;
      applyPagerUi(pullupState, pagerPullupInfo, pagerPullupPrev, pagerPullupNext);
    }
  }

  async function fetchStartupList() {
    if (startupState.loading) return;
    startupState.loading = true;
    startupState.pageSize =
      parseInt(pagerStartupSize.value, 10) || 20;
    applyPagerUi(startupState, pagerStartupInfo, pagerStartupPrev, pagerStartupNext);
    try {
      const params = new URLSearchParams({
        page: String(startupState.page),
        page_size: String(startupState.pageSize),
      });
      const res = await fetch("/api/strategy/startup-list?" + params.toString());
      const json = await res.json();
      if (!json.ok) {
        startupState.total = 0;
        listStartup.innerHTML =
          '<div class="status-bar error">' +
          (json.error || "启动策列加载失败") +
          "</div>";
        applyPagerUi(startupState, pagerStartupInfo, pagerStartupPrev, pagerStartupNext);
        return;
      }
      const items = json.items || [];
      startupState.total = json.total != null ? json.total : 0;
      const tp = totalPages(startupState.total, startupState.pageSize);
      if (startupState.page > tp) {
        startupState.page = Math.max(1, tp);
        startupState.loading = false;
        await fetchStartupList();
        return;
      }
      listStartup.innerHTML = "";
      if (!items.length) {
        renderEmptyHint(listStartup, "startup");
      } else {
        items.forEach((it, i) => {
          listStartup.appendChild(makeStockItemEl(it, "startup", i));
        });
      }
      refreshActiveHighlight();
    } catch (e) {
      startupState.total = 0;
      listStartup.innerHTML =
        '<div class="status-bar error">请求失败</div>';
    } finally {
      startupState.loading = false;
      applyPagerUi(startupState, pagerStartupInfo, pagerStartupPrev, pagerStartupNext);
    }
  }

  async function fetchSearchList() {
    const q = searchState.query;
    if (!q || q.length < 1) return;
    if (searchState.loading) return;
    searchState.loading = true;
    searchState.pageSize =
      parseInt(pagerSearchSize.value, 10) || 20;
    applyPagerUi(searchState, pagerSearchInfo, pagerSearchPrev, pagerSearchNext);
    try {
      const params = new URLSearchParams({
        q: q,
        page: String(searchState.page),
        page_size: String(searchState.pageSize),
      });
      const res = await fetch("/api/strategy/search?" + params.toString());
      const json = await res.json();
      if (!json.ok) {
        searchState.total = 0;
        listSearch.innerHTML =
          '<div class="status-bar error">' +
          (json.error || "检索失败") +
          "</div>";
        applyPagerUi(searchState, pagerSearchInfo, pagerSearchPrev, pagerSearchNext);
        return;
      }
      const items = json.items || [];
      searchState.total = json.total != null ? json.total : 0;
      const tp = totalPages(searchState.total, searchState.pageSize);
      if (searchState.page > tp) {
        searchState.page = Math.max(1, tp);
        searchState.loading = false;
        await fetchSearchList();
        return;
      }
      listSearch.innerHTML = "";
      if (!items.length) {
        renderEmptyHint(listSearch, "search");
      } else {
        items.forEach((it, i) => {
          listSearch.appendChild(makeStockItemEl(it, "search", i));
        });
      }
      refreshActiveHighlight();
    } catch (e) {
      searchState.total = 0;
      listSearch.innerHTML =
        '<div class="status-bar error">请求失败</div>';
    } finally {
      searchState.loading = false;
      applyPagerUi(searchState, pagerSearchInfo, pagerSearchPrev, pagerSearchNext);
    }
  }

  function refreshActiveHighlight() {
    if (!activeCode) return;
    document.querySelectorAll(".stock-item").forEach((el) => {
      el.classList.toggle("active", el.dataset.code === activeCode);
    });
  }

  async function enterSearchMode(q) {
    sidebarNormal.setAttribute("hidden", "");
    sidebarSearch.removeAttribute("hidden");
    searchPanelTitle.textContent =
      "检索结果（清空检索框恢复策列列表）";
    searchState.query = q;
    searchState.page = 1;
    searchState.pageSize =
      parseInt(pagerSearchSize.value, 10) || 20;
    listSearch.innerHTML = "";
    void sidebarSearch.offsetHeight;
    await fetchSearchList();
  }

  async function leaveSearchMode() {
    sidebarSearch.setAttribute("hidden", "");
    sidebarNormal.removeAttribute("hidden");
    pullupState.page = 1;
    startupState.page = 1;
    listPullup.innerHTML = "";
    listStartup.innerHTML = "";
    await Promise.all([fetchPullupList(), fetchStartupList()]);
  }

  async function onSearchInputChanged() {
    const q = searchInput.value.trim();
    if (q.length >= 1) {
      searchState.query = q;
      searchState.page = 1;
      await enterSearchMode(q);
    } else {
      await leaveSearchMode();
    }
  }

  pagerPullupPrev.addEventListener("click", function () {
    if (pullupState.page > 1) {
      pullupState.page -= 1;
      fetchPullupList();
    }
  });
  pagerPullupNext.addEventListener("click", function () {
    const tp = totalPages(pullupState.total, pullupState.pageSize);
    if (pullupState.page < tp) {
      pullupState.page += 1;
      fetchPullupList();
    }
  });
  pagerPullupSize.addEventListener("change", function () {
    pullupState.pageSize =
      parseInt(pagerPullupSize.value, 10) || 20;
    pullupState.page = 1;
    fetchPullupList();
  });

  pagerStartupPrev.addEventListener("click", function () {
    if (startupState.page > 1) {
      startupState.page -= 1;
      fetchStartupList();
    }
  });
  pagerStartupNext.addEventListener("click", function () {
    const tp = totalPages(startupState.total, startupState.pageSize);
    if (startupState.page < tp) {
      startupState.page += 1;
      fetchStartupList();
    }
  });
  pagerStartupSize.addEventListener("change", function () {
    startupState.pageSize =
      parseInt(pagerStartupSize.value, 10) || 20;
    startupState.page = 1;
    fetchStartupList();
  });

  pagerSearchPrev.addEventListener("click", function () {
    if (searchState.page > 1) {
      searchState.page -= 1;
      fetchSearchList();
    }
  });
  pagerSearchNext.addEventListener("click", function () {
    const tp = totalPages(searchState.total, searchState.pageSize);
    if (searchState.page < tp) {
      searchState.page += 1;
      fetchSearchList();
    }
  });
  pagerSearchSize.addEventListener("change", function () {
    searchState.pageSize =
      parseInt(pagerSearchSize.value, 10) || 20;
    searchState.page = 1;
    fetchSearchList();
  });

  searchInput.addEventListener("input", function () {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(onSearchInputChanged, 320);
  });

  btnManual.addEventListener("click", function () {
    const c = (manualCode.value || "").trim();
    if (c) loadKline(c);
  });

  manualCode.addEventListener("keydown", function (ev) {
    if (ev.key === "Enter") {
      const c = (manualCode.value || "").trim();
      if (c) loadKline(c);
    }
  });

  window.addEventListener("resize", function () {
    chart.resize();
  });

  fetchPullupList();
  fetchStartupList();
})();
