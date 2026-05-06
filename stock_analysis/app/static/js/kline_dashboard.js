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
  const footerPullup = document.getElementById("footer-pullup");
  const footerStartup = document.getElementById("footer-startup");
  const footerSearch = document.getElementById("footer-search");
  const scrollPullup = document.getElementById("scroll-pullup");
  const scrollStartup = document.getElementById("scroll-startup");
  const scrollSearch = document.getElementById("scroll-search");
  const sentinelPullup = document.getElementById("sentinel-pullup");
  const sentinelStartup = document.getElementById("sentinel-startup");
  const sentinelSearch = document.getElementById("sentinel-search");

  const PAGE_SIZE = 20;

  let chart = echarts.init(chartDom, null, { renderer: "canvas" });
  let activeCode = null;
  let searchTimer = null;

  const pullupState = {
    page: 0,
    loaded: 0,
    total: 0,
    loading: false,
    done: false,
    error: null,
  };
  const startupState = {
    page: 0,
    loaded: 0,
    total: 0,
    loading: false,
    done: false,
    error: null,
  };
  const searchState = {
    page: 0,
    loaded: 0,
    total: 0,
    loading: false,
    done: false,
    error: null,
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

  function updateInfiniteFooter(footerEl, st) {
    if (!footerEl) return;
    footerEl.classList.toggle("error", !!st.error);
    if (st.error) {
      footerEl.textContent = st.error;
      return;
    }
    if (st.loading) {
      footerEl.textContent = "加载中…";
      return;
    }
    if (st.total === 0 && st.loaded === 0 && st.done) {
      footerEl.textContent = "暂无数据";
      return;
    }
    const tail =
      st.done
        ? "已全部加载"
        : "滚动到底部加载更多";
    footerEl.textContent =
      "共 " +
      (st.total != null ? st.total : st.loaded) +
      " 条 · " +
      tail;
  }

  function resetStreamState(st) {
    st.page = 0;
    st.loaded = 0;
    st.total = 0;
    st.loading = false;
    st.done = false;
    st.error = null;
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

  async function fetchPullupPage(append) {
    if (pullupState.loading || pullupState.done) return;
    pullupState.loading = true;
    pullupState.error = null;
    updateInfiniteFooter(footerPullup, pullupState);
    const nextPage = append ? pullupState.page + 1 : 1;
    try {
      const params = new URLSearchParams({
        turnover_min: "0",
        recent_days: "120",
        page: String(nextPage),
        page_size: String(PAGE_SIZE),
      });
      const res = await fetch("/api/strategy/buy-signals?" + params.toString());
      const json = await res.json();
      if (!json.ok) {
        pullupState.error = json.error || "列表加载失败";
        updateInfiniteFooter(footerPullup, pullupState);
        return;
      }
      const items = json.items || [];
      pullupState.total = json.total != null ? json.total : 0;
      if (!append) {
        listPullup.innerHTML = "";
        pullupState.page = 0;
        pullupState.loaded = 0;
      }
      pullupState.page = nextPage;
      if (!items.length && !append) {
        renderEmptyHint(listPullup, "signal");
        pullupState.done = true;
        pullupState.loaded = 0;
      } else {
        if (!items.length) {
          pullupState.done = true;
        } else {
          const base = listPullup.querySelectorAll(".stock-item").length;
          items.forEach((it, i) => {
            listPullup.appendChild(makeStockItemEl(it, "signal", base + i));
          });
          pullupState.loaded += items.length;
          pullupState.done = pullupState.loaded >= pullupState.total;
          if (
            !append &&
            items.length &&
            !activeCode
          ) {
            loadKline(items[0].code);
          }
        }
      }
      refreshActiveHighlight();
    } catch (e) {
      pullupState.error = "请求失败";
    } finally {
      pullupState.loading = false;
      updateInfiniteFooter(footerPullup, pullupState);
    }
  }

  async function fetchStartupPage(append) {
    if (startupState.loading || startupState.done) return;
    startupState.loading = true;
    startupState.error = null;
    updateInfiniteFooter(footerStartup, startupState);
    const nextPage = append ? startupState.page + 1 : 1;
    try {
      const params = new URLSearchParams({
        page: String(nextPage),
        page_size: String(PAGE_SIZE),
      });
      const res = await fetch("/api/strategy/startup-list?" + params.toString());
      const json = await res.json();
      if (!json.ok) {
        startupState.error = json.error || "启动策列加载失败";
        updateInfiniteFooter(footerStartup, startupState);
        return;
      }
      const items = json.items || [];
      startupState.total = json.total != null ? json.total : 0;
      if (!append) {
        listStartup.innerHTML = "";
        startupState.page = 0;
        startupState.loaded = 0;
      }
      startupState.page = nextPage;
      if (!items.length && !append) {
        renderEmptyHint(listStartup, "startup");
        startupState.done = true;
        startupState.loaded = 0;
      } else {
        if (!items.length) {
          startupState.done = true;
        } else {
          const base = listStartup.querySelectorAll(".stock-item").length;
          items.forEach((it, i) => {
            listStartup.appendChild(makeStockItemEl(it, "startup", base + i));
          });
          startupState.loaded += items.length;
          startupState.done = startupState.loaded >= startupState.total;
        }
      }
      refreshActiveHighlight();
    } catch (e) {
      startupState.error = "请求失败";
    } finally {
      startupState.loading = false;
      updateInfiniteFooter(footerStartup, startupState);
    }
  }

  async function fetchSearchPage(append) {
    if (searchState.loading || searchState.done) return;
    const q = searchState.query;
    if (!q || q.length < 1) return;
    searchState.loading = true;
    searchState.error = null;
    updateInfiniteFooter(footerSearch, searchState);
    const nextPage = append ? searchState.page + 1 : 1;
    try {
      const params = new URLSearchParams({
        q: q,
        page: String(nextPage),
        page_size: String(PAGE_SIZE),
      });
      const res = await fetch("/api/strategy/search?" + params.toString());
      const json = await res.json();
      if (!json.ok) {
        searchState.error = json.error || "检索失败";
        listSearch.innerHTML =
          '<div class="status-bar error">' +
          searchState.error +
          "</div>";
        searchState.done = true;
        updateInfiniteFooter(footerSearch, searchState);
        return;
      }
      const items = json.items || [];
      searchState.total = json.total != null ? json.total : 0;
      if (!append) {
        listSearch.innerHTML = "";
        searchState.page = 0;
        searchState.loaded = 0;
      }
      searchState.page = nextPage;
      if (!items.length && !append) {
        renderEmptyHint(listSearch, "search");
        searchState.done = true;
        searchState.loaded = 0;
      } else {
        if (!items.length) {
          searchState.done = true;
        } else {
          const base = listSearch.querySelectorAll(".stock-item").length;
          items.forEach((it, i) => {
            listSearch.appendChild(makeStockItemEl(it, "search", base + i));
          });
          searchState.loaded += items.length;
          searchState.done = searchState.loaded >= searchState.total;
        }
      }
      refreshActiveHighlight();
    } catch (e) {
      searchState.error = "请求失败";
      listSearch.innerHTML =
        '<div class="status-bar error">请求失败</div>';
      searchState.done = true;
    } finally {
      searchState.loading = false;
      updateInfiniteFooter(footerSearch, searchState);
    }
  }

  function refreshActiveHighlight() {
    if (!activeCode) return;
    document.querySelectorAll(".stock-item").forEach((el) => {
      el.classList.toggle("active", el.dataset.code === activeCode);
    });
  }

  function bindInfiniteScroll(scrollEl, sentinelEl, footerEl, streamState, fetchPage) {
    if (!scrollEl || !sentinelEl) return;
    const io = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (en) {
          if (!en.isIntersecting) return;
          if (streamState.loading || streamState.done) return;
          fetchPage(true);
        });
      },
      { root: scrollEl, rootMargin: "100px", threshold: 0 }
    );
    io.observe(sentinelEl);
    updateInfiniteFooter(footerEl, streamState);
    return io;
  }

  let ioPullup = null;
  let ioStartup = null;
  let ioSearch = null;

  function setupObservers() {
    if (ioPullup) ioPullup.disconnect();
    if (ioStartup) ioStartup.disconnect();
    if (ioSearch) ioSearch.disconnect();
    ioPullup = bindInfiniteScroll(
      scrollPullup,
      sentinelPullup,
      footerPullup,
      pullupState,
      fetchPullupPage
    );
    ioStartup = bindInfiniteScroll(
      scrollStartup,
      sentinelStartup,
      footerStartup,
      startupState,
      fetchStartupPage
    );
    ioSearch = bindInfiniteScroll(
      scrollSearch,
      sentinelSearch,
      footerSearch,
      searchState,
      fetchSearchPage
    );
  }

  async function enterSearchMode(q) {
    sidebarNormal.setAttribute("hidden", "");
    sidebarSearch.removeAttribute("hidden");
    searchPanelTitle.textContent =
      "检索结果（清空检索框恢复策列列表）";
    resetStreamState(searchState);
    searchState.query = q;
    listSearch.innerHTML = "";
    updateInfiniteFooter(footerSearch, searchState);
    /* 检索面板默认长期 hidden，root 无布局；展开后强制重排并重建 IO，避免折叠手风琴后检索区高度为 0 或不刷新 */
    void sidebarSearch.offsetHeight;
    if (ioSearch) ioSearch.disconnect();
    ioSearch = bindInfiniteScroll(
      scrollSearch,
      sentinelSearch,
      footerSearch,
      searchState,
      fetchSearchPage
    );
    await fetchSearchPage(false);
  }

  async function leaveSearchMode() {
    sidebarSearch.setAttribute("hidden", "");
    sidebarNormal.removeAttribute("hidden");
    resetStreamState(pullupState);
    resetStreamState(startupState);
    listPullup.innerHTML = "";
    listStartup.innerHTML = "";
    updateInfiniteFooter(footerPullup, pullupState);
    updateInfiniteFooter(footerStartup, startupState);
    await Promise.all([fetchPullupPage(false), fetchStartupPage(false)]);
  }

  async function onSearchInputChanged() {
    const q = searchInput.value.trim();
    if (q.length >= 1) {
      resetStreamState(searchState);
      searchState.query = q;
      await enterSearchMode(q);
    } else {
      await leaveSearchMode();
    }
  }

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

  setupObservers();
  fetchPullupPage(false);
  fetchStartupPage(false);
})();
