(function () {
  const DATA_URL = "https://seokleejh.github.io/etf_search/kospi.json";

  const COLS = [
    { key: "#",          label: "#",      align: "right", sortable: false },
    { key: "티커",       label: "티커",   align: "left",  sortable: true  },
    { key: "종목명",     label: "종목명", align: "left",  sortable: true  },
    { key: "섹터",       label: "섹터",   align: "left",  sortable: true  },
    { key: "시가총액(억)", label: "시가총액", align: "right", sortable: true },
    { key: "PER",        label: "PER",    align: "right", sortable: true  },
    { key: "PBR",        label: "PBR",    align: "right", sortable: true  },
    { key: "EPS",        label: "EPS",    align: "right", sortable: true  },
    { key: "BPS",        label: "BPS",    align: "right", sortable: true  },
  ];

  let allRows = [];
  let sortKey = "시가총액(억)";
  let sortAsc = false;

  async function init() {
    try {
      const res = await fetch(DATA_URL);
      if (!res.ok) throw new Error("HTTP error: " + res.status);
      const fund = await res.json();

      allRows = fund.data;

      const d = fund.date;
      document.getElementById("kw-date").textContent =
        "기준일: " + d.slice(0, 4) + "-" + d.slice(4, 6) + "-" + d.slice(6, 8);

      const sectors = [...new Set(allRows.map(r => r["섹터"]).filter(Boolean))].sort();
      const sel = document.getElementById("kw-sector");
      sectors.forEach(s => {
        const opt = document.createElement("option");
        opt.value = s;
        opt.textContent = s;
        sel.appendChild(opt);
      });

      buildHeader();
      document.getElementById("kw-status").style.display = "none";
      document.getElementById("kw-table-wrap").style.display = "";
      render();
    } catch (e) {
      const el = document.getElementById("kw-status");
      el.className = "kw-error";
      el.textContent = "데이터를 불러오지 못했습니다. (" + e.message + ")";
    }
  }

  function buildHeader() {
    const tr = document.getElementById("kw-thead");
    COLS.forEach(col => {
      const th = document.createElement("th");
      if (col.align === "left") th.className = "kw-left";
      if (col.sortable) {
        th.innerHTML = col.label + '<span class="kw-arrow"></span>';
        th.addEventListener("click", () => {
          if (sortKey === col.key) {
            sortAsc = !sortAsc;
          } else {
            sortKey = col.key;
            sortAsc = col.key !== "시가총액(억)";
          }
          render();
        });
      } else {
        th.textContent = col.label;
      }
      if (col.key === sortKey) th.classList.add("kw-sorted");
      tr.appendChild(th);
    });
  }

  function render() {
    const sector = document.getElementById("kw-sector").value;
    const search = document.getElementById("kw-search").value.trim().toLowerCase();
    const perMin = parseFloat(document.getElementById("kw-per-min").value);
    const perMax = parseFloat(document.getElementById("kw-per-max").value);
    const pbrMin = parseFloat(document.getElementById("kw-pbr-min").value);
    const pbrMax = parseFloat(document.getElementById("kw-pbr-max").value);
    const capMin = parseFloat(document.getElementById("kw-cap-min").value) * 10000;

    let rows = allRows.filter(r => {
      if (sector && r["섹터"] !== sector) return false;
      if (search && r["종목명"].toLowerCase().indexOf(search) === -1) return false;
      if (!isNaN(perMin) && r["PER"] < perMin) return false;
      if (!isNaN(perMax) && r["PER"] > perMax) return false;
      if (!isNaN(pbrMin) && r["PBR"] < pbrMin) return false;
      if (!isNaN(pbrMax) && r["PBR"] > pbrMax) return false;
      if (!isNaN(capMin) && r["시가총액(억)"] < capMin) return false;
      return true;
    });

    if (sortKey && sortKey !== "#") {
      rows = rows.slice().sort((a, b) => {
        const av = a[sortKey], bv = b[sortKey];
        if (typeof av === "number") return sortAsc ? av - bv : bv - av;
        return sortAsc
          ? String(av).localeCompare(String(bv), "ko")
          : String(bv).localeCompare(String(av), "ko");
      });
    }

    document.querySelectorAll("#kw-thead th").forEach((th, i) => {
      const col = COLS[i];
      if (!col.sortable) return;
      th.classList.toggle("kw-sorted", col.key === sortKey);
      const arrow = th.querySelector(".kw-arrow");
      if (arrow) arrow.textContent = col.key === sortKey ? (sortAsc ? " ▲" : " ▼") : " ▲▼";
    });

    document.getElementById("kw-count").textContent = rows.length + "개 종목";

    const tbody = document.getElementById("kw-tbody");
    tbody.innerHTML = "";
    rows.forEach((r, i) => {
      const tr = document.createElement("tr");
      const cap = r["시가총액(억)"];
      const capStr = cap >= 10000 ? (cap / 10000).toFixed(1) + "조" : cap.toLocaleString() + "억";
      const per = r["PER"];
      const perCls = per < 10 ? "per-low" : per < 25 ? "per-mid" : "per-high";
      const epsStr = Number.isFinite(r["EPS"]) ? r["EPS"].toLocaleString() : "-";
      const bpsStr = Number.isFinite(r["BPS"]) ? r["BPS"].toLocaleString() : "-";
      tr.innerHTML =
        '<td class="kw-left kw-rank">' + (i + 1) + "</td>" +
        '<td class="kw-left kw-ticker">' + r["티커"] + "</td>" +
        '<td class="kw-left kw-name">' + r["종목명"] + "</td>" +
        '<td class="kw-left kw-sector">' + r["섹터"] + "</td>" +
        "<td>" + capStr + "</td>" +
        '<td class="kw-per ' + perCls + '">' + per.toFixed(2) + "</td>" +
        "<td>" + r["PBR"].toFixed(2) + "</td>" +
        "<td>" + epsStr + "</td>" +
        "<td>" + bpsStr + "</td>";
      tbody.appendChild(tr);
    });
  }

  document.getElementById("kw-sector").addEventListener("change", render);
  document.getElementById("kw-search").addEventListener("input", render);
  document.getElementById("kw-per-min").addEventListener("input", render);
  document.getElementById("kw-per-max").addEventListener("input", render);
  document.getElementById("kw-pbr-min").addEventListener("input", render);
  document.getElementById("kw-pbr-max").addEventListener("input", render);
  document.getElementById("kw-cap-min").addEventListener("input", render);

  document.getElementById("kw-reset").addEventListener("click", function () {
    document.getElementById("kw-sector").value = "";
    document.getElementById("kw-search").value = "";
    document.getElementById("kw-per-min").value = "";
    document.getElementById("kw-per-max").value = "";
    document.getElementById("kw-pbr-min").value = "";
    document.getElementById("kw-pbr-max").value = "";
    document.getElementById("kw-cap-min").value = "";
    render();
  });

  init();
})();