let logsData = [], filteredData = [], rowsShown = 0;
const rowsPerPage = 50;

// Mobile hamburger menu
const hamburger = document.getElementById("hamburger");
const tabs = document.getElementById("tabs");

if (hamburger && tabs) {
  hamburger.addEventListener("click", () => {
    hamburger.classList.toggle("active");
    tabs.classList.toggle("active");
  });
  
  // Close menu when clicking a tab button on mobile
  document.querySelectorAll(".tab-button").forEach(btn => {
    btn.addEventListener("click", () => {
      if (window.innerWidth <= 768) {
        hamburger.classList.remove("active");
        tabs.classList.remove("active");
      }
    });
  });
  
  // Close menu when clicking outside on mobile
  document.addEventListener("click", (e) => {
    if (window.innerWidth <= 768 && 
        !tabs.contains(e.target) && 
        !hamburger.contains(e.target) &&
        tabs.classList.contains("active")) {
      hamburger.classList.remove("active");
      tabs.classList.remove("active");
    }
  });
}

// Tabs
document.querySelectorAll(".tab-button").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab-button").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".tab-content").forEach(c => c.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById(btn.dataset.tab).classList.add("active");
  });
});
// Utilities
function parseFromDate(str) {
  if (!str) return new Date(0);
  const m = str.match(/(\d{2})\/(\d{2})\/(\d{2})/);
  if (!m) return new Date(0);
  let [_, mm, dd, yy] = m;
  let year = +yy < 50 ? 2000 + +yy : 1900 + +yy;
  return new Date(year, mm - 1, dd);
}
function formatPacificDate(val) {
  const d = new Date(val);
  return `${d.getMonth() + 1}/${d.getDate()}/${d.getFullYear()}`;
}

// Animate counters
function animateValue(el, start, end, duration) {
  const range = end - start;
  if (range === 0) { el.textContent = end + "%"; return; }
  let startTime;
  function step(ts) {
    if (!startTime) startTime = ts;
    const progress = Math.min((ts - startTime) / duration, 1);
    el.textContent = Math.round(start + range * progress) + "%";
    if (progress < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

// latest "Date From" in the dataset
function latestDateFrom(){
  const ds = logsData.map(d=>parseFromDate(d["Date From"])).filter(d=>!isNaN(d));
  return new Date(Math.max(...ds));
}
// last 30-day window based on latest Date From
function last30(){
  const end = latestDateFrom();
  const start = new Date(end); start.setDate(start.getDate()-29);
  return {start,end};
}
// month helpers
function startOfMonth(d){ return new Date(d.getFullYear(), d.getMonth(), 1); }
function endOfMonth(d){ return new Date(d.getFullYear(), d.getMonth()+1, 0); }
function addMonths(d,n){ return new Date(d.getFullYear(), d.getMonth()+n, 1); }
function plural(n, one, many){ return n===1?one:many; }

function monthLabel(ts){
  const d = new Date(ts);
  const last = latestDateFrom();
  const base = d.toLocaleString(undefined, { month: "short" });
  const isCur = d.getMonth() === last.getMonth() && d.getFullYear() === last.getFullYear();
  return isCur ? `${base} (MTD)` : base;
}



// MOST COMMON
const mostCommonQueries = {
  "Assault": 'in:(Offense) "ASSAULT -" or "ASSAULT-"',
  "Battery": 'in:(Offense) "BATTERY -" or "BATTERY-"',
  "Rape": 'in:(Offense) "SEX OFFENSE - Rape" or "SEX OFFENSE-Rape"',
  "Robbery": 'in:(Offense) "ROBBERY -" or "ROBBERY-"',
  "Kidnapping": 'in:(Offense) "KIDNAPPING -"',

  "Theft": 'in:(Offense) "THEFT-PETTY -" or "THEFT-MOTOR VEHICLE -" or "THEFT-TRICK -" or "THEFT-GRAND -"',
  "Motor vehicle theft": 'in:(Offense) "MOTOR VEHICLE THEFT -"',

  "Burglary": 'in:(Offense) "BURGLARY -" or "BURGLARY-"',
  "Arson": 'in:(Offense) "ARSON -"',
  "Vandalism": 'in:(Offense) "VANDALISM -"',
  "Trespassing": 'in:(Offense) "TRESPASS -"'
};

// BIKE/SCOOTER/SKATEBOARD THEFT
const bikeQuery = 'in:(Offense) "MOTOR VEHICLE THEFT - Theft of Motorized Bicycle/Scooter" or "THEFT-PETTY - Theft Bicycle"';

// PARTIES & RELATED (last 30 days chart)
const partyQueries = {
  "Party shut down": 'in:(Final Incident) "Party/Event Shut Down"',
  "Noise complaint": 'in:(Final Incident) "Loud and Raucous Noise"',
  "Alcohol Overdose": 'in:(Final Incident) "Alcohol Overdose"',
  "Drug Overdose": 'in:(Final Incident) "Drug Overdose"'
};



// Query parsing + row evaluation
function parseQuery(query) {
  return query.match(/from:\(\d{1,2}\/\d{1,2}\/\d{4}\s+to\s+\d{1,2}\/\d{1,2}\/\d{4}\)|after:\d{1,2}\/\d{1,2}\/\d{4}|before:\d{1,2}\/\d{1,2}\/\d{4}|in:\([^)]+\)\s+"[^"]+"|in:\([^)]+\)\s+[^\s]+|"(.*?)"|and|or|[^\s]+/gi) || [];
}

function evaluateRow(row, tokens) {
  let result = null, currentOp = "AND", lastColumn = null;
  for (let t of tokens) {
    const token = t.toLowerCase();
    if (token==="and"){currentOp="AND";continue;}
    if (token==="or"){currentOp="OR";continue;}
    let condition=false;
    if (token.startsWith("from:(")) {
      const m=token.match(/from:\((\d{1,2}\/\d{1,2}\/\d{4})\s+to\s+(\d{1,2}\/\d{1,2}\/\d{4})\)/);
      if(m){const start=new Date(m[1]), end=new Date(m[2]), dt=parseFromDate(row["Date From"]); condition=dt>=start&&dt<=end;}
    } else if(token.startsWith("after:")) {
      const m=token.match(/after:(\d{1,2}\/\d{1,2}\/\d{4})/); if(m){const dt=parseFromDate(row["Date From"]); condition=dt>new Date(m[1]);}
    } else if(token.startsWith("before:")) {
      const m=token.match(/before:(\d{1,2}\/\d{1,2}\/\d{4})/); if(m){const dt=parseFromDate(row["Date From"]); condition=dt<new Date(m[1]);}
    } else if(token.startsWith("in:(")) {
      const m=token.match(/in:\(([^)]+)\)\s*(.+)/); if(m){const col=m[1].trim().toLowerCase(); lastColumn=colMap[col]; condition=matchValue(row[lastColumn],m[2].trim());}
    } else if(lastColumn){condition=matchValue(row[lastColumn],token);}
    else{condition=matchRow(row,token);}
    if(result===null)result=condition; else if(currentOp==="AND")result=result&&condition; else if(currentOp==="OR")result=result||condition;
  }
  return result===null?false:result;
}

function matchValue(cell,valString){
  const cellVal=(cell||"").toLowerCase();
  if(valString.startsWith('"')&&valString.endsWith('"')){
    const val=valString.slice(1,-1).toLowerCase();
    return cellVal.includes(val);
  } else {
    const words=valString.split(/\s+/);
    return words.some(w=>cellVal.includes(w.toLowerCase()));
  }
}

function matchRow(row,valString){
  if(valString.startsWith('"')&&valString.endsWith('"')){
    const val=valString.slice(1,-1).toLowerCase();
    return Object.values(row).some(v=>v&&v.toString().toLowerCase().includes(val));
  } else {
    const words=valString.split(/\s+/);
    return words.some(w=>Object.values(row).some(v=>v&&v.toString().toLowerCase().includes(w.toLowerCase())));
  }
}

const colMap = {
  "date reported":"Date Reported",
  "event #":"Event #",
  "case #":"Case #",
  "offense":"Offense",
  "initial incident":"Initial Incident",
  "final incident":"Final Incident",
  "date from":"Date From",
  "date to":"Date To",
  "location":"Location",
  "disposition":"Disposition",
  "url":"URL"
};
// Render table (desktop + mobile)
function renderTable(reset=false) {
  const table=document.getElementById("logsTableAll"); 
  if(reset)table.innerHTML="";
  let displayData=[...filteredData];
  if(!displayData.length){
    table.innerHTML="<tr><td>No results found</td></tr>";
    document.getElementById("loadMoreAll").style.display="none";
    return;
  }

  const columns=Object.keys(displayData[0]);
  if(reset){
    const thead=document.createElement("thead");
    const headRow=document.createElement("tr");
    columns.forEach(col=>{
      if(col==="URL")return;
      const th=document.createElement("th");
      th.textContent=col;
      th.style.width=(100/columns.length)+"%";
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);
  }

  let tbody=table.querySelector("tbody"); 
  if(!tbody){tbody=document.createElement("tbody");table.appendChild(tbody);}
  const end=Math.min(rowsShown+rowsPerPage,displayData.length);

  for(let i=rowsShown;i<end;i++){
    const row=displayData[i];
    const tr=document.createElement("tr");

    if(window.innerWidth<=768){
      // Mobile card view - show all fields in a card that unfolds
      const primaryFields=["Date Reported","Final Incident","Location"];
      const secondaryFields=["Offense","Date From","Disposition","Event #"];
      
      // Primary fields (always visible)
      primaryFields.forEach(col=>{
        const td=document.createElement("td");
        td.textContent=row[col]||"";
        td.setAttribute("data-label",col);
        tr.appendChild(td);
      });

      // Extra fields (unfold on click)
      const extra=document.createElement("div");
      extra.classList.add("extra-fields");
      Object.keys(row).forEach(col=>{
        if(primaryFields.includes(col)||col==="URL")return;
        const div=document.createElement("div");
        div.classList.add("field-row");
        const label=document.createElement("span");
        label.classList.add("field-label");
        label.textContent=col+": ";
        const value=document.createElement("span");
        value.classList.add("field-value");
        value.textContent=row[col]||"";
        div.appendChild(label);
        div.appendChild(value);
        extra.appendChild(div);
      });
      tr.appendChild(extra);

      // Add click handler to unfold
      tr.addEventListener("click",()=>{tr.classList.toggle("expanded");});

    } else {
      // Desktop
      Object.keys(row).forEach(col=>{
        if(col==="URL")return;
        const td=document.createElement("td");
        if(col==="Event #"&&row["URL"]){
          const a=document.createElement("a");
          a.href=row["URL"];
          a.textContent=row[col]||"";
          a.style.color="#ac2124";
          a.target="_blank";
          td.appendChild(a);
        } else {
          td.textContent=row[col]||"";
        }
        td.setAttribute("data-label",col);
        tr.appendChild(td);
      });
    }
    tbody.appendChild(tr);
  }

  rowsShown=end;
  document.getElementById("loadMoreAll").style.display=rowsShown<displayData.length?"block":"none";
}

async function loadLogs() {
  // Add cache-busting parameter to ensure fresh data
  // Try ../data/ first (for localhost/web/), then fallback to data/ (for root-level)
  const dataPath = "../data/usc_crime_logs.json";
  const response = await fetch(dataPath + "?" + new Date().getTime()).catch(() => 
    fetch("data/usc_crime_logs.json?" + new Date().getTime())
  );
  logsData = await response.json();

  // Clean whitespace
  logsData = logsData.map(d => {
    const cleaned = {};
    for (const k in d) cleaned[k] = typeof d[k] === "string" ? d[k].replace(/\s+/g, " ").trim() : d[k];
    return cleaned;
  });

  // Sort newest first by Date From
  logsData.sort((a,b) => parseFromDate(b["Date From"]) - parseFromDate(a["Date From"]));

  // Table
  filteredData = [...logsData];
  updateDateRangeNote(filteredData);
  rowsShown = 0;
  renderTable(true);

  // --- DASHBOARD MODULES ---
  buildYoYTrendChart();        // Year-over-year stacked column chart
  buildMostCommonChart();     // all-time most common types
  buildBikeTheftChart();      // 12-month line + last-30 headline
  buildPartiesChart();        // last-30 bars + headline
}


  // Update incidents tab note
  function updateDateRangeNote(data){
    const dates = data
      .map(d => parseFromDate(d["Date Reported"]))
      .filter(d => !isNaN(d));
    if (!dates.length) {
      document.getElementById("dateRangeNote").textContent = "No data available.";
      return;
    }
    const minDate = new Date(Math.min(...dates));
    const maxDate = new Date(Math.max(...dates));
    document.getElementById("dateRangeNote").textContent =
      `Data available from ${minDate.toLocaleDateString()} to ${maxDate.toLocaleDateString()}`;
  }

  function plural(n, one, many) {
    return n === 1 ? one : many;
  }




  // Dashboard listeners
  ["chartStartDate","chartEndDate"].forEach(id=>{
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("change",()=>{
      const start=new Date(document.getElementById("chartStartDate").value);
      const end=new Date(document.getElementById("chartEndDate").value);
      if(!isNaN(start) && !isNaN(end)) buildDashboardChart(start,end);
    });
  });

  // Search box
  document.getElementById("searchAll").addEventListener("input",e=>{
    const q=e.target.value.trim();
    if(!q){
      filteredData=[...logsData];
      renderTable(true);
      return;
    }
    const tokens=parseQuery(q);
    if(!tokens.some(t=>/in:|and|or|from:|after:|before:/i.test(t))){
      const words=q.toLowerCase().split(/\s+/);
      filteredData=logsData.filter(row=>
        words.some(w=>Object.values(row).some(val=>val&&val.toString().toLowerCase().includes(w)))
      );
    } else {
      filteredData=logsData.filter(row=>evaluateRow(row,tokens));
    }
    rowsShown=0;
    renderTable(true);
  });

  // On-campus filter
  function applyOnCampusFilter(checkboxId){
    const box=document.getElementById("searchAll");
    let query=box.value.trim();
    const checked=document.getElementById(checkboxId).checked;
    if(checked){
      if(!query.toLowerCase().includes('in:(location) "on campus"')){
        query=query ? query+' and in:(Location) "on campus"' : 'in:(Location) "on campus"';
      }
    } else {
      query=query.replace(/\s*and\s*in:\(location\)\s*"on campus"/i,"");
      query=query.replace(/in:\(location\)\s*"on campus"\s*and\s*/i,"");
      query=query.replace(/in:\(location\)\s*"on campus"/i,"");
    }
    box.value=query.trim();
    box.dispatchEvent(new Event("input"));
  }
  document.getElementById("logsOnCampus").addEventListener("change",()=>applyOnCampusFilter("logsOnCampus"));
  document.getElementById("loadMoreAll").addEventListener("click",()=>renderTable(false));



function buildMostCommonChart() {
  const container = document.getElementById("mcChart");
  const headlineEl = document.getElementById("mcHeadline");
  if (!container || !headlineEl) return;
  if (!logsData || !logsData.length) {
    headlineEl.textContent = "No data available.";
    container.innerHTML = "";
    return;
  }

  // local helper: lowercase first letter
  const lcFirst = s => (s ? s.charAt(0).toLowerCase() + s.slice(1) : s);

  // last 30 days based on latest "Date From" (uses your existing last30())
  const { start, end } = last30();

  // violent categories use yellow
  const violent = new Set(["Assault", "Battery", "Rape", "Robbery", "Kidnapping"]);

  // Build counts for the last 30 days only
  const labels = Object.keys(mostCommonQueries);
  const counts = labels.map(label => {
    const tokens = parseQuery(mostCommonQueries[label]);
    return logsData.filter(row => {
      const dt = parseFromDate(row["Date From"]);
      return !isNaN(dt) && dt >= start && dt <= end && evaluateRow(row, tokens);
    }).length;
  });

  // Headline: most common category (lowercase first letter), handle ties simply
  const max = Math.max(...counts);
  const leaders = labels.filter((_, i) => counts[i] === max);
  const leaderText = lcFirst(leaders[0]) + (leaders.length > 1 ? " (tie)" : "");
  headlineEl.innerHTML = `In the last 30 days, the most common crime was <span class="pct">${leaderText}</span>.`;

  // Colors per bar (violent = yellow, else red)
  const colors = labels.map(l => (violent.has(l) ? "#FFCC00" : "#ac2124"));

  // Render bar chart (no legend, distributed colors)
  container.innerHTML = "";
  new ApexCharts(container, {
    chart: { type: "bar", height: 320, toolbar: { show: false } },
    legend: { show: false },
    plotOptions: { bar: { borderRadius: 3, columnWidth: "55%", distributed: true } },
    dataLabels: { enabled: false },
    xaxis: { categories: labels, labels: { rotate: -30, style: { fontSize: "12px" } } },
    yaxis: { min: 0, forceNiceScale: true, title: { text: "# of incidents (last 30 days)" } },
    series: [{ name: "Incidents", data: counts }],
    colors
  }).render();
}


function buildBikeTheftChart(){
  if (!logsData?.length) return;

  const startOfMonth = d => new Date(d.getFullYear(), d.getMonth(), 1);
  const endOfMonth   = d => new Date(d.getFullYear(), d.getMonth()+1, 0, 23,59,59,999);
  const addMonths    = (d,n) => new Date(d.getFullYear(), d.getMonth()+n, 1);
  const inRange      = (dt,s,e) => !isNaN(dt) && dt >= s && dt <= e;

  const latest = latestDateFrom();
  const start30 = new Date(latest); start30.setDate(start30.getDate()-29);

  const bikeTok = parseQuery(
    'in:(Offense) "MOTOR VEHICLE THEFT - Theft of Motorized Bicycle/Scooter" or "THEFT-PETTY - Theft Bicycle"'
  );

  // Headline
  const last30Count = logsData.filter(r=>{
    const dt = parseFromDate(r["Date From"]);
    return inRange(dt, start30, latest) && evaluateRow(r, bikeTok);
  }).length;
  document.getElementById("bikeHeadline").innerHTML =
    `There were <span class="pct">${last30Count}</span> bikes, scooters, and skateboards reported stolen.`;

  // Monthly counts
  const curMonth   = startOfMonth(latest);
  const firstMonth = addMonths(curMonth, -11);
  const months     = [];
  for (let m = new Date(firstMonth); m <= curMonth; m = addMonths(m,1)) months.push(new Date(m));

  const monthlyCounts = months.map(m=>{
    const ms = startOfMonth(m), me = endOfMonth(m);
    return logsData.reduce((acc,row)=>{
      const dt = parseFromDate(row["Date From"]);
      return acc + (inRange(dt, ms, me) && evaluateRow(row, bikeTok) ? 1 : 0);
    }, 0);
  });

  const isPartialMonth = latest < endOfMonth(curMonth);

  const categories = months.map(m=>{
    const base = m.toLocaleString(undefined,{month:"short"});
    return (m.getMonth()===curMonth.getMonth() && m.getFullYear()===curMonth.getFullYear() && isPartialMonth)
      ? `${base} (MTD)` : base;
  });

  // Break the base series and create a transparent overlay for the final segment
  const baseSeries = monthlyCounts.slice();
  const overlaySeries = new Array(monthlyCounts.length).fill(null);

  if (isPartialMonth && monthlyCounts.length >= 2) {
    // remove the final point from the base so the last segment isn't drawn at full opacity
    baseSeries[baseSeries.length - 1] = null;
    // overlay only the connecting segment
    overlaySeries[overlaySeries.length - 2] = monthlyCounts[monthlyCounts.length - 2];
    overlaySeries[overlaySeries.length - 1] = monthlyCounts[monthlyCounts.length - 1];
  }

  const el = document.getElementById("bikeChart");
  el.innerHTML = "";

  const series = [{ name:"Thefts", data: baseSeries }];
  if (isPartialMonth) {
    series.push({
      name: "Thefts (MTD)",
      data: overlaySeries,
      color: "rgba(172,33,36,0.4)", // same hue, more transparent
      stroke: { width: 3, curve: "smooth" },
      markers: { size: 3, colors: "rgba(172,33,36,0.4)", strokeColors: "rgba(172,33,36,0.4)" }
    });
  }

  new ApexCharts(el, {
    chart:{ type:"line", height:320, toolbar:{show:false}, zoom:{enabled:false} },
    stroke:{ width:3, curve:"smooth" },
    markers:{ size:3 },
    series,
    colors: ["#ac2124", "rgba(172,33,36,0.4)"],
    xaxis:{
      type:"category",
      categories,
      tickPlacement:"on",
      labels:{ rotate:-30, style:{ fontSize:"12px" } },
      tooltip:{ enabled:false }
    },
    yaxis:{ min:0, forceNiceScale:true, title:{ text:"# of incidents" } },
    legend:{ show:false }
  }).render();
}




function buildPartiesChart(){
  const {start,end} = last30();

  const labels = Object.keys(partyQueries);
  const counts = labels.map(label=>{
    const tokens = parseQuery(partyQueries[label]);
    return logsData.filter(r=>{
      const dt = parseFromDate(r["Date From"]);
      return !isNaN(dt) && dt>=start && dt<=end && evaluateRow(r,tokens);
    }).length;
  });

  const shutDown = counts[labels.indexOf("Party shut down")] || 0;
  const noise = counts[labels.indexOf("Noise complaint")] || 0;

  document.getElementById("partyHeadline").innerHTML =
    `DPS shut down <span class="pct">${shutDown}</span> ${plural(shutDown,"party","parties")} and logged <span class="pct">${noise}</span> ${plural(noise,"noise complaint","noise complaints")}.`;


  new ApexCharts(document.getElementById("partyChart"), {
    chart:{ type:"bar", height:320, toolbar:{show:false} },
    plotOptions:{ bar:{ borderRadius:3, columnWidth:"55%" } },
    dataLabels:{ enabled:false },
    xaxis:{ categories:labels, labels:{ rotate:-15, style:{fontSize:"12px"} } },
    yaxis:{ min:0, forceNiceScale:true, title:{ text:"# of incidents (last 30 days)" } },
    colors:["#ac2124"],
    series:[{ name:"Incidents", data:counts }]
  }).render();
}


function buildYoYTrendChart() {
  const container = document.getElementById("yoyTrendChart");
  const headlineEl = document.getElementById("yoyHeadline");
  if (!container || !headlineEl) return;
  if (!logsData || !logsData.length) {
    headlineEl.textContent = "No data available.";
    container.innerHTML = "";
    return;
  }

  // Helper functions
  const toStartOfDay = d => new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const addDays = (d, n) => { const x = new Date(d); x.setDate(x.getDate() + n); return x; };
  const inRange = (dt, s, e) => !isNaN(dt) && dt >= s && dt <= e;

  // Property vs Violent patterns (matching Python script)
  const PROPERTY_PATTERNS = ["THEFT -", "BURGLARY -", "ARSON -", "VANDALISM -", "TRESPASS -"];
  const VIOLENT_PATTERNS = ["ROBBERY -", "ASSAULT -", "BATTERY -", "RAPE", "MURDER", "HOMICIDE", "KIDNAPPING -", "CARJACKING"];

  const categorizeOffense = (offense) => {
    if (!offense) return null;
    const upper = offense.toUpperCase();
    if (PROPERTY_PATTERNS.some(p => upper.includes(p))) return "Property";
    if (VIOLENT_PATTERNS.some(p => upper.includes(p))) return "Violent";
    return null;
  };

  // Get latest date and 13 months back
  const latest = latestDateFrom();
  const startDate = new Date(latest);
  startDate.setMonth(startDate.getMonth() - 13);

  // Filter to date range and categorize
  const filtered = logsData
    .map(r => {
      const dt = parseFromDate(r["Date From"]);
      if (isNaN(dt) || dt < startDate || dt > latest) return null;
      const category = categorizeOffense(r["Offense"]);
      if (!category) return null;
      return { date: dt, category };
    })
    .filter(r => r !== null);

  // Group by week (Monday start)
  const weekData = {};
  filtered.forEach(({ date, category }) => {
    const weekStart = new Date(date);
    const dayOfWeek = weekStart.getDay();
    const daysToMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
    weekStart.setDate(weekStart.getDate() - daysToMonday);
    weekStart.setHours(0, 0, 0, 0);
    
    const weekKey = weekStart.toISOString().split('T')[0];
    if (!weekData[weekKey]) {
      weekData[weekKey] = { Property: 0, Violent: 0 };
    }
    weekData[weekKey][category]++;
  });

  // Sort weeks and prepare data
  const weeks = Object.keys(weekData).sort();
  const propertyData = weeks.map(w => weekData[w].Property);
  const violentData = weeks.map(w => weekData[w].Violent);
  
  // Format week labels - only show month start labels
  const weekLabels = weeks.map((w, idx) => {
    const d = new Date(w);
    const prevWeek = idx > 0 ? new Date(weeks[idx - 1]) : null;
    
    // Show label if it's the first week of the month or first week overall
    if (idx === 0 || (prevWeek && d.getMonth() !== prevWeek.getMonth())) {
      return d.toLocaleString(undefined, { month: 'short', day: 'numeric' });
    }
    return ''; // Empty string for weeks that aren't month starts
  });

  // Headline calculation (last 30 days vs same period last year)
  // Last 30 days: from (latest - 29 days) to latest (inclusive, so 30 days total)
  const startCurr = addDays(latest, -29);
  const endCurr = latest;
  
  // Same period last year: exactly 365 days before
  const startPrev = new Date(startCurr);
  startPrev.setFullYear(startPrev.getFullYear() - 1);
  const endPrev = new Date(endCurr);
  endPrev.setFullYear(endPrev.getFullYear() - 1);

  const currTotal = filtered.filter(r => 
    inRange(r.date, startCurr, endCurr) && 
    (r.category === "Property" || r.category === "Violent")
  ).length;
  
  const prevTotal = filtered.filter(r => 
    inRange(r.date, startPrev, endPrev) && 
    (r.category === "Property" || r.category === "Violent")
  ).length;

  if (prevTotal < 1) {
    headlineEl.textContent = "Not enough data for a year-over-year comparison.";
  } else {
    const pct = Math.round(((currTotal - prevTotal) / prevTotal) * 100);
    const word = (pct >= 0) ? "up" : "down";
    headlineEl.innerHTML =
      `Reported crimes are <span class="trend-word">${word}</span> <span class="pct"><span>${Math.abs(pct)}</span>%</span> compared to this time last year.`;
  }

  // Find indices for highlighting periods
  // Need to match the exact same logic used for grouping weeks
  const findWeekIndices = (startDate, endDate) => {
    const indices = [];
    weeks.forEach((weekKey, idx) => {
      // Parse the week key (which is in YYYY-MM-DD format)
      const weekStart = new Date(weekKey + 'T00:00:00');
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekEnd.getDate() + 6);
      weekEnd.setHours(23, 59, 59, 999);
      
      // Normalize dates to start of day for comparison
      const start = new Date(startDate);
      start.setHours(0, 0, 0, 0);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      
      // Check if this week overlaps with the date range
      // Week overlaps if: weekStart <= endDate AND weekEnd >= startDate
      if (weekStart <= end && weekEnd >= start) {
        indices.push(idx);
      }
    });
    return indices;
  };

  // Debug: log the date ranges
  console.log('Current period:', startCurr.toISOString().split('T')[0], 'to', endCurr.toISOString().split('T')[0]);
  console.log('Previous period:', startPrev.toISOString().split('T')[0], 'to', endPrev.toISOString().split('T')[0]);

  const currIndices = findWeekIndices(startCurr, endCurr);
  const prevIndices = findWeekIndices(startPrev, endPrev);
  
  console.log('Current week indices:', currIndices);
  console.log('Previous week indices:', prevIndices);
  
  // Create annotation ranges
  const annotations = {
    xaxis: []
  };
  
  // Highlight current 30-day period
  if (currIndices.length > 0) {
    const currStartIdx = Math.min(...currIndices);
    const currEndIdx = Math.max(...currIndices);
    // For category-based charts, need to add 0.5 offset for proper alignment
    annotations.xaxis.push({
      x: currStartIdx - 0.5,
      x2: currEndIdx + 0.5,
      fillColor: 'rgba(172, 33, 36, 0.15)',
      opacity: 1,
      label: {
        text: 'Last 30 days',
        style: {
          color: '#fff',
          background: '#ac2124',
          fontSize: '11px',
          fontWeight: 600,
          padding: {
            left: 4,
            right: 4,
            top: 2,
            bottom: 2
          }
        },
        orientation: 'horizontal',
        position: 'top',
        offsetY: -10
      }
    });
  }
  
  // Highlight previous year's 30-day period
  if (prevIndices.length > 0) {
    const prevStartIdx = Math.min(...prevIndices);
    const prevEndIdx = Math.max(...prevIndices);
    annotations.xaxis.push({
      x: prevStartIdx - 0.5,
      x2: prevEndIdx + 0.5,
      fillColor: 'rgba(255, 204, 0, 0.15)',
      opacity: 1,
      label: {
        text: 'Same period last year',
        style: {
          color: '#fff',
          background: '#FFCC00',
          fontSize: '11px',
          fontWeight: 600,
          padding: {
            left: 4,
            right: 4,
            top: 2,
            bottom: 2
          }
        },
        orientation: 'horizontal',
        position: 'top',
        offsetY: -10
      }
    });
  }

  // Render stacked column chart
  container.innerHTML = "";
  new ApexCharts(container, {
    chart: {
      type: "bar",
      height: 400,
      stacked: true,
      toolbar: { show: false }
    },
    plotOptions: {
      bar: {
        horizontal: false,
        borderRadius: 3,
        columnWidth: "70%"
      }
    },
    dataLabels: { enabled: false },
    xaxis: {
      categories: weekLabels,
      labels: { rotate: -45, style: { fontSize: "11px" } }
    },
    yaxis: {
      min: 0,
      forceNiceScale: true,
      title: { text: "# of incidents" }
    },
    colors: ["#ac2124", "#FFCC00"],
    series: [
      { name: "Property crimes", data: propertyData },
      { name: "Violent crimes", data: violentData }
    ],
    legend: {
      position: "top",
      horizontalAlign: "center"
    },
    annotations: annotations
  }).render();
}


function triggerDownloadFromBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function downloadFile(path, filename, mime) {
  try {
    // Build an absolute URL and bypass cache so you get the freshest Action-updated file
    const url = new URL(path, window.location.href).toString();
    const res = await fetch(url + (url.includes('?') ? '&' : '?') + '_=' + Date.now(), { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const blob = await res.blob();

    // If server didn't send a type, force one
    const typedBlob = blob.type ? blob : new Blob([blob], { type: mime });
    triggerDownloadFromBlob(typedBlob, filename);
  } catch (err) {
    console.error('Download failed:', err);
    // Fallback: navigate to the file (lets the browser handle it)
    window.location.href = path;
  }
}

// ---- Wire up the buttons ----
const btnCSV = document.getElementById('downloadCSV');
const btnJSON = document.getElementById('downloadJSON');

if (btnCSV) {
  btnCSV.addEventListener('click', () => {
    const csvPath = '../data/usc_crime_logs.csv';
    downloadFile(csvPath, 'usc_crime_logs.csv', 'text/csv;charset=utf-8').catch(() =>
      downloadFile('data/usc_crime_logs.csv', 'usc_crime_logs.csv', 'text/csv;charset=utf-8')
    );
  });
}
if (btnJSON) {
  btnJSON.addEventListener('click', () => {
    const jsonPath = '../data/usc_crime_logs.json';
    downloadFile(jsonPath, 'usc_crime_logs.json', 'application/json;charset=utf-8').catch(() =>
      downloadFile('data/usc_crime_logs.json', 'usc_crime_logs.json', 'application/json;charset=utf-8')
    );
  });
}




  // Show warning modal on page load
  const modal = document.getElementById('warningModal');
  const closeBtn = document.getElementById('closeModal');
  
  // Check if user has dismissed before
  if (!localStorage.getItem('warningDismissed')) {
    modal.classList.add('show');
  }
  
  closeBtn.addEventListener('click', () => {
    modal.classList.remove('show');
    localStorage.setItem('warningDismissed', 'true');
  });
  
  // Close modal when clicking outside
  modal.addEventListener('click', (e) => {
    if (e.target === modal) {
      modal.classList.remove('show');
      localStorage.setItem('warningDismissed', 'true');
    }
  });

  // Init
  loadLogs();

