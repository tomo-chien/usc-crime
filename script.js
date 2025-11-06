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
    
    // Clean up YoY chart legend when switching to dashboard tab
    if (btn.dataset.tab === "dashboard") {
      // Call the global hide function if it exists
      if (window.hideYoYPartialSeries) {
        setTimeout(() => {
          window.hideYoYPartialSeries();
        }, 50);
        setTimeout(() => {
          window.hideYoYPartialSeries();
        }, 150);
        setTimeout(() => {
          window.hideYoYPartialSeries();
        }, 300);
      } else {
        // Fallback: direct cleanup
        setTimeout(() => {
          const yoyContainer = document.getElementById("yoyTrendChart");
          if (yoyContainer) {
            const legendItems = yoyContainer.querySelectorAll('.apexcharts-legend-series');
            legendItems.forEach((item, index) => {
              const textContent = (item.textContent || '').toLowerCase();
              if (textContent.includes('(partial)') || index === 1 || index === 3) {
                item.style.setProperty('display', 'none', 'important');
                item.style.setProperty('visibility', 'hidden', 'important');
                item.style.setProperty('opacity', '0', 'important');
                item.style.setProperty('width', '0', 'important');
                item.style.setProperty('height', '0', 'important');
              }
            });
          }
        }, 150);
      }
    }
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
      // Primary fields shown collapsed: Final Incident, Date Reported, Location
      const primaryFields=["Final Incident","Date Reported","Location"];
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
  try {
    // Add cache-busting parameter to ensure fresh data
    const response = await fetch("data/usc_crime_logs.json?" + new Date().getTime());
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    logsData = await response.json();
    if (!Array.isArray(logsData)) {
      throw new Error('Data is not an array');
    }
  } catch (error) {
    console.error('Error loading logs data:', error);
    logsData = [];
    // Show error message to user
    const dashboard = document.getElementById('dashboard');
    if (dashboard) {
      dashboard.innerHTML = '<p style="color: red; padding: 20px;">Error loading data. Please refresh the page.</p>';
    }
    return;
  }

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
  try {
    buildYoYTrendChart();        // Year-over-year stacked column chart
  } catch (error) {
    console.error('Error building YoY trend chart:', error);
  }
  try {
    buildMostCommonChart();     // all-time most common types
  } catch (error) {
    console.error('Error building most common chart:', error);
  }
  try {
    buildBikeTheftChart();      // 12-month line + last-30 headline
  } catch (error) {
    console.error('Error building bike theft chart:', error);
  }
  try {
    buildPartiesChart();        // last-30 bars + headline
  } catch (error) {
    console.error('Error building parties chart:', error);
  }
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
  const isMobile = window.innerWidth <= 768;
  new ApexCharts(container, {
    chart: { 
      type: "bar", 
      height: isMobile ? 360 : 320, 
      toolbar: { show: false },
      offsetX: isMobile ? 0 : 0,
      offsetY: isMobile ? 0 : 0
    },
    legend: { show: false },
    plotOptions: { bar: { borderRadius: 3, columnWidth: "55%", distributed: true } },
    dataLabels: { enabled: false },
    xaxis: { 
      categories: labels, 
      labels: { 
        rotate: isMobile ? -45 : -30, 
        style: { fontSize: isMobile ? "10px" : "12px" },
        maxHeight: isMobile ? 100 : undefined,
        trim: false,
        hideOverlappingLabels: false
      },
      // More spacing on mobile
      offsetX: isMobile ? 8 : 0,
      offsetY: isMobile ? 20 : 0
    },
    yaxis: { 
      min: 0, 
      forceNiceScale: true, 
      title: { 
        text: isMobile ? "# incidents" : "# of incidents (last 30 days)",
        offsetX: isMobile ? -10 : 0,
        style: { fontSize: isMobile ? "11px" : "13px" }
      },
      labels: {
        style: { fontSize: isMobile ? "11px" : "12px" }
      }
    },
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

  const el = document.getElementById("bikeChart");
  el.innerHTML = "";

  // For line charts, we need to split into two series if MTD is partial
  // to show the last segment as transparent
  let seriesData = [];
  if (isPartialMonth && monthlyCounts.length > 1) {
    // Split into two series: complete months + MTD month
    // Complete data: all values except the last one (which will be null)
    const completeData = monthlyCounts.map((val, idx) => 
      idx === monthlyCounts.length - 1 ? null : val
    );
    // MTD data: nulls for all except the last two points (to connect the segment)
    const mtdData = monthlyCounts.map((val, idx) => 
      idx < monthlyCounts.length - 2 ? null : val
    );
    
    seriesData = [
      { name:"Thefts", data:completeData, color:"#ac2124" },
      { name:"Thefts (MTD)", data:mtdData, color:"rgba(172, 33, 36, 0.4)" }
    ];
  } else {
    seriesData = [{ name:"Thefts", data:monthlyCounts, color:"#ac2124" }];
  }

  new ApexCharts(el, {
    chart:{ 
      type:"line", 
      height:320, 
      toolbar:{show:false},
      zoom:{ enabled:false }
    },
    stroke:{
      curve:"smooth",
      width:3
    },
    series:seriesData.map(s => ({ name:s.name, data:s.data })),
    colors:seriesData.map(s => s.color),
    xaxis:{
      type:"category",
      categories,
      labels:{ rotate:-30, style:{ fontSize:"12px" } }
    },
    yaxis:{ min:0, forceNiceScale:true, title:{ text:"# of incidents" } },
    legend:{ show:false },
    dataLabels:{ enabled:false },
    markers:{
      size:4,
      hover:{ size:6 }
    }
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


  const isMobile = window.innerWidth <= 768;
  new ApexCharts(document.getElementById("partyChart"), {
    chart:{ 
      type:"bar", 
      height: isMobile ? 360 : 320, 
      toolbar:{show:false},
      offsetX: isMobile ? 0 : 0,
      offsetY: isMobile ? 0 : 0
    },
    plotOptions:{ bar:{ borderRadius:3, columnWidth:"55%" } },
    dataLabels:{ enabled:false },
    xaxis:{ 
      categories:labels, 
      labels:{ 
        rotate: isMobile ? -45 : -15, 
        style:{ fontSize: isMobile ? "10px" : "12px" },
        maxHeight: isMobile ? 100 : undefined,
        trim: false,
        hideOverlappingLabels: false
      },
      offsetX: isMobile ? 8 : 0,
      offsetY: isMobile ? 20 : 0
    },
    yaxis:{ 
      min:0, 
      forceNiceScale:true, 
      title:{ 
        text: isMobile ? "# incidents" : "# of incidents (last 30 days)",
        offsetX: isMobile ? -10 : 0,
        style: { fontSize: isMobile ? "11px" : "13px" }
      },
      labels: {
        style: { fontSize: isMobile ? "11px" : "12px" }
      }
    },
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
    weekStart.setHours(0, 0, 0, 0);
    const dayOfWeek = weekStart.getDay();
    const daysToMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
    weekStart.setDate(weekStart.getDate() - daysToMonday);
    
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
  
  // Check if the latest week is complete (week should end on Sunday)
  const lastWeekStart = new Date(weeks[weeks.length - 1]);
  const lastWeekEnd = new Date(lastWeekStart);
  lastWeekEnd.setDate(lastWeekEnd.getDate() + 6); // Add 6 days to get Sunday
  const isLastWeekComplete = latest >= lastWeekEnd;
  
  // Format week labels - only show month start labels for display
  // But keep the actual week date strings for categories (needed for annotations)
  const weekLabelsForDisplay = weeks.map((w, idx) => {
    const d = new Date(w);
    const prevWeek = idx > 0 ? new Date(weeks[idx - 1]) : null;
    
    // Show label only if it's the first week of a month (not just the first week overall)
    if (prevWeek && d.getMonth() !== prevWeek.getMonth()) {
      return d.toLocaleString(undefined, { month: 'short', day: 'numeric' });
    }
    // Also show first label if it's the 1st of the month
    if (idx === 0 && d.getDate() <= 7) {
      // Check if it's close to the start of the month (within first week)
      const firstOfMonth = new Date(d.getFullYear(), d.getMonth(), 1);
      const daysDiff = Math.abs((d - firstOfMonth) / (1000 * 60 * 60 * 24));
      if (daysDiff <= 6) {
        return d.toLocaleString(undefined, { month: 'short', day: 'numeric' });
      }
    }
    return ''; // Empty string for weeks that aren't month starts
  });
  
  // Use actual week date strings for categories (needed for annotations to work)
  const weekLabels = weeks;

  // Headline calculation (last 30 days vs same period last year)
  // Last 30 days: from (latest - 29 days) to latest (inclusive, so 30 days total)
  const startCurr = addDays(latest, -29);
  const endCurr = new Date(latest); // Make a copy to avoid modifying latest
  
  // Same period last year: exactly 365 days before
  const startPrev = new Date(startCurr);
  startPrev.setFullYear(startPrev.getFullYear() - 1);
  const endPrev = new Date(endCurr);
  endPrev.setFullYear(endPrev.getFullYear() - 1);

  // Normalize dates for comparison (set to start/end of day)
  // Use start of day for start dates, end of day for end dates
  const startCurrNorm = new Date(startCurr.getFullYear(), startCurr.getMonth(), startCurr.getDate());
  const endCurrNorm = new Date(endCurr.getFullYear(), endCurr.getMonth(), endCurr.getDate(), 23, 59, 59, 999);
  
  const startPrevNorm = new Date(startPrev.getFullYear(), startPrev.getMonth(), startPrev.getDate());
  const endPrevNorm = new Date(endPrev.getFullYear(), endPrev.getMonth(), endPrev.getDate(), 23, 59, 59, 999);

  // Count incidents in the normalized date ranges
  const currTotal = filtered.filter(r => 
    inRange(r.date, startCurrNorm, endCurrNorm) && 
    (r.category === "Property" || r.category === "Violent")
  ).length;
  
  const prevTotal = filtered.filter(r => 
    inRange(r.date, startPrevNorm, endPrevNorm) && 
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

  // Find the weeks that fall within the last 30 days
  const lastBarIdx = weeks.length - 1;
  const currStartIdx = Math.max(0, lastBarIdx - 3); // Last 4 bars (approximately 4 weeks = ~30 days)
  const currEndIdx = lastBarIdx;
  
  // Find the weeks from approximately one year prior (52 weeks ago)
  const weeksPerYear = 52;
  const prevLastBarIdx = Math.max(0, lastBarIdx - weeksPerYear);
  const prevStartIdx = Math.max(0, prevLastBarIdx - 3); // 4 bars from a year ago
  
  // Create annotation ranges
  // For stacked bar charts with category type, try using both xaxis and points annotations
  const annotations = {
    xaxis: [],
    points: []
  };
  
  // Highlight current 30-day period (last 4 bars)
  // Use the actual week date strings for category matching
  const currAnnotation = {
    x: weeks[currStartIdx],  // Use actual week date string
    x2: weeks[currEndIdx],   // Use actual week date string
    fillColor: 'rgba(172, 33, 36, 0.2)',
    opacity: 0.8,
    borderColor: 'rgba(172, 33, 36, 0.4)',
    borderWidth: 1,
    label: {
      text: 'Last 30 days',
      style: {
        color: '#666',
        background: 'rgba(255, 255, 255, 0.9)',
        fontSize: '11px',
        fontWeight: 500,
        padding: {
          left: 4,
          right: 4,
          top: 6,
          bottom: 6
        }
      },
      orientation: 'vertical',
      position: 'top',
      offsetY: -10
    }
  };
  annotations.xaxis.push(currAnnotation);
  
  // Highlight previous year's 30-day period
  if (prevLastBarIdx >= 0 && prevStartIdx < weeks.length) {
    const prevAnnotation = {
      x: weeks[prevStartIdx],  // Use actual week date string
      x2: weeks[Math.min(prevLastBarIdx, weeks.length - 1)],  // Use actual week date string
      fillColor: 'rgba(172, 33, 36, 0.15)',  // Red, slightly lighter than current period
      opacity: 0.8,
      borderColor: 'rgba(172, 33, 36, 0.4)',
      borderWidth: 1,
      label: {
        text: 'Same period last year',
        style: {
          color: '#666',
          background: 'rgba(255, 255, 255, 0.9)',
          fontSize: '11px',
          fontWeight: 500,
          padding: {
            left: 4,
            right: 4,
            top: 6,
            bottom: 6
          }
        },
        orientation: 'vertical',
        position: 'top',
        offsetY: -10
      }
    };
    annotations.xaxis.push(prevAnnotation);
  }

  // Create data arrays with transparency for incomplete last week
  const lastWeekIdx = weeks.length - 1;
  const isLastWeekIncomplete = !isLastWeekComplete;
  
  // For stacked bars, split into separate series if the last week is incomplete
  // This allows us to use transparent colors for just that week
  let propertySeries = [];
  let violentSeries = [];
  let chartColors = ["#ac2124", "#FFCC00"];
  
  // Store original data for tooltip access
  const originalPropertyData = [...propertyData];
  const originalViolentData = [...violentData];
  const hasPartialSeries = isLastWeekIncomplete && propertyData.length > 0;
  
  if (hasPartialSeries) {
    // Create data arrays excluding the last week
    const propertyDataComplete = propertyData.slice(0, -1);
    const violentDataComplete = violentData.slice(0, -1);
    
    // Create arrays for the last week only (with nulls before)
    const propertyDataLastWeek = new Array(propertyData.length - 1).fill(null).concat([propertyData[lastWeekIdx]]);
    const violentDataLastWeek = new Array(violentData.length - 1).fill(null).concat([violentData[lastWeekIdx]]);
    
    propertySeries = [
      { name: "Property crimes", data: propertyDataComplete },
      { name: "Property crimes (partial)", data: propertyDataLastWeek }
    ];
    violentSeries = [
      { name: "Violent crimes", data: violentDataComplete },
      { name: "Violent crimes (partial)", data: violentDataLastWeek }
    ];
    chartColors = ["#ac2124", "rgba(172, 33, 36, 0.4)", "#FFCC00", "rgba(255, 204, 0, 0.4)"];
  } else {
    propertySeries = [{ name: "Property crimes", data: propertyData }];
    violentSeries = [{ name: "Violent crimes", data: violentData }];
  }

  // Render stacked column chart
  container.innerHTML = "";
  const isMobile = window.innerWidth <= 768;
  const chart = new ApexCharts(container, {
    chart: {
      type: "bar",
      height: isMobile ? 440 : 400,
      stacked: true,
      toolbar: { show: false },
      offsetX: isMobile ? 0 : 0,
      offsetY: isMobile ? 0 : 0,
      events: {
        // Disable legend click functionality
        legendClick: function(chartContext, seriesIndex, config) {
          return false; // Prevent default behavior
        }
      }
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
      type: 'category',
      categories: weekLabels,  // Use actual week date strings for annotations to work
      labels: { 
        rotate: window.innerWidth <= 768 ? -60 : -45, 
        style: { fontSize: window.innerWidth <= 768 ? "10px" : "11px" },
        maxHeight: window.innerWidth <= 768 ? 90 : undefined,
        trim: false,
        hideOverlappingLabels: false,
        // Custom formatter to show only month start labels
        formatter: function(value, timestamp, opts) {
          // opts.dataPointIndex should give us the index
          const idx = opts.dataPointIndex;
          if (idx !== undefined && idx >= 0 && idx < weekLabelsForDisplay.length) {
            return weekLabelsForDisplay[idx] || '';
          }
          // Fallback: try to parse the value as a date string
          if (value && typeof value === 'string') {
            const d = new Date(value);
            if (!isNaN(d.getTime())) {
              const weekIdx = weeks.indexOf(value);
              if (weekIdx >= 0 && weekIdx < weekLabelsForDisplay.length) {
                return weekLabelsForDisplay[weekIdx] || '';
              }
            }
          }
          return '';
        }
      },
      offsetX: window.innerWidth <= 768 ? 8 : 0,
      offsetY: window.innerWidth <= 768 ? 20 : 0
    },
    yaxis: {
      min: 0,
      forceNiceScale: true,
      title: { 
        text: window.innerWidth <= 768 ? "# incidents" : "# of incidents",
        offsetX: window.innerWidth <= 768 ? -10 : 0,
        style: { fontSize: window.innerWidth <= 768 ? "11px" : "13px" }
      },
      labels: {
        style: { fontSize: window.innerWidth <= 768 ? "11px" : "12px" }
      }
    },
    colors: chartColors,
    series: [...propertySeries, ...violentSeries],
    legend: {
      position: "top",
      horizontalAlign: "center",
      show: true,
      showForNullSeries: false,
      showForZeroSeries: false,
      // Only show the first two series (Property and Violent, excluding partial series)
      showForSeries: hasPartialSeries ? [0, 2] : undefined,
      formatter: function(seriesName, opts) {
        // Hide the "(partial)" series from the legend
        return seriesName.includes("(partial)") ? "" : seriesName;
      }
    },
    tooltip: {
      custom: function({ series, seriesIndex, dataPointIndex, w }) {
        // Get the week date string from categories using the chart's globals
        const weekDateStr = w.globals.categoryLabels[dataPointIndex] || weekLabels[dataPointIndex];
        const weekDate = new Date(weekDateStr);
        
        // Format the week start date
        const weekStartFormatted = weekDate.toLocaleDateString('en-US', { 
          weekday: 'short',
          month: 'short', 
          day: 'numeric',
          year: 'numeric'
        });
        
        // Get property and violent crime counts from original data
        // Use the original data arrays we stored earlier
        const propertyCount = originalPropertyData[dataPointIndex] || 0;
        const violentCount = originalViolentData[dataPointIndex] || 0;
        const total = propertyCount + violentCount;
        
        return `
          <div style="padding: 8px 12px; background: white; border: 1px solid #e2e8f0; border-radius: 6px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
            <div style="font-weight: 600; margin-bottom: 6px; color: #2d3748; font-size: 13px;">
              Week of ${weekStartFormatted}
            </div>
            <div style="font-size: 12px; color: #4a5568;">
              <div style="margin: 4px 0;">
                <span style="color: #ac2124; font-weight: 600;">Property crimes:</span> 
                <span style="font-weight: 600;">${propertyCount}</span>
              </div>
              <div style="margin: 4px 0;">
                <span style="color: #FFCC00; font-weight: 600;">Violent crimes:</span> 
                <span style="font-weight: 600;">${violentCount}</span>
              </div>
              <div style="margin-top: 6px; padding-top: 6px; border-top: 1px solid #e2e8f0; font-weight: 600; color: #2d3748;">
                Total: ${total}
              </div>
            </div>
          </div>
        `;
      }
    },
    annotations: annotations
  });
  
  chart.render();
  
  // Function to hide partial series from legend - more aggressive approach
  const hidePartialSeries = () => {
    if (!hasPartialSeries) return;
    
    const legendItems = container.querySelectorAll('.apexcharts-legend-series');
    legendItems.forEach((item, index) => {
      // Always disable interactivity
      item.style.pointerEvents = 'none';
      item.style.cursor = 'default';
      
      // Hide partial series by checking the text content
      const textContent = (item.textContent || '').toLowerCase();
      const isPartial = textContent.includes('(partial)') || textContent.trim() === '';
      
      // Hide by index (indices 1 and 3 are partial series)
      if (index === 1 || index === 3 || isPartial) {
        item.style.setProperty('display', 'none', 'important');
        item.style.setProperty('visibility', 'hidden', 'important');
        item.style.setProperty('opacity', '0', 'important');
        item.style.setProperty('width', '0', 'important');
        item.style.setProperty('height', '0', 'important');
        item.style.setProperty('margin', '0', 'important');
        item.style.setProperty('padding', '0', 'important');
        item.style.setProperty('overflow', 'hidden', 'important');
        item.style.setProperty('position', 'absolute', 'important');
        item.style.setProperty('left', '-9999px', 'important');
        // Also remove from DOM flow
        item.setAttribute('aria-hidden', 'true');
        item.setAttribute('tabindex', '-1');
      }
    });
  };
  
  // Run immediately and on multiple intervals to catch all render cycles
  const intervals = [50, 100, 200, 500, 1000];
  intervals.forEach(delay => {
    setTimeout(hidePartialSeries, delay);
  });
  
  // Also hide when chart is updated
  if (chart.events) {
    chart.events.on('dataUpdated', hidePartialSeries);
    chart.events.on('updated', hidePartialSeries);
    chart.events.on('rendered', hidePartialSeries);
  }
  
  // Use MutationObserver to catch when legend items are recreated
  const observer = new MutationObserver(() => {
    hidePartialSeries();
  });
  
  // Observe the entire chart container for changes
  setTimeout(() => {
    if (container) {
      observer.observe(container, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ['class', 'style']
      });
    }
  }, 300);
  
  // Store the hide function globally so it can be called from tab switches
  if (!window.hideYoYPartialSeries) {
    window.hideYoYPartialSeries = hidePartialSeries;
  }
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
    downloadFile('data/usc_crime_logs.csv', 'usc_crime_logs.csv', 'text/csv;charset=utf-8');
  });
}
if (btnJSON) {
  btnJSON.addEventListener('click', () => {
    downloadFile('data/usc_crime_logs.json', 'usc_crime_logs.json', 'application/json;charset=utf-8');
  });
}




  // Initialize when DOM is ready
  function init() {
    // Show warning modal on page load
    const modal = document.getElementById('warningModal');
    const closeBtn = document.getElementById('closeModal');
    
    if (modal && closeBtn) {
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
    }

    // Check if ApexCharts is loaded
    if (typeof ApexCharts === 'undefined') {
      console.error('ApexCharts is not loaded');
      return;
    }

    // Init
    try {
      loadLogs();
    } catch (error) {
      console.error('Error loading logs:', error);
    }
  }

  // Run when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    // DOM is already ready
    init();
  }

