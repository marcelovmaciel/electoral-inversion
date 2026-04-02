const DATA_URL = "../../scraping/output/cabinet_timeline_dashboard.json";

const state = {
  data: null,
  selected: null,
  appointmentPartyIndex: new Map(),
  filters: {
    government: "all",
    ministry: "all",
    party: "all",
    review: "all",
    rangeMode: "filtered",
  },
};

const elements = {
  governmentFilter: document.querySelector("#governmentFilter"),
  ministryFilter: document.querySelector("#ministryFilter"),
  partyFilter: document.querySelector("#partyFilter"),
  reviewFilter: document.querySelector("#reviewFilter"),
  viewMode: document.querySelector("#viewMode"),
  metaLine: document.querySelector("#metaLine"),
  appointmentsCount: document.querySelector("#appointmentsCount"),
  eventsCount: document.querySelector("#eventsCount"),
  reviewCount: document.querySelector("#reviewCount"),
  interimCount: document.querySelector("#interimCount"),
  timelineAxis: document.querySelector("#timelineAxis"),
  timelineRows: document.querySelector("#timelineRows"),
  eventsTableBody: document.querySelector("#eventsTableBody"),
  partyPeriods: document.querySelector("#partyPeriods"),
  detailsPanel: document.querySelector("#detailsPanel"),
};

init();

async function init() {
  try {
    const response = await fetch(DATA_URL);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    state.data = await response.json();
    state.appointmentPartyIndex = buildAppointmentPartyIndex(state.data.appointments || []);
    elements.metaLine.textContent = `Generated ${state.data.generated_at} from ${state.data.source_pages.length} Wikipedia parses.`;
    setupControls();
    render();
  } catch (error) {
    elements.metaLine.textContent = `Failed to load ${DATA_URL}: ${error.message}`;
    elements.detailsPanel.innerHTML = `<p class="empty-state">Serve the repository root and open this page over HTTP. Expected data URL: <code>${DATA_URL}</code>.</p>`;
  }
}

function setupControls() {
  populateSelect(elements.governmentFilter, [
    { value: "all", label: "All governments" },
    ...state.data.governments.map((government) => ({
      value: government.government_id,
      label: government.label,
    })),
  ]);

  populateSelect(elements.ministryFilter, [
    { value: "all", label: "All ministries" },
    ...state.data.ministries.map((ministry) => ({
      value: ministry.ministry,
      label: ministry.ministry,
    })),
  ]);

  const parties = uniqueValues(
    state.data.appointments.flatMap((appointment) => appointment.party_codes || []),
  );
  populateSelect(elements.partyFilter, [
    { value: "all", label: "All parties" },
    ...parties.map((party) => ({ value: party, label: party })),
  ]);

  [
    elements.governmentFilter,
    elements.ministryFilter,
    elements.partyFilter,
    elements.reviewFilter,
    elements.viewMode,
  ].forEach((element) => {
    element.addEventListener("change", () => {
      state.filters.government = elements.governmentFilter.value;
      state.filters.ministry = elements.ministryFilter.value;
      state.filters.party = elements.partyFilter.value;
      state.filters.review = elements.reviewFilter.value;
      state.filters.rangeMode = elements.viewMode.value;
      render();
    });
  });
}

function populateSelect(select, options) {
  select.innerHTML = options
    .map((option) => `<option value="${escapeHtml(option.value)}">${escapeHtml(option.label)}</option>`)
    .join("");
}

function render() {
  const filteredAppointments = filterAppointments(state.data.appointments);
  const filteredEvents = filterEvents(state.data.events);
  const filteredPeriods = filterPartyPeriods(state.data.party_periods);

  elements.appointmentsCount.textContent = filteredAppointments.length.toString();
  elements.eventsCount.textContent = filteredEvents.length.toString();
  elements.reviewCount.textContent = [
    ...filteredAppointments.filter((appointment) => appointment.needs_review),
    ...filteredEvents.filter((event) => event.needs_review),
  ].length.toString();
  elements.interimCount.textContent = filteredAppointments
    .filter((appointment) => appointment.appointment_type !== "permanent")
    .length.toString();

  renderTimeline(filteredAppointments);
  renderEvents(filteredEvents);
  renderPartyPeriods(filteredPeriods);
  renderDetails(state.selected);
}

function filterAppointments(appointments) {
  return appointments.filter((appointment) => {
    if (state.filters.government !== "all" && appointment.government_id !== state.filters.government) {
      return false;
    }
    if (state.filters.ministry !== "all" && appointment.ministry !== state.filters.ministry) {
      return false;
    }
    if (state.filters.party !== "all" && !(appointment.party_codes || []).includes(state.filters.party)) {
      return false;
    }
    if (!passesReviewFilter(appointment.needs_review)) {
      return false;
    }
    return true;
  });
}

function filterEvents(events) {
  return events.filter((event) => {
    if (state.filters.government !== "all" && event.government_id !== state.filters.government) {
      return false;
    }
    if (state.filters.ministry !== "all" && event.ministerio_canonical !== state.filters.ministry) {
      return false;
    }
    if (state.filters.party !== "all" && !eventPartySummary(event).filterCodes.includes(state.filters.party)) {
      return false;
    }
    if (!passesReviewFilter(event.needs_review)) {
      return false;
    }
    return true;
  });
}

function filterPartyPeriods(periods) {
  return periods.filter((period) => {
    if (state.filters.government !== "all" && period.government_id !== state.filters.government) {
      return false;
    }
    if (state.filters.party !== "all" && !period.parties.includes(state.filters.party)) {
      return false;
    }
    return true;
  });
}

function passesReviewFilter(flag) {
  if (state.filters.review === "exclude") {
    return !flag;
  }
  if (state.filters.review === "only") {
    return flag;
  }
  return true;
}

function renderTimeline(appointments) {
  elements.timelineAxis.innerHTML = "";
  elements.timelineRows.innerHTML = "";

  if (!appointments.length) {
    elements.timelineRows.innerHTML = '<p class="empty-state">No appointments match the current filters.</p>';
    return;
  }

  const domainAppointments =
    state.filters.rangeMode === "full" ? filterAppointments(state.data.appointments) : appointments;
  const domain = computeDomain(domainAppointments);
  renderAxis(domain);

  const rows = groupBy(appointments, (appointment) => appointment.ministry);
  const ministries = Object.keys(rows).sort((a, b) => a.localeCompare(b));

  for (const ministry of ministries) {
    const rowAppointments = rows[ministry].slice().sort((a, b) => compareDates(a.start, b.start));
    const packed = assignLanes(rowAppointments);
    const row = document.createElement("div");
    row.className = "timeline-row";

    const label = document.createElement("div");
    label.className = "timeline-label";
    label.innerHTML = `
      ${escapeHtml(ministry)}
      <span class="timeline-subtle">${escapeHtml(rowAppointments[0].ministry_status_type)} · ${rowAppointments.length} interval(s)</span>
    `;

    const track = document.createElement("div");
    track.className = "timeline-track";
    const lanes = Math.max(...packed.map((item) => item.lane), 0) + 1;
    track.style.height = `${Math.max(34, lanes * 28 + 6)}px`;
    track.style.setProperty("--year-step", `${100 / Math.max(1, yearSpan(domain.start, domain.end))}%`);

    packed.forEach(({ appointment, lane }) => {
      const start = parseDate(appointment.start);
      const end = parseDate(appointment.end) || domain.end;
      const left = ((start - domain.start) / domain.spanMs) * 100;
      const width = Math.max((((end - start) / 86400000) + 1) / domain.spanDays * 100, 0.45);
      const bar = document.createElement("button");
      bar.type = "button";
      bar.className = [
        "timeline-bar",
        appointment.appointment_type,
        appointment.needs_review ? "review" : "",
      ]
        .filter(Boolean)
        .join(" ");
      bar.style.left = `${left}%`;
      bar.style.top = `${lane * 28 + 6}px`;
      bar.style.width = `${width}%`;
      bar.title = `${appointment.person} | ${appointment.start} -> ${appointment.end || "present"}`;
      bar.textContent = appointment.person || appointment.person_raw || "Unknown";
      bar.addEventListener("click", () => {
        state.selected = { type: "appointment", item: appointment };
        renderDetails(state.selected);
      });
      track.appendChild(bar);
    });

    row.appendChild(label);
    row.appendChild(track);
    elements.timelineRows.appendChild(row);
  }
}

function renderAxis(domain) {
  const axisLabel = document.createElement("div");
  axisLabel.className = "axis-label";
  axisLabel.textContent = "Timeline";
  const axisTrack = document.createElement("div");
  axisTrack.className = "axis-track";
  const startYear = domain.start.getUTCFullYear();
  const endYear = domain.end.getUTCFullYear();
  for (let year = startYear; year <= endYear; year += 1) {
    const tickDate = new Date(Date.UTC(year, 0, 1));
    const left = ((tickDate - domain.start) / domain.spanMs) * 100;
    const tick = document.createElement("span");
    tick.className = "axis-tick";
    tick.style.left = `${left}%`;
    tick.textContent = `${year}`;
    axisTrack.appendChild(tick);
  }
  elements.timelineAxis.appendChild(axisLabel);
  elements.timelineAxis.appendChild(axisTrack);
}

function renderEvents(events) {
  elements.eventsTableBody.innerHTML = "";
  if (!events.length) {
    elements.eventsTableBody.innerHTML = '<tr><td colspan="6" class="empty-state">No events match the current filters.</td></tr>';
    return;
  }

  const sorted = events.slice().sort((a, b) => compareDates(b.event_date_start, a.event_date_start));
  for (const event of sorted) {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${escapeHtml(event.event_date_start || event.event_date_display || "Unknown")}</td>
      <td>${escapeHtml(event.ministerio_canonical)}</td>
      <td>${escapeHtml(event.event_type)}</td>
      <td>${escapeHtml(event.person_name_canonical || event.person_name_raw || "—")}</td>
      <td>${escapeHtml(event.government_id || "—")}</td>
      <td>${confidencePill(event.confidence, event.needs_review)}</td>
    `;
    row.addEventListener("click", () => {
      state.selected = { type: "event", item: event };
      renderDetails(state.selected);
    });
    elements.eventsTableBody.appendChild(row);
  }
}

function renderPartyPeriods(periods) {
  elements.partyPeriods.innerHTML = "";
  if (!periods.length) {
    elements.partyPeriods.innerHTML = '<p class="empty-state">No coalition periods match the current filters.</p>';
    return;
  }
  for (const period of periods) {
    const block = document.createElement("article");
    block.className = "party-period";
    block.innerHTML = `
      <strong>${escapeHtml(period.period_id)}</strong>
      <div class="subtle">${escapeHtml(period.government_id || "—")} · ${escapeHtml(period.start)} -> ${escapeHtml(period.end)}</div>
      <div class="party-list">
        ${period.parties.map((party) => `<span class="party-chip">${escapeHtml(party)}</span>`).join("")}
      </div>
    `;
    elements.partyPeriods.appendChild(block);
  }
}

function renderDetails(selection) {
  if (!selection) {
    elements.detailsPanel.innerHTML = '<p class="empty-state">No selection yet.</p>';
    return;
  }

  if (selection.type === "appointment") {
    const appointment = selection.item;
    elements.detailsPanel.innerHTML = `
      <div class="details-block">
        <span class="detail-label">Appointment</span>
        <strong>${escapeHtml(appointment.person || appointment.person_raw || "Unknown")}</strong>
        <div class="subtle">${escapeHtml(appointment.ministry)}</div>
      </div>
      <div class="details-grid">
        ${detailRow("Window interval", `${appointment.start} -> ${appointment.end || "present"}`)}
        ${detailRow("Actual interval", `${appointment.actual_start || "unknown"} -> ${appointment.actual_end || "present"}`)}
        ${detailRow("Government", appointment.government_id)}
        ${detailRow("Party", appointmentPartySummary(appointment).display)}
        ${detailRow("Party status", appointmentPartySummary(appointment).statusLabel)}
        ${detailRow("Type", appointment.appointment_type)}
        ${detailRow("Confidence", appointment.confidence)}
        ${detailRow("Needs review", appointment.needs_review ? "yes" : "no")}
        ${detailRow("Source section", appointment.source_section || "—")}
      </div>
      <div class="details-block">
        <span class="detail-label">Source party field</span>
        <span class="detail-value">${escapeHtml(appointment.party || "—")}</span>
      </div>
      <div class="details-block">
        <span class="detail-label">Party evidence</span>
        <span class="detail-value">${escapeHtml((appointment.party_resolution || {}).evidence || "—")}</span>
      </div>
      <div class="details-block">
        <span class="detail-label">Source snippet</span>
        <span class="detail-value mono">${escapeHtml(appointment.source_snippet || "—")}</span>
      </div>
      <div class="details-block">
        <span class="detail-label">Coalition matches</span>
        <span class="detail-value">${renderCoalitionMatches(appointment.coalition_matches, appointmentPartySummary(appointment))}</span>
      </div>
      <div class="details-block">
        <span class="detail-label">Notes</span>
        <span class="detail-value">${escapeHtml(appointment.notes || "—")}</span>
      </div>
      <div class="details-block">
        <span class="detail-label">Source URL</span>
        <a class="detail-value" href="${appointment.source_url}" target="_blank" rel="noreferrer">${escapeHtml(appointment.source_url)}</a>
      </div>
    `;
    return;
  }

  const event = selection.item;
  const partySummary = eventPartySummary(event);
  elements.detailsPanel.innerHTML = `
    <div class="details-block">
      <span class="detail-label">Event</span>
      <strong>${escapeHtml(event.event_type)}</strong>
      <div class="subtle">${escapeHtml(event.ministerio_canonical)}</div>
    </div>
    <div class="details-grid">
      ${detailRow("Date", event.event_date_start || event.event_date_display || "unknown")}
      ${detailRow("Government", event.government_id || "—")}
      ${detailRow("Person", event.person_name_canonical || event.person_name_raw || "—")}
      ${detailRow("Party", partySummary.display)}
      ${detailRow("Party status", partySummary.statusLabel)}
      ${detailRow("Role classification", event.role_classification || "—")}
      ${detailRow("Confidence", event.confidence)}
      ${detailRow("Needs review", event.needs_review ? "yes" : "no")}
      ${detailRow("Source section", event.source_section || "—")}
      ${detailRow(
        "Source locator",
        `table ${event.source_locator.table_index}, row ${event.source_locator.row_index}, ${event.source_locator.column_name}`,
      )}
    </div>
    <div class="details-block">
      <span class="detail-label">Source party field</span>
      <span class="detail-value">${escapeHtml(event.party || "—")}</span>
    </div>
    <div class="details-block">
      <span class="detail-label">Party evidence</span>
      <span class="detail-value">${escapeHtml(partySummary.evidence)}</span>
    </div>
    <div class="details-block">
      <span class="detail-label">Source snippet</span>
      <span class="detail-value mono">${escapeHtml(event.source_snippet || "—")}</span>
    </div>
    <div class="details-block">
      <span class="detail-label">Notes</span>
      <span class="detail-value">${escapeHtml(event.notes || "—")}</span>
    </div>
    <div class="details-block">
      <span class="detail-label">Source URL</span>
      <a class="detail-value" href="${event.source_url}" target="_blank" rel="noreferrer">${escapeHtml(event.source_url)}</a>
    </div>
  `;
}

function renderCoalitionMatches(matches, partySummary = null) {
  if (partySummary && partySummary.status === "unresolved") {
    return "Unresolved party; coalition match intentionally withheld.";
  }
  if (partySummary && partySummary.status === "missing") {
    return "No usable party code; coalition match not attempted.";
  }
  if (!matches || !matches.length) {
    return "No explicit coalition party match for this interval.";
  }
  return matches
    .map((match) => `${match.period_id} (${match.matching_parties.join(", ")})`)
    .join("; ");
}

function detailRow(label, value) {
  return `
    <div>
      <span class="detail-label">${escapeHtml(label)}</span>
      <span class="detail-value">${escapeHtml(value || "—")}</span>
    </div>
  `;
}

function confidencePill(confidence, needsReview) {
  const reviewPill = needsReview ? '<span class="pill review">review</span>' : "";
  return `<span class="pill ${escapeHtml(confidence || "medium")}">${escapeHtml(confidence || "medium")}</span> ${reviewPill}`;
}

function buildAppointmentPartyIndex(appointments) {
  const index = new Map();
  for (const appointment of appointments) {
    const person = appointment.person || appointment.person_raw;
    const ministry = appointment.ministry;
    const government = appointment.government_id;
    if (government && ministry && person && appointment.start) {
      index.set(eventPartyKey(government, ministry, appointment.start, person), appointment);
    }
    if (government && ministry && person && appointment.end) {
      index.set(eventPartyKey(government, ministry, appointment.end, person), appointment);
    }
  }
  return index;
}

function eventPartyKey(governmentId, ministry, dateValue, person) {
  return [governmentId || "", ministry || "", dateValue || "", person || ""].join("|");
}

function appointmentPartySummary(appointment) {
  const resolution = appointment.party_resolution || {};
  const candidates = appointment.party_candidates || resolution.candidate_parties || [];
  if (resolution.status === "resolved" && appointment.resolved_party) {
    return {
      status: "resolved",
      statusLabel: "resolved",
      display: appointment.resolved_party,
      filterCodes: appointment.party_codes || [appointment.resolved_party],
      evidence: resolution.evidence || "Resolved from repository evidence.",
    };
  }
  if (resolution.status === "unresolved") {
    const label = candidates.length ? candidates.join(" / ") : appointment.party || "unknown";
    return {
      status: "unresolved",
      statusLabel: "unresolved",
      display: `Unresolved (${label})`,
      filterCodes: [],
      evidence: resolution.evidence || "Repository evidence was insufficient to fix one start-date party.",
    };
  }
  if (resolution.status === "missing" || !(appointment.party_codes || []).length) {
    return {
      status: "missing",
      statusLabel: "missing",
      display: "No party code",
      filterCodes: [],
      evidence: resolution.evidence || "Source row does not expose a usable party code.",
    };
  }
  return {
    status: "raw",
    statusLabel: "raw",
    display: appointment.party || "—",
    filterCodes: appointment.party_codes || [],
    evidence: resolution.evidence || "Using raw dashboard data.",
  };
}

function eventPartySummary(event) {
  const person = event.person_name_canonical || event.person_name_raw;
  const linkedAppointment = state.appointmentPartyIndex.get(
    eventPartyKey(event.government_id, event.ministerio_canonical, event.event_date_start, person),
  );
  if (linkedAppointment) {
    return appointmentPartySummary(linkedAppointment);
  }
  const rawCodes = extractEventPartyCodes(event.party);
  if (rawCodes.length === 1 && !event.needs_review) {
    return {
      status: "raw",
      statusLabel: "raw",
      display: rawCodes[0],
      filterCodes: rawCodes,
      evidence: event.notes || "Using the single raw party token from the event row.",
    };
  }
  if (rawCodes.length > 1 || event.needs_review) {
    return {
      status: "unresolved",
      statusLabel: "unresolved",
      display: `Unresolved (${event.party || "multiple candidates"})`,
      filterCodes: [],
      evidence: event.notes || "Raw event party string is ambiguous and is not treated as settled coalition truth.",
    };
  }
  return {
    status: "missing",
    statusLabel: "missing",
    display: "No party code",
    filterCodes: [],
    evidence: event.notes || "Event row does not expose a usable party code.",
  };
}

function computeDomain(appointments) {
  const generatedAt = parseDate(state.data.generated_at.slice(0, 10));
  const starts = appointments.map((appointment) => parseDate(appointment.start)).filter(Boolean);
  const ends = appointments
    .map((appointment) => parseDate(appointment.end) || generatedAt)
    .filter(Boolean);
  const start = new Date(Math.min(...starts.map((item) => item.getTime())));
  const end = new Date(Math.max(...ends.map((item) => item.getTime())));
  const spanMs = Math.max(end - start, 86400000);
  const spanDays = Math.max(Math.round(spanMs / 86400000), 1);
  return { start, end, spanMs, spanDays };
}

function assignLanes(appointments) {
  const laneEnds = [];
  return appointments.map((appointment) => {
    const start = parseDate(appointment.start);
    const end = parseDate(appointment.end) || parseDate(state.data.generated_at.slice(0, 10));
    let lane = laneEnds.findIndex((laneEnd) => start > laneEnd);
    if (lane === -1) {
      lane = laneEnds.length;
      laneEnds.push(end);
    } else {
      laneEnds[lane] = end;
    }
    return { appointment, lane };
  });
}

function extractEventPartyCodes(rawParty) {
  if (!rawParty) {
    return [];
  }
  return rawParty
    .replace(/\s*\/\s*/g, "/")
    .split("/")
    .map((part) => part.trim())
    .filter((part) => part && !["Sem partido"].includes(part) && !isStateCode(part));
}

function isStateCode(value) {
  return /^[A-Z]{2}$/.test(value);
}

function groupBy(items, keyFn) {
  return items.reduce((accumulator, item) => {
    const key = keyFn(item);
    if (!accumulator[key]) {
      accumulator[key] = [];
    }
    accumulator[key].push(item);
    return accumulator;
  }, {});
}

function uniqueValues(items) {
  return Array.from(new Set(items))
    .filter((item) => item && !/^[-—–]+$/.test(item))
    .sort((a, b) => a.localeCompare(b));
}

function compareDates(left, right) {
  const leftTime = left ? parseDate(left).getTime() : Number.NEGATIVE_INFINITY;
  const rightTime = right ? parseDate(right).getTime() : Number.NEGATIVE_INFINITY;
  return leftTime - rightTime;
}

function parseDate(value) {
  if (!value) {
    return null;
  }
  return new Date(`${value}T00:00:00Z`);
}

function yearSpan(start, end) {
  return Math.max(end.getUTCFullYear() - start.getUTCFullYear() + 1, 1);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
