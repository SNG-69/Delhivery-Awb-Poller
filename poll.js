require('dotenv').config();
const axios = require('axios');
const dayjs = require('dayjs');

// ---------- Config ----------
axios.defaults.timeout = 20000;
axios.defaults.headers.common['User-Agent'] = 'instasport-delhivery-jira-sync/1.2';

// Env
const DELHIVERY_TOKEN = process.env.DELHIVERY_TOKEN;
const JIRA_DOMAIN = process.env.JIRA_DOMAIN;
const JIRA_EMAIL = process.env.JIRA_EMAIL;
const JIRA_API_TOKEN = process.env.JIRA_API_TOKEN;
const JIRA_PROJECT = process.env.JIRA_PROJECT;

const TRACKING_FIELD = process.env.TRACKING_FIELD; // e.g. "customfield_12345"
const DISPATCH_DATE_FIELD = process.env.CUSTOMFIELD_DISPATCH_DATE;
const DELIVERY_DATE_FIELD = process.env.CUSTOMFIELD_DELIVERY_DATE;
const RTO_DELIVERED_DATE_FIELD = process.env.CUSTOMFIELD_RTO_DATE;
const POST_DELIVERY_ASSIGNEE = process.env.POST_DELIVERY_ASSIGNEE || '712020:d710d4e8-270f-4d7a-b65a-7303f71783fb';

// Write latest Delhivery instruction into this Jira field (short plain text)
const LATEST_INSTRUCTION_FIELD = 'customfield_10288'; // "Latest Delhivery Comments"

// Out for Delivery Date (date picker) — write-once
const OUT_FOR_DELIVERY_DATE_FIELD = 'customfield_10321';

// Promised Delivery Date (forward EDD) — write-once
const PROMISED_DELIVERY_DATE_FIELD = 'customfield_10354';

// Latest PDD (forward) — overwrite allowed (ExpectedDeliveryDate || PromisedDeliveryDate)
const LATEST_PDD_FIELD = 'customfield_10357';

// RTO fields (write-once)
const RTO_REASON_FIELD = 'customfield_10355';          // Short text
const RTO_INITIATED_DATE_FIELD = 'customfield_10356';  // Date Picker

// Diagnostics / knobs
const CREATED_SINCE_DAYS = Number(process.env.CREATED_SINCE_DAYS || 45);
const DEBUG_ISSUE_KEY = process.env.DEBUG_ISSUE_KEY || '';     // e.g. "OPS-1234"
const DEBUG_AWB = process.env.DEBUG_AWB || '';                 // e.g. "29798810134374"
const LOG_TRANSITIONS = process.env.LOG_TRANSITIONS === '1';
const SLEEP_MS = Number(process.env.SLEEP_MS || 200);

// ---------- Mappings ----------
const STATUS_MAP = {
  'Ready for pickup': 'PICKUP SCHEDULED',
  'In Transit': 'IN - TRANSIT',
  'Delivered': 'DELIVERED',
  'Out for delivery': 'OUT FOR DELIVERY',
  'RTO': 'RTO IN - TRANSIT',
  'RTO - In Transit': 'RTO IN - TRANSIT',
  'In Transit For Return': 'RTO IN - TRANSIT',
  'Delayed': 'IN - TRANSIT',
  'DELAYED': 'IN - TRANSIT',
  'On Time': 'IN - TRANSIT',
  'RTO - Returned': 'RTO DELIVERED',
  'Cancelled': 'PICKUP EXCEPTION - DELHIVERY',
  'Shipment delivery cancelled via OTP': 'PICKUP EXCEPTION - DELHIVERY',
  'NDR': 'NDR',
  'Manifested': 'PICKUP SCHEDULED',
  'Pending': 'PICKUP SCHEDULED',
  'Not Picked': 'PICKUP EXCEPTION - DELHIVERY',
  'Dispatched': 'IN - TRANSIT'
};

const JIRA_STATUS_ALIASES = {
  'RTO IN - TRANSIT': ['RTO IN - TRANSIT','RTO IN-TRANSIT','RTO IN TRANSIT','Return In-Transit','Return In Transit','RTO In Transit'],
  'IN - TRANSIT': ['IN - TRANSIT','IN-TRANSIT','IN TRANSIT'],
  'PICKUP SCHEDULED': ['PICKUP SCHEDULED','Pickup Scheduled'],
  'DELIVERED': ['DELIVERED','Delivered'],
  'RTO DELIVERED': ['RTO DELIVERED','RETURN DELIVERED','Return Delivered']
};

// --- Verified-cancellation triggers ---
const VERIFIED_CXL_PHRASES = [
  'whatsapp verified cancellation',
  'code verified cancellation',
  'consignee refused to accept/order cancelled'
];
const VERIFIED_CXL_RE = new RegExp(
  VERIFIED_CXL_PHRASES.map(p => p.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|'),
  'i'
);

// ---------- Utils ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const nameNorm = (s) => (s || '').toLowerCase().replace(/[\s_\-]+/g, '');

// Convert "customfield_10123" -> "cf[10123]" for bulletproof JQL
const toCfIdExpr = (customfield) => {
  const m = (customfield || '').match(/customfield_(\d+)/);
  return m ? `cf[${m[1]}]` : null;
};

// ---------- Jira helpers ----------
const jiraBase = () => (JIRA_DOMAIN || '').replace(/\/+$/, '');
const jiraAuth = { username: JIRA_EMAIL, password: JIRA_API_TOKEN };

const jiraAxiosCfg = {
  timeout: 20000,
  headers: {
    'User-Agent': 'instasport-delhivery-jira-sync/1.2',
    'Accept': 'application/json',
    'Content-Type': 'application/json'
  },
  auth: jiraAuth,
  validateStatus: s => s >= 200 && s < 300
};

// Fields the enhanced endpoint must return for our logic
const REQUIRED_FIELDS = [
  'status',
  TRACKING_FIELD,
  PROMISED_DELIVERY_DATE_FIELD,
  LATEST_PDD_FIELD,
  DISPATCH_DATE_FIELD,
  DELIVERY_DATE_FIELD,
  RTO_DELIVERED_DATE_FIELD,
  RTO_REASON_FIELD,
  RTO_INITIATED_DATE_FIELD,
  OUT_FOR_DELIVERY_DATE_FIELD
].filter(Boolean);

// Enhanced Search (NEW): POST /rest/api/3/search/jql with nextPageToken pagination
async function jiraSearchJQL({ jql, nextPageToken = null, maxResults = 50 }) {
  const url = `${jiraBase()}/rest/api/3/search/jql`;
  const body = {
    jql,
    maxResults,
    nextPageToken: nextPageToken || undefined,
    fields: REQUIRED_FIELDS,
    fieldsByKeys: true
  };

  try {
    const { data } = await axios.post(url, body, jiraAxiosCfg);
    return data;
  } catch (e) {
    const status = e?.response?.status;
    const data = e?.response?.data;
    console.error('❌ Jira enhanced search failed', { status, data });
    throw e;
  }
}

// ---------- Extract AWB ----------
const extractAWB = (v) => {
  if (!v || typeof v !== 'string') return null;
  const s = v.trim();
  const patterns = [
    /(?:\bawb=|\bwaybill=)(\d{10,14})\b/i,
    /\/p\/(\d{10,14})\b/i,
    /\/package\/(\d{10,14})\b/i
  ];
  for (const re of patterns) {
    const m = s.match(re);
    if (m) return m[1];
  }
  const justDigits = s.match(/\b(\d{10,14})\b/);
  return justDigits ? justDigits[1] : null;
};

// ---------- Retry ----------
const retry = async (fn, retries = 3, delayMs = 1000) => {
  let errLast;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try { return await fn(); }
    catch (err) {
      errLast = err;
      console.warn(`⚠️ Attempt ${attempt} failed: ${err.message}`);
      if (attempt < retries) await sleep(delayMs);
    }
  }
  console.error(`❌ All ${retries} attempts failed`);
  if (errLast) console.error(errLast.response?.data || errLast.message);
  return null;
};

// ---------- Delhivery ----------
const getTracking = async (awb) => {
  return await retry(async () => {
    const res = await axios.get(`https://track.delhivery.com/api/v1/packages/json/?waybill=${awb}`, {
      headers: { Authorization: `Token ${DELHIVERY_TOKEN}` }
    });
    return res.data?.ShipmentData?.[0]?.Shipment || null;
  });
};

// ---------- Jira (comments, transitions, updates) ----------
const postCommentADF = async (issueKey, commentText) => {
  if (!commentText) return;
  const payload = {
    body: { type: 'doc', version: 1, content: [{ type: 'paragraph', content: [{ type: 'text', text: commentText }] }] }
  };
  try {
    await axios.post(`${jiraBase()}/rest/api/3/issue/${issueKey}/comment`, payload, {
      auth: jiraAuth
    });
    console.log(`💬 Comment added to ${issueKey}`);
  } catch (err) {
    console.error(`❌ Failed to add comment to ${issueKey}:`, err.response?.data || err.message);
  }
};

const findTransitionByName = (transitions, target) => {
  const targets = (JIRA_STATUS_ALIASES[target] || [target]).map(nameNorm);
  const exact = transitions.find(t => targets.includes(nameNorm(t.to?.name)));
  if (exact) return exact;

  // Fuzzy fallback: for RTO we accept any status containing both "rto" and "transit"
  const targetNorm = nameNorm(target);
  if (targetNorm.includes('rtointransit')) {
    const fuzzy = transitions.find(t => {
      const n = nameNorm(t.to?.name);
      return n.includes('rto') && n.includes('transit');
    });
    if (fuzzy) return fuzzy;
  }
  return null;
};

const updateJira = async (issueKey, newStatus, customFields = {}, comment = null) => {
  try {
    const transitionRes = await axios.get(`${jiraBase()}/rest/api/3/issue/${issueKey}/transitions`, {
      auth: jiraAuth
    });

    if (LOG_TRANSITIONS) {
      console.log(`🔎 Transitions for ${issueKey}:`, transitionRes.data.transitions.map(t => t.to?.name));
    }

    const transition = findTransitionByName(transitionRes.data.transitions, newStatus);

    if (!transition) {
      console.log(`⚠️ No matching transition for "${newStatus}" on ${issueKey}. Available: ${JSON.stringify(transitionRes.data.transitions.map(t => t.to?.name))}`);
      return; // bail early — don’t write fields/comments if we didn’t move
    }

    await axios.post(`${jiraBase()}/rest/api/3/issue/${issueKey}/transitions`, {
      transition: { id: transition.id }
    }, { auth: jiraAuth });

    console.log(`✅ Status updated to "${newStatus}" for ${issueKey}`);

    if (Object.keys(customFields).length > 0) {
      await axios.put(`${jiraBase()}/rest/api/3/issue/${issueKey}`, { fields: customFields }, {
        auth: jiraAuth
      });
      console.log(`🛠️ Custom fields updated for ${issueKey}`);
    }

    if (["DELIVERED", "RTO DELIVERED"].includes(newStatus)) {
      await axios.put(`${jiraBase()}/rest/api/3/issue/${issueKey}/assignee`, {
        accountId: POST_DELIVERY_ASSIGNEE
      }, { auth: jiraAuth });
      console.log(`👤 Assigned ${issueKey} to post-delivery assignee`);
    }

    if (comment) {
      await postCommentADF(issueKey, comment); // keep your original status comment
    }

  } catch (err) {
    console.error(`❌ Failed to update JIRA ${issueKey}:`, err.response?.data || err.message);
  }
};

// Fields-only updater (for when status is unchanged)
const updateJiraFieldsOnly = async (issueKey, fields) => {
  if (!fields || Object.keys(fields).length === 0) return;
  try {
    await axios.put(`${jiraBase()}/rest/api/3/issue/${issueKey}`, { fields }, {
      auth: jiraAuth
    });
    console.log(`📝 Fields updated for ${issueKey} (no transition)`);
  } catch (err) {
    console.error(`❌ Failed to update fields for ${issueKey}:`, err.response?.data || err.message);
  }
};

// ---------- Classification helpers ----------
const hasRecentRTScan = (tracking, lookback = 8) => {
  const scans = Array.isArray(tracking?.Scans) ? tracking.Scans : [];
  return scans.slice(-lookback).some(x => (x?.ScanDetail?.ScanType || '').toUpperCase() === 'RT');
};

const hasTerminalRTO = (t) => {
  const s = t?.Status || {};
  const scans = Array.isArray(t?.Scans) ? t.Scans : [];
  const instr = (s.Instructions || '').toLowerCase();

  if (t.ReturnedDate) return true;

  if ((s.StatusType || s.ScanType || '').toUpperCase() === 'DL' &&
      ((s.Status || '').toLowerCase().includes('rto') || instr.includes('return accepted'))) return true;

  return scans.some(x => {
    const sd = x?.ScanDetail || {};
    return (sd.ScanType || '').toUpperCase() === 'DL' &&
           ((sd.Scan || '').toLowerCase().includes('rto') ||
            (sd.Instructions || '').toLowerCase().includes('return accepted'));
  });
};

const isReturnFlow = (t) => {
  const s = t?.Status || {};
  const statusType = (s.StatusType || s.ScanType || '').toUpperCase();
  const statusTxt = (s.Status || '').toLowerCase();
  const instr = (s.Instructions || '').toLowerCase();
  return (
    ['RT','RTO','RET'].includes(statusType) ||
    !!t.ReverseInTransit ||
    !!t.RTOStartedDate ||
    hasRecentRTScan(t) ||
    statusTxt.includes('rto') ||
    instr.includes('rto')
  );
};

const interpretStatus = (t) => {
  const s = t?.Status || {};
  const status = (s.Status || '').trim();
  const instructions = (s.Instructions || '').toLowerCase();

  // 1) Terminal RTO first
  if (hasTerminalRTO(t)) return 'RTO DELIVERED';

  // 2) Return leg next (RTO in transit)
  if (isReturnFlow(t)) return 'RTO IN - TRANSIT';

  // 3) Forward terminal
  if (t.DeliveryDate) return 'DELIVERED';

  // 4) Heuristics (forward)
  if (instructions.includes('consignee will collect')) return 'IN - TRANSIT';
  if (instructions.includes('consignee to collect from branch')) return 'IN - TRANSIT';
  if (instructions.includes('shipment received at facility')) return 'IN - TRANSIT';
  if (instructions.includes('consignee unavailable')) return 'IN - TRANSIT';
  if (instructions.includes('agent remark incorrect')) return 'IN - TRANSIT';
  if (instructions.includes('arriving today')) return 'IN - TRANSIT';
  if (instructions.includes('office/institute closed')) return 'IN - TRANSIT';
  if (instructions.includes('agent remark verified')) return 'IN - TRANSIT';
  if (instructions.includes("reattempt as per client's instruction")) return 'IN - TRANSIT';
  if (instructions.includes("center changed by system")) return 'IN - TRANSIT';
  if (instructions.includes("reattempt - as per ndr instructions")) return 'IN - TRANSIT';
  if (instructions.includes("service disruption")) return 'IN - TRANSIT';
  if (instructions.includes("vehicle departed")) return 'IN - TRANSIT';
  if (instructions.includes('payment mode / amt dispute')) return 'IN - TRANSIT';

  // 5) Heuristics implying RTO (centralized)
  if (VERIFIED_CXL_RE.test(instructions)) return 'RTO IN - TRANSIT';
  if (instructions.includes('dispatched for rto')) return 'RTO IN - TRANSIT';
  if (instructions.includes('return accepted')) return 'RTO DELIVERED';
  if (instructions.includes('not attempted')) return 'NDR';
  if (instructions.includes('maximum attempts reached')) return 'IN - TRANSIT';
  if (instructions.includes('package missing in audit')) return 'IN - TRANSIT';
  if (instructions.includes('package found in audit')) return 'IN - TRANSIT';
  if (instructions.includes('delivery rescheduled by customer')) return 'IN - TRANSIT';
  if (instructions.includes('delayed due to weather conditions')) return 'IN - TRANSIT';
  if (instructions.includes('natural disaster')) return 'IN - TRANSIT';
  if (instructions.includes('ntd updated')) return 'RTO IN - TRANSIT';
  if (instructions.includes('returned as per security instruction')) return 'RTO IN - TRANSIT';
  if (instructions.includes('recipient unavailable.establishment closed')) return 'RTO IN - TRANSIT';

  // 6) Fallback map
  return STATUS_MAP[status] || null;
};

// ---------- Field updates ----------
const buildDateUpdates = (issue, t, updatedStatus) => {
  const out = {};
  const fmt = (d) => dayjs(d).format('YYYY-MM-DD');
  const cur = issue.fields || {};

  // Dispatch Date
  if (t.OriginRecieveDate) {
    const v = fmt(t.OriginRecieveDate);
    if (cur[DISPATCH_DATE_FIELD] !== v) out[DISPATCH_DATE_FIELD] = v;
  }

  // Delivery Date (forward only)
  if (t.DeliveryDate && updatedStatus === 'DELIVERED') {
    const v = fmt(t.DeliveryDate);
    if (cur[DELIVERY_DATE_FIELD] !== v) out[DELIVERY_DATE_FIELD] = v;
  }

  // RTO Delivered Date (return only)
  if (t.ReturnedDate && updatedStatus === 'RTO DELIVERED') {
    const v = fmt(t.ReturnedDate);
    if (cur[RTO_DELIVERED_DATE_FIELD] !== v) out[RTO_DELIVERED_DATE_FIELD] = v;
  }

  return out;
};

// ---------- Latest instruction helpers ----------
const getLatestInstruction = (t) => {
  if (!t) return null;

  // Prefer top-level Status.Instructions (usually latest)
  const statusIns = t.Status?.Instructions && String(t.Status.Instructions).trim();
  const statusWhen = t.Status?.StatusDateTime || t.DeliveryDate || t.DestRecieveDate || null;
  const statusWhere = t.Status?.StatusLocation || null;
  const statusCode = t.Status?.StatusCode || null;

  if (statusIns) {
    return { instruction: statusIns, when: statusWhen, where: statusWhere, code: statusCode };
  }

  // Fallback to most recent scan’s instruction/scan
  const scans = Array.isArray(t.Scans) ? t.Scans : [];
  const latest = scans
    .map(s => s?.ScanDetail || {})
    .filter(sd => sd && (sd.Instructions || sd.Scan))
    .sort((a, b) => new Date(b.ScanDateTime || 0) - new Date(a.ScanDateTime || 0))[0];

  if (!latest) return null;
  return {
    instruction: String(latest.Instructions || latest.Scan || '').trim(),
    when: latest.StatusDateTime || latest.ScanDateTime || null,
    where: latest.ScannedLocation || null,
    code: latest.StatusCode || null
  };
};

// Find the most relevant timestamp for an "Out for delivery" event
const getOFDWhen = (tracking, latestIns) => {
  // 1) If the *latest instruction* is OFD, use its timestamp
  if (latestIns && /out for delivery/i.test(latestIns.instruction || '') && latestIns.when) {
    return latestIns.when;
  }
  // 2) If top-level status itself is OFD, prefer StatusDateTime
  if (/out for delivery/i.test(String(tracking?.Status?.Instructions || tracking?.Status?.Status || ''))) {
    if (tracking?.Status?.StatusDateTime) return tracking.Status.StatusDateTime;
  }
  // 3) Search scans for the most recent OFD entry
  const scans = Array.isArray(tracking?.Scans) ? tracking.Scans : [];
  const ofdScan = scans
    .map(s => s?.ScanDetail || {})
    .filter(sd => /out for delivery/i.test(String(sd.Instructions || sd.Scan || '')))
    .sort((a, b) => new Date(b.ScanDateTime || 0) - new Date(a.ScanDateTime || 0))[0];
  if (ofdScan) return ofdScan.StatusDateTime || ofdScan.ScanDateTime;

  // 4) Fallback to generic status time if present
  return tracking?.Status?.StatusDateTime || null;
};

// Find earliest "verified cancellation" based on the trigger list
const findVerifiedCancellation = (t) => {
  const scans = Array.isArray(t?.Scans) ? t.Scans : [];
  const matches = scans
    .map(s => s?.ScanDetail || {})
    .filter(sd => VERIFIED_CXL_RE.test(String(sd.Instructions || '')))
    .sort((a, b) => new Date(a.StatusDateTime || a.ScanDateTime || 0) - new Date(b.StatusDateTime || b.ScanDateTime || 0));
  return matches[0] || null;
};

// ---------- Jira issue search (ENHANCED) ----------
const fetchAllIssues = async (jql) => {
  const all = [];
  const pageSize = 50; // Jira Cloud safe
  let nextPageToken = null;

  while (true) {
    const data = await jiraSearchJQL({
      jql,
      nextPageToken,
      maxResults: pageSize
    });

    const issues = data?.issues || [];
    all.push(...issues);

    // Enhanced pagination: stop when isLast or no token returned.
    if (data?.isLast || !data?.nextPageToken || issues.length === 0) break;
    nextPageToken = data.nextPageToken;
  }
  return all;
};

// ---------- JQL builders using cf[ID] for the tracking field ----------
function buildTrackingCfExpr() {
  const cfExpr = toCfIdExpr(TRACKING_FIELD);
  return cfExpr || `"Shipping Tracking Details"`; // fallback to display name if env missing
}

function buildJqlPickup(sinceYmd) {
  const cfExpr = buildTrackingCfExpr();
  const cond = [
    `project = ${JIRA_PROJECT}`,
    `${cfExpr} IS NOT EMPTY`,
    `created >= ${sinceYmd}`,
    `status = "PICKUP SCHEDULED"`
  ].join(' AND ');
  return `${cond} ORDER BY updated DESC`;
}

function buildJqlOthers(sinceYmd) {
  const cfExpr = buildTrackingCfExpr();
  const cond = [
    `project = ${JIRA_PROJECT}`,
    `${cfExpr} IS NOT EMPTY`,
    `created >= ${sinceYmd}`,
    `status NOT IN ("RTO DELIVERED","PICKUP SCHEDULED")`
  ].join(' AND ');
  return `${cond} ORDER BY updated DESC`;
}

// ---------- Issues ----------
const getJiraIssues = async () => {
  const since = dayjs().subtract(CREATED_SINCE_DAYS, 'day').format('YYYY-MM-DD');

  if (DEBUG_ISSUE_KEY) {
    const single = await fetchAllIssues(`key = ${DEBUG_ISSUE_KEY}`);
    return single;
  }

  console.log('📥 Fetching PICKUP SCHEDULED issues...');
  const pickupIssues = await fetchAllIssues(buildJqlPickup(since));

  console.log('📥 Fetching other eligible issues...');
  const otherIssues = await fetchAllIssues(buildJqlOthers(since));

  return [...pickupIssues, ...otherIssues];
};

// ---------- Main ----------
const run = async () => {
  console.log(`🔄 Sync started at ${new Date().toISOString()}`);
  const issues = await getJiraIssues();
  if (!issues || issues.length === 0) {
    console.log('ℹ️ No issues found for the current window.');
    return;
  }

  let updated = 0, skipped = 0;

  for (const issue of issues) {
    try {
      const trackingFieldValue = issue.fields[TRACKING_FIELD];
      let awb = extractAWB(trackingFieldValue);
      if (DEBUG_AWB && awb !== DEBUG_AWB) continue;

      const currentStatus = issue.fields.status?.name || '';

      if (!awb) {
        console.log(`⚠️ No valid AWB for ${issue.key}`);
        continue;
      }

      const tracking = await getTracking(awb);
      if (!tracking) {
        console.log(`⚠️ No tracking payload for AWB ${awb} (${issue.key})`);
        continue;
      }

      // Classify
      const updatedStatus = interpretStatus(tracking);
      console.log(
        `[decision] ${issue.key} awb=${awb} cur="${currentStatus}" -> new="${updatedStatus}" ` +
        `type=${(tracking.Status?.StatusType||tracking.Status?.ScanType)||""} ` +
        `reverse=${!!tracking.ReverseInTransit} rtoStart=${!!tracking.RTOStartedDate} hasRTScan=${hasRecentRTScan(tracking)}`
      );

      if (!updatedStatus) {
        console.log(`⚠️ Unknown status "${tracking.Status?.Status}" for AWB ${awb}`);
        continue;
      }

      // Always prepare possible field updates (dates + latest instruction) BEFORE the skip check
      let customFields = buildDateUpdates(issue, tracking, updatedStatus);

      // === Promised Delivery Date (one-time, forward) ===
      const existingPDD = issue.fields?.[PROMISED_DELIVERY_DATE_FIELD];
      if (!existingPDD) {
        const rawPDD = tracking?.PromisedDeliveryDate; // e.g., "2025-09-14T23:59:59"
        if (rawPDD) {
          const pdd = dayjs(rawPDD).isValid() ? dayjs(rawPDD).format('YYYY-MM-DD') : null;
          if (pdd) {
            customFields[PROMISED_DELIVERY_DATE_FIELD] = pdd;
            console.log(`🗓️ Promised Delivery Date (forward) prepared for ${issue.key}: ${pdd}`);
          }
        }
      } else {
        console.log(`🗓️ Promised Delivery Date already set for ${issue.key} (${existingPDD}); not overwriting.`);
      }

      // === Latest PDD (overwrite allowed, forward) ===
      const rawLatestPDD = tracking?.ExpectedDeliveryDate || tracking?.PromisedDeliveryDate || null;
      if (rawLatestPDD) {
        const newPdd = dayjs(rawLatestPDD).isValid() ? dayjs(rawLatestPDD).format('YYYY-MM-DD') : null;
        const currentPdd = issue.fields?.[LATEST_PDD_FIELD] || null;
        if (newPdd && newPdd !== currentPdd) {
          customFields[LATEST_PDD_FIELD] = newPdd;
          if (currentPdd) {
            console.log(`🗓️ Latest PDD updated for ${issue.key}: ${currentPdd} -> ${newPdd}`);
          } else {
            console.log(`🗓️ Latest PDD set for ${issue.key}: ${newPdd}`);
          }
        } else if (newPdd && newPdd === currentPdd) {
          console.log(`🗓️ Latest PDD unchanged for ${issue.key}: ${currentPdd}`);
        }
      } else {
        console.log(`🗓️ No forward PDD present in payload for ${issue.key}; skipping Latest PDD.`);
      }

      // === RTO Reason & Initiated Date (write-once) ===
      const cancelEvent = findVerifiedCancellation(tracking);
      if (cancelEvent) {
        const reasonText = String(cancelEvent.Instructions || '').trim();
        const when = cancelEvent.StatusDateTime || cancelEvent.ScanDateTime;
        const dateYmd = when ? dayjs(when).format('YYYY-MM-DD') : null;

        const currentReason = issue.fields?.[RTO_REASON_FIELD];
        const currentRtoDate = issue.fields?.[RTO_INITIATED_DATE_FIELD];

        if (!currentReason && reasonText) {
          customFields[RTO_REASON_FIELD] = reasonText;
          console.log(`🏷️ RTO Reason (write-once) set for ${issue.key}: ${reasonText}`);
        } else if (currentReason) {
          console.log(`🏷️ RTO Reason already set for ${issue.key} (${currentReason}); not overwriting.`);
        }

        if (!currentRtoDate && dateYmd) {
          customFields[RTO_INITIATED_DATE_FIELD] = dateYmd;
          console.log(`📅 RTO Initiated Date (write-once) set for ${issue.key}: ${dateYmd}`);
        } else if (currentRtoDate) {
          console.log(`📅 RTO Initiated Date already set for ${issue.key} (${currentRtoDate}); not overwriting.`);
        }
      } else {
        console.log(`ℹ️ No verified cancellation match found for ${issue.key}`);
      }

      // Grab the most recent instruction (prefers top-level Status.Instructions)
      const latestIns = getLatestInstruction(tracking);
      if (latestIns) {
        const instrOnly = latestIns.instruction || '';
        const currentInstr = issue.fields?.[LATEST_INSTRUCTION_FIELD] || '';
        if (instrOnly && currentInstr !== instrOnly) {
          customFields[LATEST_INSTRUCTION_FIELD] = instrOnly;
          console.log(`ℹ️ Latest instruction (plain) prepared for ${issue.key}: ${instrOnly}`);
        } else if (instrOnly) {
          console.log(`ℹ️ Instruction unchanged for ${issue.key}: ${instrOnly}`);
        } else {
          console.log(`ℹ️ Instruction computed empty for ${issue.key}`);
        }

        // === Out for Delivery Date (one-time) — from INSTRUCTION
        const existingOFD = issue.fields?.[OUT_FOR_DELIVERY_DATE_FIELD];
        if (!existingOFD && /out for delivery/i.test(instrOnly)) {
          const whenFromInstr = getOFDWhen(tracking, latestIns);
          if (whenFromInstr) {
            const ofdDate = dayjs(whenFromInstr).format('YYYY-MM-DD');
            if (ofdDate) {
              customFields[OUT_FOR_DELIVERY_DATE_FIELD] = ofdDate;
              console.log(`🚚 Out-for-delivery date (write-once) set from INSTRUCTION for ${issue.key}: ${ofdDate}`);
            }
          }
        } else if (existingOFD) {
          console.log(`🚚 Out-for-delivery date already set for ${issue.key} (${existingOFD}); not overwriting.`);
        }
      } else {
        console.log(`ℹ️ No instruction found in payload for ${issue.key}`);
      }

      // === Out for Delivery Date (one-time) — from STATUS fallback
      const existingOFD2 = issue.fields?.[OUT_FOR_DELIVERY_DATE_FIELD];
      if (!existingOFD2 && updatedStatus === 'OUT FOR DELIVERY') {
        const whenFromStatus = getOFDWhen(tracking, latestIns);
        if (whenFromStatus) {
          const ofdDate = dayjs(whenFromStatus).format('YYYY-MM-DD');
          if (ofdDate) {
            customFields[OUT_FOR_DELIVERY_DATE_FIELD] = ofdDate;
            console.log(`🚚 Out-for-delivery date (write-once) set from STATUS for ${issue.key}: ${ofdDate}`);
          }
        }
      } else if (existingOFD2) {
        console.log(`🚚 Out-for-delivery date already set for ${issue.key} (${existingOFD2}); not overwriting.`);
      }

      // If status is unchanged, only push field updates (if any), then skip transition
      if (currentStatus && nameNorm(currentStatus) === nameNorm(updatedStatus)) {
        if (Object.keys(customFields).length > 0) {
          await updateJiraFieldsOnly(issue.key, customFields);
        }
        console.log(`⏩ Skipping transition for ${issue.key} — already "${updatedStatus}"`);
        skipped++;
        continue;
      }

      // Comment (unchanged)
      let comment = null;
      switch (updatedStatus) {
        case 'IN - TRANSIT': comment = `Order is now in transit as of ${new Date().toISOString()}`; break;
        case 'NDR': comment = `Order marked as NDR (Non-Delivery Report) as of ${new Date().toISOString()}`; break;
        case 'RTO IN - TRANSIT':
          comment = `Order is now RTO in transit as of ${new Date().toISOString()} (Signals: StatusType=${(tracking.Status?.StatusType||tracking.Status?.ScanType)||"?"}, ReverseInTransit=${!!tracking.ReverseInTransit}, RTOStartedDate=${tracking.RTOStartedDate || "N/A"}, hasRTScan=${hasRecentRTScan(tracking)})`;
          break;
        case 'RTO DELIVERED': comment = `Order RTO delivered as of ${new Date().toISOString()}`; break;
        case 'DELIVERED': comment = `Order successfully delivered on ${new Date().toISOString()}`; break;
        case 'OUT FOR DELIVERY': comment = `Order is out for delivery as of ${new Date().toISOString()}`; break;
      }

      await updateJira(issue.key, updatedStatus, customFields, comment);
      updated++;

      await sleep(SLEEP_MS);

    } catch (err) {
      console.error(`💥 Error handling ${issue.key}:`, err.response?.data || err.message);
    }
  }

  console.log(`✅ Sync finished at ${new Date().toISOString()}`);
  console.log(`📊 Summary: ${updated} updated, ${skipped} skipped`);
};

process.on('unhandledRejection', (reason) => {
  console.error('💥 Unhandled Rejection:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('💥 Uncaught Exception:', err);
});

run().catch(err => {
  console.error('💥 Script failed:', err);
  process.exit(1);
});
