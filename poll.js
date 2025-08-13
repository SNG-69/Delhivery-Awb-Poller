require('dotenv').config();
const axios = require('axios');
const dayjs = require('dayjs');

// ---------- Config ----------
axios.defaults.timeout = 20000;
axios.defaults.headers.common['User-Agent'] = 'instasport-delhivery-jira-sync/1.1';

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
  'NDR': 'NDR - 3',
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

// ---------- Utils ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const nameNorm = (s) => (s || '').toLowerCase().replace(/[\s_\-]+/g, '');

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

// ---------- Jira ----------
const fetchAllIssues = async (jql) => {
  const all = [];
  let startAt = 0;
  const pageSize = 50; // safe for Jira Cloud

  while (true) {
    const { data } = await axios.get(`${JIRA_DOMAIN}/rest/api/3/search`, {
      params: { jql, startAt, maxResults: pageSize },
      auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN }
    });

    const issues = data.issues || [];
    all.push(...issues);

    const nextStart = (data.startAt || 0) + issues.length;
    const total = data.total ?? nextStart;

    if (issues.length === 0 || nextStart >= total) break;
    startAt = nextStart;
  }
  return all;
};

const postCommentADF = async (issueKey, commentText) => {
  if (!commentText) return;
  const payload = {
    body: { type: 'doc', version: 1, content: [{ type: 'paragraph', content: [{ type: 'text', text: commentText }] }] }
  };
  try {
    await axios.post(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}/comment`, payload, {
      auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN }
    });
    console.log(`💬 Comment added to ${issueKey}`);
  } catch (err) {
    console.error(`❌ Failed to add comment to ${issueKey}:`, err.response?.data || err.message);
  }
};

const updateJira = async (issueKey, newStatus, customFields = {}, comment = null) => {
  try {
    const transitionRes = await axios.get(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}/transitions`, {
      auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN }
    });

    if (LOG_TRANSITIONS) {
      console.log(`🔎 Transitions for ${issueKey}:`, transitionRes.data.transitions.map(t => t.to?.name));
    }

    const transition = findTransitionByName(transitionRes.data.transitions, newStatus);

    if (!transition) {
      console.log(`⚠️ No matching transition for "${newStatus}" on ${issueKey}. Available: ${JSON.stringify(transitionRes.data.transitions.map(t => t.to?.name))}`);
      return; // bail early — don’t write fields/comments if we didn’t move
    }

    await axios.post(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}/transitions`, {
      transition: { id: transition.id }
    }, { auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN } });

    console.log(`✅ Status updated to "${newStatus}" for ${issueKey}`);

    if (Object.keys(customFields).length > 0) {
      await axios.put(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}`, { fields: customFields }, {
        auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN }
      });
      console.log(`🛠️ Custom fields updated for ${issueKey}`);
    }

    if (comment) await postCommentADF(issueKey, comment);

    if (["DELIVERED", "RTO DELIVERED"].includes(newStatus)) {
      await axios.put(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}/assignee`, {
        accountId: POST_DELIVERY_ASSIGNEE
      }, { auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN } });
      console.log(`👤 Assigned ${issueKey} to post-delivery assignee`);
    }

  } catch (err) {
    console.error(`❌ Failed to update JIRA ${issueKey}:`, err.response?.data || err.message);
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

  // ReturnedDate beats everything
  if (t.ReturnedDate) return true;

  // Terminal RTO style scan on the status
  if ((s.StatusType || s.ScanType || '').toUpperCase() === 'DL' &&
      ( (s.Status || '').toLowerCase().includes('rto') || instr.includes('return accepted') )) return true;

  // Any DL scan that looks like RTO complete
  return scans.some(x => {
    const sd = x?.ScanDetail || {};
    return (sd.ScanType || '').toUpperCase() === 'DL' &&
           ( (sd.Scan || '').toLowerCase().includes('rto') ||
             (sd.Instructions || '').toLowerCase().includes('return accepted') );
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
  if (instructions.includes('shipment received at facility')) return 'IN - TRANSIT';
  if (instructions.includes('consignee unavailable')) return 'IN - TRANSIT';
  if (instructions.includes('agent remark incorrect')) return 'IN - TRANSIT';
  if (instructions.includes('arriving today')) return 'IN - TRANSIT';
  if (instructions.includes('office/institute closed')) return 'IN - TRANSIT';

  // 5) Heuristics implying RTO
  if (instructions.includes('whatsapp verified cancellation')) return 'RTO IN - TRANSIT';
  if (instructions.includes('code verified cancellation')) return 'RTO IN - TRANSIT';
  if (instructions.includes('dispatched for rto')) return 'RTO IN - TRANSIT';
  if (instructions.includes('return accepted')) return 'RTO DELIVERED';

  if (instructions.includes('not attempted')) return 'NDR - 3';
  if (instructions.includes('maximum attempts reached')) return 'IN - TRANSIT';

  // 6) Fallback map
  return STATUS_MAP[status] || null;
};

// ---------- Field updates ----------
const buildDateUpdates = (issue, t, updatedStatus) => {
  const out = {};
  const fmt = (d) => dayjs(d).format('YYYY-MM-DD');
  const cur = issue.fields || {};

  if (t.OriginRecieveDate) {
    const v = fmt(t.OriginRecieveDate);
    if (cur[DISPATCH_DATE_FIELD] !== v) out[DISPATCH_DATE_FIELD] = v;
  }

  // Only write Delivery Date for forward deliveries
  if (t.DeliveryDate && updatedStatus === 'DELIVERED') {
    const v = fmt(t.DeliveryDate);
    if (cur[DELIVERY_DATE_FIELD] !== v) out[DELIVERY_DATE_FIELD] = v;
  }

  if (t.ReturnedDate && updatedStatus === 'RTO DELIVERED') {
    const v = fmt(t.ReturnedDate);
    if (cur[RTO_DELIVERED_DATE_FIELD] !== v) out[RTO_DELIVERED_DATE_FIELD] = v;
  }

  return out;
};

// ---------- Issues ----------
const getJiraIssues = async () => {
  const since = dayjs().subtract(CREATED_SINCE_DAYS, 'day').format('YYYY-MM-DD');

  // Include Delivered in "others" so we can correct wrong forwards → RTO
  const jqlPickup = `project = ${JIRA_PROJECT} AND "Shipping Tracking Details" IS NOT EMPTY AND created >= "${since}" AND status = "PICKUP SCHEDULED" ORDER BY updated DESC`;
  const jqlOthers = `project = ${JIRA_PROJECT} AND "Shipping Tracking Details" IS NOT EMPTY AND created >= "${since}" AND status NOT IN ("RTO DELIVERED","PICKUP SCHEDULED") ORDER BY updated DESC`;

  if (DEBUG_ISSUE_KEY) {
    const single = await fetchAllIssues(`key = ${DEBUG_ISSUE_KEY}`);
    return single;
  }

  console.log('📥 Fetching PICKUP SCHEDULED issues...');
  const pickupIssues = await fetchAllIssues(jqlPickup);

  console.log('📥 Fetching other eligible issues...');
  const otherIssues = await fetchAllIssues(jqlOthers);

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

      console.log(`[decision] ${issue.key} awb=${awb} cur="${currentStatus}" -> new="${updatedStatus}" type=${(tracking.Status?.StatusType||tracking.Status?.ScanType)||""} reverse=${!!tracking.ReverseInTransit} rtoStart=${!!tracking.RTOStartedDate} hasRTScan=${hasRecentRTScan(tracking)}`);

      if (!updatedStatus) {
        console.log(`⚠️ Unknown status "${tracking.Status?.Status}" for AWB ${awb}`);
        continue;
      }

      if (currentStatus && nameNorm(currentStatus) === nameNorm(updatedStatus)) {
        console.log(`⏩ Skipping ${issue.key} — already "${updatedStatus}"`);
        skipped++;
        continue;
      }

      // Date fields (only changed ones)
      const customFields = buildDateUpdates(issue, tracking, updatedStatus);

      // Comment
      let comment = null;
      switch (updatedStatus) {
        case 'IN - TRANSIT': comment = `Order is now in transit as of ${new Date().toISOString()}`; break;
        case 'NDR - 3': comment = `Order marked as NDR (Non-Delivery Report) as of ${new Date().toISOString()}`; break;
        case 'RTO IN - TRANSIT':
          comment = `Order is now RTO in transit as of ${new Date().toISOString()} (Signals: StatusType=${(tracking.Status?.StatusType||tracking.Status?.ScanType)||"?"}, ReverseInTransit=${!!tracking.ReverseInTransit}, RTOStartedDate=${tracking.RTOStartedDate || "N/A"}, hasRTScan=${hasRecentRTScan(tracking)})`;
          break;
        case 'RTO DELIVERED': comment = `Order RTO delivered as of ${new Date().toISOString()}`; break;
        case 'DELIVERED': comment = `Order successfully delivered on ${new Date().toISOString()}`; break;
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
