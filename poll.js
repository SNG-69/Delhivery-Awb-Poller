require('dotenv').config();
const axios = require('axios');
const dayjs = require('dayjs');

// =============================
// Config & constants
// =============================
const DELHIVERY_TOKEN = process.env.DELHIVERY_TOKEN;
const JIRA_DOMAIN = process.env.JIRA_DOMAIN;                 // e.g. https://yourdomain.atlassian.net
const JIRA_EMAIL = process.env.JIRA_EMAIL;
const JIRA_API_TOKEN = process.env.JIRA_API_TOKEN;
const JIRA_PROJECT = process.env.JIRA_PROJECT;               // e.g. INST

const TRACKING_FIELD = process.env.TRACKING_FIELD;           // e.g. customfield_12345 (text)
const DISPATCH_DATE_FIELD = process.env.CUSTOMFIELD_DISPATCH_DATE; // date
const DELIVERY_DATE_FIELD = process.env.CUSTOMFIELD_DELIVERY_DATE; // date
const RTO_DELIVERED_DATE_FIELD = process.env.CUSTOMFIELD_RTO_DATE; // date
const POST_DELIVERY_ASSIGNEE = process.env.POST_DELIVERY_ASSIGNEE || '712020:d710d4e8-270f-4d7a-b65a-7303f71783fb';

const CREATED_SINCE_DAYS = Number(process.env.CREATED_SINCE_DAYS || 60);
const DEBUG_ISSUE_KEY = process.env.DEBUG_ISSUE_KEY || '';   // e.g. INST-123 to isolate one issue
const LOG_TRANSITIONS = process.env.LOG_TRANSITIONS === '1';  // set to 1 to log available transitions for each issue

const STATUS_MAP = {
  'Ready for pickup': 'PICKUP SCHEDULED',
  'Manifested': 'PICKUP SCHEDULED',
  'Pending': 'PICKUP SCHEDULED',
  'Not Picked': 'PICKUP EXCEPTION - DELHIVERY',
  'Dispatched': 'IN - TRANSIT',
  'In Transit': 'IN - TRANSIT',
  'Out for delivery': 'OUT FOR DELIVERY',
  'Delivered': 'DELIVERED',
  'Delayed': 'IN - TRANSIT',
  'DELAYED': 'IN - TRANSIT',
  'On Time': 'IN - TRANSIT',
  'RTO': 'RTO IN - TRANSIT',
  'RTO - In Transit': 'RTO IN - TRANSIT',
  'In Transit For Return': 'RTO IN - TRANSIT',
  'RTO - Returned': 'RTO DELIVERED',
  'Cancelled': 'PICKUP EXCEPTION - DELHIVERY',
  'Shipment delivery cancelled via OTP': 'PICKUP EXCEPTION - DELHIVERY',
  'NDR': 'NDR - 3'
};

// =============================
// Axios instance
// =============================
const http = axios.create({
  headers: {
    'User-Agent': 'instasport-delhivery-jira-sync/1.3'
  },
  timeout: 20000
});

// =============================
// Utilities
// =============================
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const retry = async (fn, retries = 3, delayMs = 1000) => {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try { return await fn(); }
    catch (err) {
      console.warn(`⚠️ Attempt ${attempt} failed: ${err.message}`);
      if (attempt < retries) await sleep(delayMs);
    }
  }
  console.error(`❌ All ${retries} attempts failed`);
  return null;
};

// Extract AWB from a freeform tracking field
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

// =============================
// Delhivery API
// =============================
const getTracking = async (awb) => {
  return await retry(async () => {
    const res = await http.get(`https://track.delhivery.com/api/v1/packages/json/?waybill=${awb}`,
      { headers: { Authorization: `Token ${DELHIVERY_TOKEN}` } }
    );
    return res.data?.ShipmentData?.[0]?.Shipment || null;
  });
};

// =============================
// Jira helpers
// =============================
// Normalize status names by removing non-alphanumerics and lowercasing
const norm = (s) => (s || '').toLowerCase().replace(/[^a-z0-9]/g, '');

const JIRA_STATUS_ALIASES = {
  'RTO IN - TRANSIT': [
    'RTO IN - TRANSIT', 'RTO IN-TRANSIT', 'RTO IN TRANSIT',
    'RTO - IN TRANSIT', 'RTO - In Transit', 'RTO- IN TRANSIT',
    'Return In-Transit', 'Return In Transit', 'Return InTransit'
  ],
  'IN - TRANSIT': ['IN - TRANSIT', 'IN-TRANSIT', 'IN TRANSIT'],
  'PICKUP SCHEDULED': ['PICKUP SCHEDULED'],
  'OUT FOR DELIVERY': ['OUT FOR DELIVERY'],
  'DELIVERED': ['DELIVERED'],
  'RTO DELIVERED': ['RTO DELIVERED', 'RETURN DELIVERED']
};

const targetsFor = (target) => (JIRA_STATUS_ALIASES[target] || [target]).map(norm);

const findTransitionCandidates = (transitions, target) => {
  const wanted = new Set(targetsFor(target));
  return transitions.filter(t => wanted.has(norm(t.to?.name)));
};

const fetchAllIssues = async (jql) => {
  const all = [];
  let startAt = 0;
  const maxResults = 500;

  while (true) {
    const res = await retry(async () => {
      const response = await http.get(`${JIRA_DOMAIN}/rest/api/3/search`, {
        params: { jql, startAt, maxResults },
        auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN }
      });
      return response.data;
    });

    if (!res || !Array.isArray(res.issues)) break; // guard on retry==null

    all.push(...res.issues);
    if (res.issues.length < maxResults) break;
    startAt += maxResults;
  }

  return all;
};

const postCommentADF = async (issueKey, commentText) => {
  if (!commentText) return;
  const payload = {
    body: { type: 'doc', version: 1, content: [{ type: 'paragraph', content: [{ type: 'text', text: commentText }] }] }
  };
  try {
    await http.post(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}/comment`, payload, {
      auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN }
    });
    console.log(`💬 Comment added to ${issueKey}`);
  } catch (err) {
    console.error(`❌ Failed to add comment to ${issueKey}:`, err.response?.data || err.message);
  }
};

const tryTransition = async (issueKey, newStatus) => {
  const res = await http.get(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}/transitions`, {
    auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN }
  });
  const transitions = res.data.transitions || [];
  if (LOG_TRANSITIONS) {
    console.log(`🔎 Transitions for ${issueKey}:`, transitions.map(t => t.to?.name));
  }
  const candidates = findTransitionCandidates(transitions, newStatus);
  if (candidates.length === 0) {
    console.log(`⚠️ No matching transition for "${newStatus}" on ${issueKey}. Available: ${JSON.stringify(transitions.map(t => t.to?.name))}`);
    return { ok: false };
  }
  // try first candidate
  const chosen = candidates[0];
  await http.post(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}/transitions`, { transition: { id: chosen.id } }, {
    auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN }
  });
  return { ok: true };
};

const updateJira = async (issueKey, newStatus, customFields = {}, comment = null) => {
  try {
    const { ok } = await tryTransition(issueKey, newStatus);
    if (!ok) return; // bail — don’t write fields/comments if we didn’t actually move

    console.log(`✅ Status updated to "${newStatus}" for ${issueKey}`);

    if (Object.keys(customFields).length > 0) {
      await http.put(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}`, { fields: customFields }, {
        auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN }
      });
      console.log(`🛠️ Custom fields updated for ${issueKey}`);
    }

    if (comment) await postCommentADF(issueKey, comment);

    if (["DELIVERED", "RTO DELIVERED"].includes(newStatus)) {
      await http.put(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}/assignee`, { accountId: POST_DELIVERY_ASSIGNEE }, {
        auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN }
      });
      console.log(`👤 Assigned ${issueKey} to post-delivery assignee`);
    }
  } catch (err) {
    console.error(`❌ Failed to update JIRA ${issueKey}:`, err.response?.data || err.message);
  }
};

// =============================
// Status interpretation (RTO-first)
// =============================
const interpretStatus = (tracking) => {
  const s = tracking?.Status || {};
  const status = (s.Status || '').trim();                        // e.g., "In Transit"
  const instructions = (s.Instructions || '').toLowerCase();     // normalized
  const statusType = (s.StatusType || s.ScanType || '').toUpperCase(); // e.g., "RT"

  const scans = Array.isArray(tracking.Scans) ? tracking.Scans : [];
  const hasRecentRTScan = scans.slice(-8).some(x => (x?.ScanDetail?.ScanType || '').toUpperCase() === 'RT');

  // 0) Terminal RTO first
  if (tracking.ReturnedDate) return 'RTO DELIVERED';

  // 1) Return leg wins over everything else
  const isReturnFlow =
    ['RT', 'RTO', 'RET'].includes(statusType) ||
    !!tracking.ReverseInTransit ||
    !!tracking.RTOStartedDate ||
    hasRecentRTScan ||
    status.toLowerCase().includes('rto') ||
    instructions.includes('rto');

  if (isReturnFlow) return 'RTO IN - TRANSIT';

  // 2) Forward final state next
  if (tracking.DeliveryDate) return 'DELIVERED';

  // 3) Heuristics (all lowercase)
  if (instructions.includes('consignee will collect')) return 'IN - TRANSIT';
  if (instructions.includes('shipment received at facility')) return 'IN - TRANSIT';
  if (instructions.includes('consignee unavailable')) return 'IN - TRANSIT';
  if (instructions.includes('agent remark incorrect')) return 'IN - TRANSIT';
  if (instructions.includes('arriving today')) return 'IN - TRANSIT';
  if (instructions.includes('office/institute closed')) return 'IN - TRANSIT';

  if (instructions.includes('whatsapp verified cancellation')) return 'RTO IN - TRANSIT';
  if (instructions.includes('code verified cancellation')) return 'RTO IN - TRANSIT';
  if (instructions.includes('dispatched for rto')) return 'RTO IN - TRANSIT';
  if (instructions.includes('return accepted')) return 'RTO DELIVERED';

  if (instructions.includes('not attempted')) return 'NDR - 3';
  if (instructions.includes('maximum attempts reached')) return 'IN - TRANSIT';

  // 4) Fallback explicit map
  return STATUS_MAP[status] || null;
};

// Only write changed dates
const buildDateUpdates = (issue, t) => {
  const out = {};
  const fmt = (d) => dayjs(d).format('YYYY-MM-DD');
  const current = issue.fields || {};

  if (t.OriginRecieveDate) {
    const v = fmt(t.OriginRecieveDate);
    if (current[DISPATCH_DATE_FIELD] !== v) out[DISPATCH_DATE_FIELD] = v;
  }
  if (t.DeliveryDate) {
    const v = fmt(t.DeliveryDate);
    if (current[DELIVERY_DATE_FIELD] !== v) out[DELIVERY_DATE_FIELD] = v;
  }
  if (t.ReturnedDate) {
    const v = fmt(t.ReturnedDate);
    if (current[RTO_DELIVERED_DATE_FIELD] !== v) out[RTO_DELIVERED_DATE_FIELD] = v;
  }
  return out;
};

// =============================
// Issue selection
// =============================
const getJiraIssues = async () => {
  if (DEBUG_ISSUE_KEY) {
    const jql = `key = ${DEBUG_ISSUE_KEY}`;
    console.log('📥 Debug mode — fetching single issue:', DEBUG_ISSUE_KEY);
    return await fetchAllIssues(jql);
  }

  const since = dayjs().subtract(CREATED_SINCE_DAYS, 'day').format('YYYY-MM-DD');
  // Include DELIVERED so we can flip to RTO if needed; exclude only final RTO and pickup scheduled
  const jqlPickup = `project = ${JIRA_PROJECT} AND "Shipping Tracking Details" IS NOT EMPTY AND created >= "${since}" AND status = "PICKUP SCHEDULED" ORDER BY updated DESC`;
  const jqlOthers = `project = ${JIRA_PROJECT} AND "Shipping Tracking Details" IS NOT EMPTY AND created >= "${since}" AND status NOT IN ("RTO DELIVERED", "PICKUP SCHEDULED") ORDER BY updated DESC`;

  console.log('📥 Fetching PICKUP SCHEDULED issues...');
  const pickupIssues = await fetchAllIssues(jqlPickup);

  console.log('📥 Fetching other eligible issues...');
  const otherIssues = await fetchAllIssues(jqlOthers);

  return [...pickupIssues, ...otherIssues];
};

// =============================
// Main
// =============================
const run = async () => {
  console.log(`🔄 Sync started at ${new Date().toISOString()}`);
  const issues = await getJiraIssues();
  if (!issues || issues.length === 0) {
    console.log('ℹ️ No issues to process.');
    return;
  }

  let updated = 0;
  let skipped = 0;

  for (const issue of issues) {
    try {
      const trackingFieldValue = issue.fields?.[TRACKING_FIELD];
      const awb = extractAWB(trackingFieldValue);
      const currentStatus = issue.fields?.status?.name || '';

      if (!awb) {
        console.log(`⚠️ No valid AWB for ${issue.key}`);
        continue;
      }

      const tracking = await getTracking(awb);
      if (!tracking) {
        console.log(`⚠️ No tracking for AWB ${awb} (${issue.key})`);
        continue;
      }

      const updatedStatus = interpretStatus(tracking);
      console.log(`[decision] ${issue.key} awb=${awb} cur="${currentStatus}" -> new="${updatedStatus}" type=${(tracking.Status?.StatusType || tracking.Status?.ScanType) || ''} reverse=${!!tracking.ReverseInTransit} rtoStart=${!!tracking.RTOStartedDate}`);

      if (!updatedStatus) {
        console.log(`⚠️ Unknown status "${tracking.Status?.Status}" for AWB ${awb}`);
        continue;
      }

      if (currentStatus === updatedStatus) {
        console.log(`⏩ Skipping ${issue.key} — already "${updatedStatus}"`);
        skipped++;
        continue;
      }

      const customFields = buildDateUpdates(issue, tracking);

      let comment = null;
      switch (updatedStatus) {
        case 'IN - TRANSIT':
          comment = `Order is now in transit as of ${new Date().toISOString()}`;
          break;
        case 'OUT FOR DELIVERY':
          comment = `Order is out for delivery as of ${new Date().toISOString()}`;
          break;
        case 'NDR - 3':
          comment = `Order marked as NDR (Non-Delivery Report) as of ${new Date().toISOString()}`;
          break;
        case 'RTO IN - TRANSIT':
          comment = `Order is now RTO in transit as of ${new Date().toISOString()} (Reason: StatusType=${(tracking.Status?.StatusType || tracking.Status?.ScanType) || '?'}, ReverseInTransit=${!!tracking.ReverseInTransit}, RTOStartedDate=${tracking.RTOStartedDate || 'N/A'})`;
          break;
        case 'RTO DELIVERED':
          comment = `Order RTO delivered as of ${new Date().toISOString()}`;
          break;
        case 'DELIVERED':
          comment = `Order successfully delivered on ${new Date().toISOString()}`;
          break;
      }

      await updateJira(issue.key, updatedStatus, customFields, comment);
      updated++;

      // small pacing to avoid 429s
      await sleep(200);
    } catch (err) {
      console.error(`❌ Error processing ${issue?.key}:`, err.message);
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