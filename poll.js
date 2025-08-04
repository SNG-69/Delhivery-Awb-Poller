require('dotenv').config();
const axios = require('axios');
const dayjs = require('dayjs');

// --- Environment Variables ---
const DELHIVERY_TOKEN = process.env.DELHIVERY_TOKEN;
const JIRA_DOMAIN = process.env.JIRA_DOMAIN;
const JIRA_EMAIL = process.env.JIRA_EMAIL;
const JIRA_API_TOKEN = process.env.JIRA_API_TOKEN;
const JIRA_PROJECT = process.env.JIRA_PROJECT;

const TRACKING_FIELD = process.env.TRACKING_FIELD;
const DISPATCH_DATE_FIELD = process.env.CUSTOMFIELD_DISPATCH_DATE;
const DELIVERY_DATE_FIELD = process.env.CUSTOMFIELD_DELIVERY_DATE;
const RTO_DELIVERED_DATE_FIELD = process.env.CUSTOMFIELD_RTO_DATE;
const POST_DELIVERY_ASSIGNEE = '712020:d710d4e8-270f-4d7a-b65a-7303f71783fb';

const STATUS_MAP = {
  'Ready for pickup': 'Pickup Scheduled',
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

// --- Utility Functions ---
const extractAWB = (trackingFieldValue) => {
  if (!trackingFieldValue) return null;
  const regexes = [
    /(?:awb=|waybill=)(\d{11,})/i,
    /\/p\/(\d{11,})/,
    /\/package\/(\d{11,})/
  ];
  for (const re of regexes) {
    const match = trackingFieldValue.match(re);
    if (match) return match[1];
  }
  return null;
};

const retry = async (fn, retries = 3, delayMs = 1000) => {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      console.warn(`⚠️ Attempt ${attempt} failed: ${err.message}`);
      if (attempt < retries) await new Promise(r => setTimeout(r, delayMs));
    }
  }
  console.error(`❌ All ${retries} attempts failed`);
  return null;
};

const getTracking = async (awb) => {
  return await retry(async () => {
    const res = await axios.get(`https://track.delhivery.com/api/v1/packages/json/?waybill=${awb}`, {
      headers: { Authorization: `Token ${DELHIVERY_TOKEN}` }
    });
    return res.data?.ShipmentData?.[0]?.Shipment || null;
  });
};

const fetchAllIssues = async (jql) => {
  let allIssues = [];
  let startAt = 0;
  const maxResults = 100;

  while (true) {
    const res = await retry(async () => {
      const response = await axios.get(`${JIRA_DOMAIN}/rest/api/3/search`, {
        params: { jql, startAt, maxResults },
        auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN }
      });
      return response.data;
    });

    allIssues.push(...res.issues);
    if (res.issues.length < maxResults) break;
    startAt += maxResults;
  }

  return allIssues;
};

const getJiraIssues = async () => {
  const jqlPickup = `project = ${JIRA_PROJECT} AND "Shipping Tracking Details" IS NOT EMPTY AND created >= "2025-07-01" AND status = "PICKUP SCHEDULED" ORDER BY updated DESC`;
  const jqlOthers = `project = ${JIRA_PROJECT} AND "Shipping Tracking Details" IS NOT EMPTY AND created >= "2025-07-01" AND status NOT IN ("DELIVERED", "RTO DELIVERED", "PICKUP SCHEDULED") ORDER BY updated DESC`;

  console.log('📥 Fetching PICKUP SCHEDULED issues...');
  const pickupIssues = await fetchAllIssues(jqlPickup);

  console.log('📥 Fetching other eligible issues...');
  const otherIssues = await fetchAllIssues(jqlOthers);

  return [...pickupIssues, ...otherIssues];
};

const postCommentADF = async (issueKey, commentText) => {
  if (!commentText) return;

  const commentPayload = {
    body: {
      type: 'doc',
      version: 1,
      content: [{
        type: 'paragraph',
        content: [{ type: 'text', text: commentText }]
      }]
    }
  };

  try {
    await axios.post(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}/comment`, commentPayload, {
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

    const transition = transitionRes.data.transitions.find(t =>
      t.to.name.toLowerCase() === newStatus.toLowerCase()
    );

    if (transition) {
      await axios.post(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}/transitions`, {
        transition: { id: transition.id }
      }, { auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN } });
      console.log(`✅ Status updated to "${newStatus}" for ${issueKey}`);
    } else {
      const available = transitionRes.data.transitions.map(t => t.to.name).join(', ');
      console.log(`⚠️ No matching transition for "${newStatus}" on ${issueKey}. Available targets: ${available}`);
    }

    if (Object.keys(customFields).length > 0) {
      await axios.put(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}`, {
        fields: customFields
      }, { auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN } });
      console.log(`🛠️ Custom fields updated for ${issueKey}`);
    }

    if (comment) {
      await postCommentADF(issueKey, comment);
    }

    if (['DELIVERED', 'RTO DELIVERED'].includes(newStatus)) {
      await axios.put(`${JIRA_DOMAIN}/rest/api/3/issue/${issueKey}/assignee`, {
        accountId: POST_DELIVERY_ASSIGNEE
      }, { auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN } });
      console.log(`👤 Assigned ${issueKey} to post-delivery assignee`);
    }

  } catch (err) {
    console.error(`❌ Failed to update JIRA ${issueKey}:`, err.response?.data || err.message);
  }
};

// --- Main Runner ---
const run = async () => {
  console.log(`🔄 Sync started at ${new Date().toISOString()}`);
  const issues = await getJiraIssues();
  if (!issues) return;

  let updated = 0;
  let skipped = 0;

  for (const issue of issues) {
    const trackingFieldValue = issue.fields[TRACKING_FIELD];
    const awb = extractAWB(trackingFieldValue);
    const currentStatus = issue.fields.status?.name;

    if (!awb) {
      console.log(`⚠️ No valid AWB for ${issue.key}`);
      continue;
    }

    const tracking = await getTracking(awb);
    if (!tracking) continue;

    const status = tracking.Status?.Status;
    const updatedStatus = STATUS_MAP[status];
    if (!updatedStatus) {
      console.log(`⚠️ Unknown status "${status}" for AWB ${awb}`);
      continue;
    }

    if (currentStatus === updatedStatus) {
      console.log(`⏩ Skipping ${issue.key} — already in status "${updatedStatus}"`);
      skipped++;
      continue;
    }

    const customFields = {};
    if (tracking.OriginRecieveDate) customFields[DISPATCH_DATE_FIELD] = dayjs(tracking.OriginRecieveDate).format('YYYY-MM-DD');
    if (tracking.DeliveryDate) customFields[DELIVERY_DATE_FIELD] = dayjs(tracking.DeliveryDate).format('YYYY-MM-DD');
    if (tracking.ReturnedDate) customFields[RTO_DELIVERED_DATE_FIELD] = dayjs(tracking.ReturnedDate).format('YYYY-MM-DD');

    let comment = null;
    switch (updatedStatus) {
      case 'IN - TRANSIT':
        comment = `Order is now in transit as of ${new Date().toISOString()}`; break;
      case 'NDR - 3':
        comment = `Order marked as NDR (Non-Delivery Report) as of ${new Date().toISOString()}`; break;
      case 'RTO IN - TRANSIT':
        comment = `Order is now RTO in transit as of ${new Date().toISOString()}`; break;
      case 'RTO DELIVERED':
        comment = `Order RTO delivered as of ${new Date().toISOString()}`; break;
      case 'DELIVERED':
        comment = `Order successfully delivered on ${new Date().toISOString()}`; break;
    }

    await updateJira(issue.key, updatedStatus, customFields, comment);
    updated++;
  }

  console.log(`✅ Sync finished at ${new Date().toISOString()}`);
  console.log(`📊 Summary: ${updated} updated, ${skipped} skipped`);
};

// --- Error Handling ---
process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 Unhandled Rejection:', reason);
});

process.on('uncaughtException', (err) => {
  console.error('💥 Uncaught Exception:', err);
});

run().catch(err => {
  console.error('💥 Script failed:', err);
  process.exit(1);
});
