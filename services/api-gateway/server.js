const express = require('express');
const cors = require('cors');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const multer = require('multer');

const NamingService = require('../common/NamingService');
const DSMiddleware = require('../common/DSMiddleware');
const DFSManager = require('../common/DFSManager');
const VectorClock = require('../common/VectorClock');
const ElectionEngine = require('../common/ElectionEngine');

const PORT = process.env.PORT || 5000;

const namingService = new NamingService();
const dsMiddleware = new DSMiddleware('GATEWAY');
const dfsManager = new DFSManager();
const gatewayVectorClock = new VectorClock('GATEWAY');

const NODES = {
  CE: { nodeId: 'node-ce', department: 'CE', url: process.env.NODE_CE_URL || 'http://localhost:5001', port: 5001, status: 'UNKNOWN', processId: 1 },
  IT: { nodeId: 'node-it', department: 'IT', url: process.env.NODE_IT_URL || 'http://localhost:5002', port: 5002, status: 'UNKNOWN', processId: 2 },
  'E&TC': { nodeId: 'node-etc', department: 'E&TC', url: process.env.NODE_ETC_URL || 'http://localhost:5003', port: 5003, status: 'UNKNOWN', processId: 3 }
};

const app = express();
app.use(cors());
app.use(express.json());

const uploadsRoot = path.join(__dirname, '..', '..', 'uploads');
if (!fs.existsSync(uploadsRoot)) fs.mkdirSync(uploadsRoot, { recursive: true });
app.use('/uploads', express.static(uploadsRoot));
const gatewayUpload = multer({
  dest: path.join(uploadsRoot, '.gateway-temp'),
  limits: { fileSize: 100 * 1024 * 1024 }
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

function broadcastWS(data) {
  const payload = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

// Heartbeat & Beacon Ping Monitor
async function checkNodeHealth(deptKey) {
  const node = NODES[deptKey];
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 2000);
    const response = await fetch(`${node.url}/health`, { signal: controller.signal });
    clearTimeout(timeoutId);

    if (response.ok) {
      const data = await response.json();
      NODES[deptKey].status = 'ONLINE';
      NODES[deptKey].details = data;
      dsMiddleware.handleBeaconPing(deptKey, 'ONLINE', data.vectorClock);
    } else {
      NODES[deptKey].status = 'OFFLINE';
      NODES[deptKey].details = { error: 'Node returned non-200' };
      dsMiddleware.handleBeaconPing(deptKey, 'OFFLINE', null);
    }
  } catch (err) {
    NODES[deptKey].status = 'OFFLINE';
    NODES[deptKey].details = { error: err.message || 'Connection Refused' };
    dsMiddleware.handleBeaconPing(deptKey, 'OFFLINE', null);
  }
}

setInterval(() => {
  Promise.all(Object.keys(NODES).map(key => checkNodeHealth(key))).then(() => {
    broadcastWS({
      type: 'NODE_STATUS_UPDATE',
      nodes: NODES,
      gatewayVectorClock: gatewayVectorClock.getVector(),
      timestamp: new Date().toISOString()
    });
  });
}, 3000);

Promise.all(Object.keys(NODES).map(key => checkNodeHealth(key)));

wss.on('connection', ws => {
  ws.send(JSON.stringify({
    type: 'INITIAL_STATE',
    nodes: NODES,
    namingDirectory: namingService.getDirectory(),
    gatewayVectorClock: gatewayVectorClock.getVector()
  }));
});

// API Routes

// 1. Get Node Statuses & Directory
app.get('/api/nodes', (req, res) => {
  res.json({
    gatewayStatus: 'ONLINE',
    nodes: NODES,
    namingDirectory: namingService.getDirectory(),
    gatewayVectorClock: gatewayVectorClock.getVector()
  });
});

// Proxy Node Internal Audit Logs
app.get('/api/nodes/:dept/logs', async (req, res) => {
  const deptKey = req.params.dept.toUpperCase();
  const targetNode = NODES[deptKey];
  if (!targetNode || targetNode.status !== 'ONLINE') {
    return res.json({ nodeId: deptKey, logs: [] });
  }
  try {
    const resp = await fetch(`${targetNode.url}/logs`);
    if (resp.ok) {
      const text = await resp.text();
      const data = text ? JSON.parse(text) : { logs: [] };
      return res.json(data);
    }
    res.json({ nodeId: deptKey, logs: [] });
  } catch (err) {
    res.json({ nodeId: deptKey, logs: [] });
  }
});

// 2. Toggle Node Status
app.post('/api/nodes/:dept/toggle', async (req, res) => {
  const deptKey = req.params.dept.toUpperCase();
  const targetNode = NODES[deptKey];
  if (!targetNode) return res.status(404).json({ error: 'Department node not found' });

  const nextStatus = targetNode.status === 'ONLINE' ? 'OFFLINE' : 'ONLINE';
  try {
    const response = await fetch(`${targetNode.url}/status`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: nextStatus })
    });
    const data = await response.json();
    NODES[deptKey].status = nextStatus;

    gatewayVectorClock.tick(`ToggleNode:${deptKey}:${nextStatus}`);

    broadcastWS({
      type: 'NODE_STATUS_TOGGLED',
      department: deptKey,
      newStatus: nextStatus,
      timestamp: new Date().toISOString()
    });

    res.json({ message: `Toggled ${deptKey} node to ${nextStatus}`, data, nodes: NODES });
  } catch (err) {
    res.status(500).json({ error: `Failed to toggle node status: ${err.message}` });
  }
});

// 3. Aggregate Resources
app.get('/api/resources', async (req, res) => {
  const { department, subject, resourceType, search } = req.query;

  let targetDepts = Object.keys(NODES);
  if (department && department !== 'ALL') {
    const normalizedDept = department.toUpperCase();
    targetDepts = targetDepts.filter(d => d === normalizedDept || (d === 'E&TC' && (normalizedDept === 'ETC' || normalizedDept === 'E&TC')));
  }

  let allResources = [];
  let nodeStatusSummary = {};

  for (const deptKey of targetDepts) {
    const node = NODES[deptKey];
    if (node.status === 'ONLINE') {
      try {
        const queryParams = new URLSearchParams();
        if (subject) queryParams.append('subject', subject);
        if (resourceType) queryParams.append('resourceType', resourceType);
        if (search) queryParams.append('search', search);

        const resp = await fetch(`${node.url}/resources?${queryParams.toString()}`);
        if (resp.ok) {
          const data = await resp.json();
          allResources.push(...(data.resources || []));
          nodeStatusSummary[deptKey] = { status: 'ONLINE', count: data.count, vectorClock: data.vectorClock };
        } else {
          nodeStatusSummary[deptKey] = { status: 'OFFLINE', error: 'HTTP error' };
        }
      } catch (err) {
        nodeStatusSummary[deptKey] = { status: 'OFFLINE', error: err.message };
      }
    } else {
      nodeStatusSummary[deptKey] = { status: 'OFFLINE', note: 'Node marked offline' };
    }
  }

  allResources.sort((a, b) => new Date(b.uploadDate) - new Date(a.uploadDate));

  res.json({
    gateway: 'API-Gateway-Proxy',
    totalResources: allResources.length,
    nodeStatusSummary,
    gatewayVectorClock: gatewayVectorClock.getVector(),
    resources: allResources
  });
});

// 4. Upload Resource (Proxies to target node & registers in Mini-DFS)
app.post('/api/resources/upload', gatewayUpload.single('file'), async (req, res) => {
  const dept = (req.headers['x-department'] || req.body?.department || 'CE').toUpperCase();
  const deptKey = (dept === 'ETC' || dept === 'E&TC') ? 'E&TC' : dept;
  
  // Location Transparency Resolution
  const resolvedEndpoint = dsMiddleware.resolveLocation(deptKey);
  const targetNode = NODES[deptKey];

  if (!targetNode) return res.status(400).json({ error: `Invalid department: ${dept}` });
  
  // Re-verify node health on demand if not online
  if (targetNode.status !== 'ONLINE') {
    await checkNodeHealth(deptKey);
  }

  if (targetNode.status !== 'ONLINE') {
    return res.status(503).json({
      error: `Department Node [Node ${deptKey}] is currently OFFLINE. Please check Nodes Monitor to turn Node ${deptKey} ON.`
    });
  }

  gatewayVectorClock.tick(`UploadProxy:${deptKey}`);

  try {
    const form = new FormData();
    Object.entries(req.body || {}).forEach(([key, value]) => form.append(key, value));
    form.append('vector', JSON.stringify(gatewayVectorClock.getVector()));
    if (req.file) {
      form.append('file', new Blob([fs.readFileSync(req.file.path)], { type: req.file.mimetype }), req.file.originalname);
    }

    const resp = await fetch(`${targetNode.url}/resources`, { method: 'POST', body: form });
    const text = await resp.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch (e) {
      console.error(`[Upload Proxy Error] Department node ${deptKey} returned non-JSON response:`, text);
      return res.status(500).json({ error: text || 'Invalid response from department node' });
    }

    if (!resp.ok) {
      console.error(`[Upload Proxy Error] Department node ${deptKey} status ${resp.status}:`, data);
      return res.status(resp.status || 500).json({ error: data.error || data.message || `Department node ${deptKey} upload failed` });
    }

    // Unit IV DFS Registration & Primary-Backup Replication
    const dfsRecord = dfsManager.registerDFSFile(data.resource || req.body);

    broadcastWS({
      type: 'RESOURCE_UPLOADED',
      department: deptKey,
      resource: data.resource,
      dfsRecord,
      timestamp: new Date().toISOString()
    });

    res.status(201).json({ ...data, dfsRecord });
  } catch (err) {
    res.status(500).json({ error: `Failed to proxy resource upload to ${deptKey}: ${err.message}` });
  } finally {
    if (req.file) fs.unlink(req.file.path, () => {});
  }
});

// 5. Delete Resource (Proxies delete to target node)
app.delete('/api/resources/:dept/:id', async (req, res) => {
  let deptKey = req.params.dept.toUpperCase();
  if (deptKey === 'ETC') deptKey = 'E&TC';

  const targetNode = NODES[deptKey];
  if (!targetNode) return res.status(404).json({ error: `Invalid department: ${req.params.dept}` });

  if (targetNode.status !== 'ONLINE') {
    return res.status(503).json({ error: `Department Node [Node ${deptKey}] is currently OFFLINE.` });
  }

  try {
    const resp = await fetch(`${targetNode.url}/resources/${req.params.id}`, {
      method: 'DELETE'
    });
    const text = await resp.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch (e) {
      data = { message: text };
    }

    if (!resp.ok) {
      return res.status(resp.status || 500).json(data);
    }

    gatewayVectorClock.tick(`DeleteProxy:${deptKey}:${req.params.id}`);

    broadcastWS({
      type: 'RESOURCE_DELETED',
      department: deptKey,
      resourceId: req.params.id,
      timestamp: new Date().toISOString()
    });

    res.json({ message: `Resource deleted from Node ${deptKey}`, ...data });
  } catch (err) {
    res.status(500).json({ error: `Failed to proxy delete to ${deptKey}: ${err.message}` });
  }
});

// 6. Download Endpoint (Guaranteed PDF Download)
app.get('/api/download', (req, res) => {
  const filePath = req.query.filePath || '';
  let fileName = req.query.fileName || req.query.title || 'NoteSync_Academic_Resource.pdf';

  // Sanitize filename and force .pdf extension
  fileName = fileName.replace(/[/\\?%*:|"<>]/g, '_');
  if (!fileName.toLowerCase().endsWith('.pdf')) {
    fileName = fileName + '.pdf';
  }

  if (filePath) {
    const cleanPath = filePath.startsWith('/') ? filePath.slice(1) : filePath;
    const absolutePath = path.join(__dirname, '..', '..', cleanPath);

    if (fs.existsSync(absolutePath)) {
      return res.download(absolutePath, fileName);
    }
  }

  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
  res.send(`%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Resources<</Font<</F1 4 0 R>>>>/Contents 5 0 R>>endobj\n4 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n5 0 obj<</Length 44>>stream\nBT /F1 14 Tf 50 700 TD (NoteSync Academic PDF Resource) Tj ET\nendstream\nendobj\nxref\n0 6\n0000000000 65535 f \n0000000009 00000 n \n0000000052 00000 n \n00000000102 00000 n \n00000000212 00000 n \n00000000281 00000 n \ntrailer<</Size 6/Root 1 0 R>>\nstartxref\n376\n%%EOF`);
});

// Serve static client bundle if built
const clientDist = path.join(__dirname, '..', '..', 'client', 'dist');
if (fs.existsSync(clientDist)) {
  app.use(express.static(clientDist));
  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api') || req.path.startsWith('/uploads')) return next();
    res.sendFile(path.join(clientDist, 'index.html'));
  });
}

server.listen(PORT, () => {
  console.log(`=======================================================`);
  console.log(`[API Gateway] Running on http://localhost:${PORT}`);
  console.log(`[API Gateway] WebSocket server listening for real-time DS events`);
  console.log(`=======================================================`);
});
