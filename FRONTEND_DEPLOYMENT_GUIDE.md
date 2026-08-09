# Frontend Deployment to AWS EKS - Quick Guide
## React UI Applications for Item Kafka POC

---

## 📁 Frontend Projects Overview

Your workspace contains multiple React frontend applications:

1. **ReactJS-UI-For-Item-Kafka-Producer-POC**
   - Main UI for Kafka Producer
   - Located: `C:\Workspaces\ReactJS-UI-For-Item-Kafka-Producer-POC`
   - Purpose: Exercise all backend producer endpoints, paginated item grid, message publishing

2. **ReactJS-UI-For-Item-Kafka-Producer-Consumer-POC**
   - Extended UI for Consumer workflows
   - Located: `C:\Workspaces\ReactJS-UI-For-Item-Kafka-Producer-Consumer-POC`
   - Purpose: Consume items, view consumed records, manual consumer testing

3. **ReactJS_TestClient_For_ConfluentCloud_Kafka-POC**
   - Test client application (CURRENTLY EMPTY - needs initialization)
   - Located: `C:\Workspaces\ReactJS_TestClient_For_ConfluentCloud_Kafka-POC`
   - Purpose: API testing, load testing, health checks

---

## 🔧 Frontend Build Arguments

### VITE_API_BASE_URL (Required)

This environment variable is **compiled at build time** because Vite is a static site builder.

```powershell
# For development (local backend)
docker build -t item-kafka-ui:dev `
  --build-arg VITE_API_BASE_URL=http://localhost:8082 .

# For staging (AWS endpoint)
docker build -t item-kafka-ui:staging `
  --build-arg VITE_API_BASE_URL=https://api-staging.yourdomain.com .

# For production (AWS endpoint)
docker build -t item-kafka-ui:prod `
  --build-arg VITE_API_BASE_URL=https://api.yourdomain.com .
```

### Other Environment Variables (Optional)

- `VITE_ENVIRONMENT` - dev/staging/prod (affects logging, error handling)
- `VITE_LOG_LEVEL` - debug/info/warn/error
- `VITE_API_TIMEOUT` - Request timeout in ms (default: 30000)

---

## 📦 Build & Push Frontend Images

### Producer UI

```powershell
cd C:\Workspaces\ReactJS-UI-For-Item-Kafka-Producer-POC

# Build
docker build -t item-kafka-ui-producer:latest `
  --build-arg VITE_API_BASE_URL=https://api.yourdomain.com .

# Tag
docker tag item-kafka-ui-producer:latest `
  123456789012.dkr.ecr.af-south-1.amazonaws.com/item-kafka-ui-producer:latest

# Push
docker push 123456789012.dkr.ecr.af-south-1.amazonaws.com/item-kafka-ui-producer:latest
```

### Consumer UI

```powershell
cd C:\Workspaces\ReactJS-UI-For-Item-Kafka-Producer-Consumer-POC

# Build
docker build -t item-kafka-ui-consumer:latest `
  --build-arg VITE_API_BASE_URL=https://api.yourdomain.com .

# Tag
docker tag item-kafka-ui-consumer:latest `
  123456789012.dkr.ecr.af-south-1.amazonaws.com/item-kafka-ui-consumer:latest

# Push
docker push 123456789012.dkr.ecr.af-south-1.amazonaws.com/item-kafka-ui-consumer:latest
```

### Test Client UI (Once Initialized)

```powershell
cd C:\Workspaces\ReactJS_TestClient_For_ConfluentCloud_Kafka-POC

# Build
docker build -t item-kafka-test-client:latest `
  --build-arg VITE_API_BASE_URL=https://api.yourdomain.com .

# Tag & Push
docker tag item-kafka-test-client:latest `
  123456789012.dkr.ecr.af-south-1.amazonaws.com/item-kafka-test-client:latest
docker push 123456789012.dkr.ecr.af-south-1.amazonaws.com/item-kafka-test-client:latest
```

---

## ☸️ Kubernetes Manifests for Frontends

### frontend-producer-deployment.yaml

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: item-kafka-ui-producer
  namespace: item-kafka-poc
  labels:
    app: item-kafka-ui-producer
spec:
  replicas: 2
  selector:
    matchLabels:
      app: item-kafka-ui-producer
  template:
    metadata:
      labels:
        app: item-kafka-ui-producer
    spec:
      containers:
      - name: ui-producer
        image: 123456789012.dkr.ecr.af-south-1.amazonaws.com/item-kafka-ui-producer:latest
        imagePullPolicy: Always
        ports:
        - name: http
          containerPort: 3000
          protocol: TCP
        env:
        - name: NODE_ENV
          value: "production"
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /
            port: 3000
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /
            port: 3000
          initialDelaySeconds: 10
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: item-kafka-ui-producer
  namespace: item-kafka-poc
  labels:
    app: item-kafka-ui-producer
spec:
  type: LoadBalancer
  selector:
    app: item-kafka-ui-producer
  ports:
  - port: 80
    targetPort: 3000
    name: http
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: item-kafka-ui-producer-hpa
  namespace: item-kafka-poc
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: item-kafka-ui-producer
  minReplicas: 2
  maxReplicas: 5
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: item-kafka-ui-producer-ingress
  namespace: item-kafka-poc
  annotations:
    kubernetes.io/ingress.class: alb
    alb.ingress.kubernetes.io/scheme: internet-facing
    alb.ingress.kubernetes.io/target-type: ip
spec:
  rules:
  - host: ui.yourdomain.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: item-kafka-ui-producer
            port:
              number: 80
```

### frontend-consumer-deployment.yaml

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: item-kafka-ui-consumer
  namespace: item-kafka-poc
  labels:
    app: item-kafka-ui-consumer
spec:
  replicas: 2
  selector:
    matchLabels:
      app: item-kafka-ui-consumer
  template:
    metadata:
      labels:
        app: item-kafka-ui-consumer
    spec:
      containers:
      - name: ui-consumer
        image: 123456789012.dkr.ecr.af-south-1.amazonaws.com/item-kafka-ui-consumer:latest
        imagePullPolicy: Always
        ports:
        - name: http
          containerPort: 3000
          protocol: TCP
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /
            port: 3000
          initialDelaySeconds: 30
          periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: item-kafka-ui-consumer
  namespace: item-kafka-poc
spec:
  type: LoadBalancer
  selector:
    app: item-kafka-ui-consumer
  ports:
  - port: 80
    targetPort: 3000
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: item-kafka-ui-consumer-hpa
  namespace: item-kafka-poc
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: item-kafka-ui-consumer
  minReplicas: 2
  maxReplicas: 5
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

---

## 🚀 Deploy Frontend to EKS

```powershell
# Copy manifests to k8s/frontend/ directory
mkdir -p k8s/frontend

# Apply manifests (use whichever UI(s) you want to deploy)
kubectl apply -f k8s/frontend/frontend-producer-deployment.yaml

# Or consumer
kubectl apply -f k8s/frontend/frontend-consumer-deployment.yaml

# Verify
kubectl get pods -n item-kafka-poc -l app=item-kafka-ui-producer
kubectl get svc -n item-kafka-poc

# Get LoadBalancer endpoint
kubectl get svc item-kafka-ui-producer -n item-kafka-poc -o wide
```

---

## 🔐 Frontend CORS Requirements

The React frontend calls the backend API via CORS. Ensure the backend's CORS configuration allows the frontend domain:

### Backend ConfigMap Configuration

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: item-kafka-config
  namespace: item-kafka-poc
data:
  item_cors_allowedOrigins: "https://ui.yourdomain.com,https://ui-consumer.yourdomain.com,https://test.yourdomain.com"
```

### Backend Java Config (CorsConfig.java)

The backend should have a `CorsConfig` class that reads this property and applies CORS headers.

---

## 📝 Local Development Setup

### Prerequisites

```powershell
# Node.js 18+ (includes npm)
node --version
npm --version

# If not installed:
choco install -y nodejs
```

### Run Frontend Locally

```powershell
cd C:\Workspaces\ReactJS-UI-For-Item-Kafka-Producer-POC

# Set API URL environment
$env:VITE_API_BASE_URL = "http://localhost:8082"

# Install dependencies
npm install

# Start dev server (runs on http://localhost:5173)
npm run dev

# Or build for production
npm run build
npm run preview  # Preview production build locally
```

### Local Docker Development

```powershell
# Build with local backend URL
docker build -t item-kafka-ui:local `
  --build-arg VITE_API_BASE_URL=http://host.docker.internal:8082 .

# Run locally
docker run -p 3000:3000 item-kafka-ui:local

# Access at http://localhost:3000
```

---

## 🧪 Frontend Environment Per Deployment

### Development (Local)

```bash
VITE_API_BASE_URL=http://localhost:8082
VITE_ENVIRONMENT=dev
VITE_LOG_LEVEL=debug
```

### Staging (AWS)

```bash
VITE_API_BASE_URL=https://api-staging.yourdomain.com
VITE_ENVIRONMENT=staging
VITE_LOG_LEVEL=info
```

### Production (AWS)

```bash
VITE_API_BASE_URL=https://api.yourdomain.com
VITE_ENVIRONMENT=prod
VITE_LOG_LEVEL=warn
```

---

## 🧠 TestClient Initialization Guide

The **ReactJS_TestClient_For_ConfluentCloud_Kafka-POC** project is currently empty. Here's how to set it up:

### Option 1: Initialize from NPM template

```powershell
cd C:\Workspaces\ReactJS_TestClient_For_ConfluentCloud_Kafka-POC

# Create Vite React project skeleton
npm create vite@latest . -- --template react

# Install dependencies
npm install

# Add test utilities
npm install -D vitest @testing-library/react @testing-library/jest-dom

# Install API testing libraries
npm install axios jest-mock-extended
```

### Option 2: Copy and Modify Existing UI

```powershell
# Copy producer UI as template
Copy-Item -Recurse "C:\Workspaces\ReactJS-UI-For-Item-Kafka-Producer-POC\*" `
  -Destination "C:\Workspaces\ReactJS_TestClient_For_ConfluentCloud_Kafka-POC\" `
  -Exclude node_modules,.git,dist,build

# Update package.json project name
# Update README with test client documentation
```

### TestClient Structure (Recommended)

```
ReactJS_TestClient_For_ConfluentCloud_Kafka-POC/
├── src/
│   ├── tests/
│   │   ├── api.test.js
│   │   ├── e2e.test.js
│   │   └── load.test.js
│   ├── utils/
│   │   ├── apiClient.js
│   │   └── testHelpers.js
│   ├── App.jsx
│   └── main.jsx
├── vitest.config.js
├── package.json
├── README.md
└── Dockerfile
```

### Sample TestClient API Testing Component

```javascript
// src/utils/apiClient.js
import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8082';

const client = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
});

export const apiTests = {
  // Health check
  async health() {
    return client.get('/actuator/health');
  },

  // Producer APIs
  async publishItems() {
    return client.post('/item-kafka/app/publish-items/v1');
  },

  async sendItems(payload) {
    return client.post('/item-kafka/app/send-items/v1', payload);
  },

  // Consumer APIs
  async consumeItems() {
    return client.get('/item-kafka/app/consume-items/v1');
  },

  async consumerStatus() {
    return client.get('/item-kafka/consumer/consume-status/v1');
  },

  async manualConsume() {
    return client.post('/item-kafka/consumer/manual-consume/v1');
  },

  // Flink Jobs
  async submitJob(jobType) {
    return client.post(`/flink/job/submit/${jobType}`);
  },

  // Load test - publish N items
  async loadTest(count = 100) {
    const results = [];
    for (let i = 0; i < count; i++) {
      results.push(await this.publishItems());
    }
    return results;
  },
};

export default client;
```

---

## 🔗 Connecting the UIs

### Producer UI Flow
1. User logs in
2. Publishes items to Kafka
3. Checks producer status

### Consumer UI Flow
1. Connects to backend
2. Views consumed messages
3. Interacts with Flink jobs

### Test Client Flow
1. Runs API endpoint tests
2. Executes load tests
3. Monitors backend health

---

## 📊 Nginx Reverse Proxy (Optional)

If you want to serve all frontends from a single domain with URL paths:

```nginx
# frontend-nginx.conf
server {
    listen 80;
    server_name yourdomain.com;

    # Producer UI
    location / {
        proxy_pass http://item-kafka-ui-producer:3000;
        proxy_set_header Host $host;
    }

    # Consumer UI
    location /consumer {
        proxy_pass http://item-kafka-ui-consumer:3000;
        proxy_set_header Host $host;
    }

    # Test Client
    location /test {
        proxy_pass http://item-kafka-test-client:3000;
        proxy_set_header Host $host;
    }

    # Backend API (proxy-pass)
    location /api {
        proxy_pass http://item-kafka-backend:8082;
        proxy_set_header Host $host;
    }
}
```

---

## ✅ Frontend Deployment Checklist

- [ ] Set correct `VITE_API_BASE_URL` for your environment
- [ ] Build Docker images successfully
- [ ] Push images to ECR
- [ ] Update Kubernetes deployment manifests with image URIs
- [ ] Backend CORS configuration includes frontend origin
- [ ] HPA configured for auto-scaling
- [ ] ALB ingress rules correct
- [ ] Route53 DNS records point to ALB
- [ ] Test frontend load time and API response
- [ ] Setup monitoring/alerts for frontend pod health

---

## 🆘 Frontend Troubleshooting

| Issue | Solution |
|---|---|
| "Cannot reach API" | Check VITE_API_BASE_URL env var, verify backend ALB is accessible |
| CORS errors | Verify backend CORS config includes frontend domain |
| Blank page | Check pod logs: `kubectl logs -n item-kafka-poc -l app=item-kafka-ui-producer` |
| Slow UI load | Check pod resources, enable HPA to add replicas |
| API 404 errors | Verify backend API endpoint path and method |

---

## 📚 Reference

- Frontend (Producer): `C:\Workspaces\ReactJS-UI-For-Item-Kafka-Producer-POC/EKS_README.md`
- Main Cheatsheet: `AWS_QUICKSTART_CHEATSHEET.md`
- Backend API: `API_DOCUMENTATION.md`

---

**Last Updated:** August 8, 2026

