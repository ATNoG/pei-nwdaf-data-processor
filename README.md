# pei-nwdaf-processor


## **How to Test**

### **1. Launch Kafka via Docker**

```bash
docker run -p 9092:9092 apache/kafka:4.1.1
```

### **2. Create Required Topics**

You need two Kafka topics: **raw-data** and **processed-data**.

```bash
utils/topic.sh [container] "raw-data" -c
utils/topic.sh [container] "processed-data" -c
```

---

### **3. Start the FastAPI Server (Ingestion Component)**

```bash
uvicorn receiver:app --reload --host 0.0.0.0 --port 8000
```

---

### **4. Run the Processor Component**

```bash
python3 producer/main.py -a "http://localhost:8000/receive" -f dataset/hbahn/latency_data.csv
```

---



## TODO: 
- Implement empty window strategies