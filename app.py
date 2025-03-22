import streamlit as st
import redis

st.title("Redis Key-Value Viewer")

# Adjust this to match your Redis container network settings
# If you're using Docker Compose, you can often reach Redis via the service name ("redis")
redis_host = "redis"
redis_port = 6379

r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

keys = r.keys("*")
if not keys:
    st.write("No keys found in Redis.")
else:
    data = [{"key": k, "value": r.get(k)} for k in keys]
    st.table(data)
