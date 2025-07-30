import json
import streamlit as st
import os

st.title("Real-time Weather Monitoring Dashboard")

def read_json(file_path):
    if os.path.exists(file_path):
        with open(file_path) as f:
            data = [json.loads(line) for line in f]
            return data
    return []

st.subheader("Top Hottest Cities")
top_data = read_json("output/top_hottest_cities.json")
st.write(top_data)

st.subheader("Weather Alerts")
alerts_data = read_json("output/weather_alerts.json")
st.write(alerts_data)

st.subheader("Spike Alerts")
spike_data = read_json("output/spike_alerts.json")
st.write(spike_data)

st.subheader("Categorized Weather")
cat_data = read_json("output/categorized_weather.json")
st.write(cat_data)

st.subheader("Average Temperature Per City")
avg_data = read_json("output/avg_temp_per_city.json")
st.write(avg_data)