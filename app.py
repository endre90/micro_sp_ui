import streamlit as st
import redis
import json

# Adjust for your environment
REDIS_HOST = "redis"
REDIS_PORT = 6379

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

import streamlit as st

def main():
    st.set_page_config(layout="wide")
    st.title("micro_sp_ui")

    tab1, tab2, tab3, tab4= st.tabs(["View State", "Set State", "Operations", "About"])

    data = read_all_data()

    with tab1:
        state_viewer(data)
    with tab2:
        state_setter()
    # with tab3:
    #     view_operation_state()

def read_all_data():
    all_keys = r.keys("*")
    data = []
    for key in all_keys:
        raw_value = r.get(key)
    
        try:
            parsed_value = json.loads(raw_value)  # Try to parse JSON
            value_type = parsed_value.get("type", "Unknown")
            value_content = parsed_value.get("value", "Unknown")

            # Handle cases where value is a nested dictionary
            # TODO This has to be recursive
            if isinstance(value_content, dict):
                value_content = next(iter(value_content.values()), "Unknown")

        except json.JSONDecodeError:
            value_type = "Raw"
            value_content = raw_value

        data.append({"Variable Name": key, "Value": value_content, "Type": value_type})
    return data


def state_viewer(data):
    st.subheader("Current Data on Redis")

    if not data:
        st.write("No keys found in Redis.")
    else:
        st.dataframe(data, use_container_width=True)

def state_setter(data):
    st.subheader("Set Values")

    if not data:
        st.write("No keys found in Redis.")
        return

    

    # Initialize session state for key-value pairs if not present
    if "key_values" not in st.session_state:
        st.session_state.key_values = [{"key": all_keys[0], "value": ""}]
    
    # Button to add an extra row for a new key-value pair
    if st.button("Add another key field"):
        st.session_state.key_values.append({"key": all_keys[0], "value": ""})
    
    # Render dropdowns and text inputs side by side for each row
    for i, pair in enumerate(st.session_state.key_values):
        cols = st.columns(2)
        with cols[0]:
            st.session_state.key_values[i]["key"] = st.selectbox(
                f"Select Variable {i+1}", all_keys, index=all_keys.index(pair["key"]), key=f"key_select_{i}"
            )
        
        raw_value = r.get(st.session_state.key_values[i]["key"])
        try:
            parsed_value = json.loads(raw_value)
            current_type = parsed_value.get("type", "Unknown")
            current_value = parsed_value.get("value", "")
            if isinstance(current_value, dict):
                current_value = next(iter(current_value.values()), "")
        except json.JSONDecodeError:
            current_type = "Raw"
            current_value = raw_value
        
        with cols[1]:
            st.session_state.key_values[i]["value"] = st.text_input(
                f"New Value {i+1}", value=current_value, key=f"value_input_{i}"
            )
    
    # Button to update all values in Redis
    if st.button("Update All"):
        for pair in st.session_state.key_values:
            updated_entry = json.dumps({"type": current_type, "value": pair["value"]})
            print(updated_entry)
            r.set(pair["key"], updated_entry)
        st.success("Values updated!")




if __name__ == "__main__":
    main()


