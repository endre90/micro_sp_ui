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

    with tab1:
        state_viewer()
    with tab2:
        state_setter()
    with tab3:
        view_operation_state()

def state_viewer():
    st.subheader("Current Data on Redis")

    all_keys = r.keys("*")
    if not all_keys:
        st.write("No keys found in Redis.")
    else:
        data = []
        for key in all_keys:
            raw_value = r.get(key)
            
            try:
                parsed_value = json.loads(raw_value)  # Try to parse JSON
                value_type = parsed_value.get("type", "Unknown")
                value_content = parsed_value.get("value", "Unknown")

                # Handle cases where value is a nested dictionary
                if isinstance(value_content, dict):
                    value_content = next(iter(value_content.values()), "Unknown")

            except json.JSONDecodeError:
                value_type = "Raw"
                value_content = raw_value

            data.append({"Variable Name": key, "Value": value_content, "Type": value_type})

        # Display the table with sorting enabled
        st.dataframe(data, use_container_width=True)

# def state_viewer():
#     st.subheader("Current Data on Redis")

#     all_keys = r.keys("*")
#     if not all_keys:
#         st.write("No keys found in Redis.")
#     else:
#         data = []
#         for index, key in enumerate(all_keys, start=1):
#             raw_value = r.get(key)
            
#             try:
#                 parsed_value = json.loads(raw_value)  # Try to parse JSON
#                 value_type = parsed_value.get("type", "Unknown")
#                 value_content = parsed_value.get("value", "Unknown")

#                 # Handle cases where value is a nested dictionary
#                 if isinstance(value_content, dict):
#                     value_content = next(iter(value_content.values()), "Unknown")

#             except json.JSONDecodeError:
#                 value_type = "Raw"
#                 value_content = raw_value

#             data.append({"Variable Name": key, "Value": value_content, "Type": value_type})

#         st.table(data)



# def state_viewer():
#     st.subheader("Current Data on Redis")
#     all_keys = r.keys("*")
#     if not all_keys:
#         st.write("No keys found in Redis.")
#     else:
#         data = [{"key": k, "value": r.get(k)} for k in all_keys]
#         st.table(data)

def state_setter():
    # -------------------------------------
    # PART 2: Let user set multiple values
    # -------------------------------------
    st.subheader("Set Values")

    # For demonstration, let's handle N key-value pairs at once
    # We'll store them in a list of dictionaries in session state
    if "key_values" not in st.session_state:
        # Initialize with a single blank row
        st.session_state.key_values = [{"key": "", "value": ""}]

    # Button to add an extra row for a new key
    if st.button("Add another key field"):
        st.session_state.key_values.append({"key": "", "value": ""})

    # Now, render text inputs for each row
    for i, pair in enumerate(st.session_state.key_values):
        cols = st.columns(2)
        with cols[0]:
            st.session_state.key_values[i]["key"] = st.text_input(
                f"Key {i+1}",
                value=pair["key"],
                key=f"key_field_{i}"
            )
        with cols[1]:
            st.session_state.key_values[i]["value"] = st.text_input(
                f"Value {i+1}",
                value=pair["value"],
                key=f"value_field_{i}"
            )

    # -----------------------------------------------------
    # PART 3: On update button click, write all to Redis
    # -----------------------------------------------------
    if st.button("Update All"):
        # Filter out empty keys
        valid_pairs = {d["key"]: d["value"] for d in st.session_state.key_values if d["key"].strip() != ""}
        if valid_pairs:
            # mset lets us set multiple values atomically
            r.mset(valid_pairs)
            st.success("Values updated!")
        else:
            st.warning("No valid (non-empty) keys to update.")

        # Optionally, clear fields or do something else

if __name__ == "__main__":
    main()


