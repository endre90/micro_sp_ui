import streamlit as st
import redis
import json
import logging
import sys
from typing import List, Dict, Optional, Union, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone

# --- Configuration ---
REDIS_HOST: str = "redis"  # Use "localhost" if Redis is running locally outside Docker
REDIS_PORT: int = 6379

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr,  # Direct logs to stderr for visibility in Streamlit/Docker
)

# --- Python Equivalents for Rust SPValue ---
# Using dataclasses for structured representation

@dataclass
class BoolOrUnknown:
    value: Optional[bool] = None # None represents UNKNOWN

@dataclass
class FloatOrUnknown:
    # Note: Rust's OrderedFloat is for hashing/ordering; Python float is used here.
    # Serialization might differ, but standard float is common.
    value: Optional[float] = None # None represents UNKNOWN

@dataclass
class IntOrUnknown:
    value: Optional[int] = None # None represents UNKNOWN

@dataclass
class StringOrUnknown:
    value: Optional[str] = None # None represents UNKNOWN

@dataclass
class TimeOrUnknown:
    # Assuming SystemTime serializes to something Python's datetime can parse
    # Common formats: RFC3339 string or Unix timestamp (seconds/ms/ns)
    value: Optional[datetime] = None # None represents UNKNOWN

@dataclass
class ArrayOrUnknown:
    # The list will contain instances of the SPValueType union
    value: Optional[List['SPValueType']] = field(default_factory=list) # None represents UNKNOWN

# --- Type Alias for the Union of Possible SPValue Types ---
# This represents the actual deserialized value you'll work with
SPValueType = Union[
    BoolOrUnknown,
    FloatOrUnknown,
    IntOrUnknown,
    StringOrUnknown,
    TimeOrUnknown,
    ArrayOrUnknown,
]

# --- Redis Connection ---
try:
    r: redis.Redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    # Check connection
    r.ping()
    logging.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.exceptions.ConnectionError as e:
    logging.error(f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {e}")
    st.error(f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}. Please ensure Redis is running and accessible. Error: {e}")
    # Exit gracefully if connection fails
    sys.exit(1)
except Exception as e:
    logging.error(f"An unexpected error occurred during Redis connection: {e}")
    st.error(f"An unexpected error occurred during Redis connection: {e}")
    sys.exit(1)


# --- Deserialization Function ---
def deserialize_spvalue(json_str: str) -> Optional[SPValueType]:
    """
    Deserializes a JSON string (from Redis) into a corresponding Python SPValueType object.
    Assumes the JSON structure matches Rust's serde serialization:
    {"type": "TypeName", "value": actual_value_or_UNKNOWN}
    """
    try:
        data: Dict[str, Any] = json.loads(json_str)
        value_type = data.get("type")
        raw_value = data.get("value")

        if raw_value == "UNKNOWN":
            # Create the appropriate XOrUnknown type with value=None
            if value_type == "Bool":
                return BoolOrUnknown(value=None)
            elif value_type == "Float64":
                return FloatOrUnknown(value=None)
            elif value_type == "Int64":
                return IntOrUnknown(value=None)
            elif value_type == "String":
                return StringOrUnknown(value=None)
            elif value_type == "Time":
                return TimeOrUnknown(value=None)
            elif value_type == "Array":
                return ArrayOrUnknown(value=None)
            else:
                logging.warning(f"Unknown SPValue type encountered during deserialization (UNKNOWN value): {value_type}")
                return None # Or raise an error

        # Handle known types with actual values
        if value_type == "Bool":
            if isinstance(raw_value, bool):
                 return BoolOrUnknown(value=raw_value)
            # Handle potential variations like {"Bool": true} if needed
            elif isinstance(raw_value, dict) and "Bool" in raw_value:
                 return BoolOrUnknown(value=bool(raw_value["Bool"]))
            else:
                 logging.warning(f"Unexpected Bool value format: {raw_value}")
                 return BoolOrUnknown(value=None) # Treat as unknown

        elif value_type == "Float64":
            # Rust's OrderedFloat<f64> likely serializes as a standard float or string
            # Serde might serialize FloatOrUnknown::Float64(v) as {"Float64": v}
            actual_float = None
            if isinstance(raw_value, (float, int)):
                actual_float = float(raw_value)
            elif isinstance(raw_value, dict) and "Float64" in raw_value:
                 try:
                     actual_float = float(raw_value["Float64"])
                 except (ValueError, TypeError):
                      logging.warning(f"Could not convert Float64 value to float: {raw_value['Float64']}")
            else:
                 logging.warning(f"Unexpected Float64 value format: {raw_value}")

            return FloatOrUnknown(value=actual_float)


        elif value_type == "Int64":
             actual_int = None
             if isinstance(raw_value, int):
                 actual_int = raw_value
             elif isinstance(raw_value, dict) and "Int64" in raw_value:
                 try:
                     actual_int = int(raw_value["Int64"])
                 except (ValueError, TypeError):
                      logging.warning(f"Could not convert Int64 value to int: {raw_value['Int64']}")
             else:
                 logging.warning(f"Unexpected Int64 value format: {raw_value}")
             return IntOrUnknown(value=actual_int)

        elif value_type == "String":
            actual_string = None
            if isinstance(raw_value, str):
                actual_string = raw_value
            elif isinstance(raw_value, dict) and "String" in raw_value:
                actual_string = str(raw_value["String"])
            else:
                logging.warning(f"Unexpected String value format: {raw_value}")
            return StringOrUnknown(value=actual_string)

        elif value_type == "Time":
            # Rust SystemTime serialization can vary. Common formats:
            # 1. RFC 3339 string: "2023-10-27T10:00:00Z"
            # 2. Unix timestamp (seconds, maybe with fractions): 1666886400.123
            # 3. Serde might wrap it: {"Time": "..."} or {"Time": 123...}
            time_val = raw_value
            if isinstance(raw_value, dict) and "Time" in raw_value:
                time_val = raw_value["Time"] # Extract inner value if nested

            parsed_time: Optional[datetime] = None
            if isinstance(time_val, str):
                try:
                    # Handle potential 'Z' for UTC timezone explicitly
                    if time_val.endswith('Z'):
                        time_val = time_val[:-1] + '+00:00'
                    parsed_time = datetime.fromisoformat(time_val)
                    # If timezone naive, assume UTC (common for SystemTime)
                    if parsed_time.tzinfo is None:
                         parsed_time = parsed_time.replace(tzinfo=timezone.utc)
                except ValueError:
                    logging.warning(f"Could not parse time string: {time_val}")
            elif isinstance(time_val, (int, float)):
                try:
                    # Assume seconds since epoch (standard Unix timestamp)
                    parsed_time = datetime.fromtimestamp(time_val, tz=timezone.utc)
                except (ValueError, TypeError):
                     logging.warning(f"Could not parse time from timestamp: {time_val}")
            else:
                 logging.warning(f"Unexpected Time value format or type: {time_val}")

            return TimeOrUnknown(value=parsed_time)


        elif value_type == "Array":
             # Value should be a list (or {"Array": [...]})
             list_val = raw_value
             if isinstance(raw_value, dict) and "Array" in raw_value:
                 list_val = raw_value["Array"]

             deserialized_list: List[SPValueType] = []
             if isinstance(list_val, list):
                 for index, item in enumerate(list_val):
                     # Each item in the list is a serialized SPValue (likely as a dict)
                     # Need to re-serialize it to JSON string for deserialize_spvalue,
                     # OR modify deserialize_spvalue to accept dicts (less clean)
                     try:
                        item_json = json.dumps(item)
                        deserialized_item = deserialize_spvalue(item_json)
                        if deserialized_item is not None:
                             deserialized_list.append(deserialized_item)
                        else:
                             logging.warning(f"Failed to deserialize item at index {index} in array: {item}")
                     except TypeError as e:
                         logging.error(f"Error serializing array item back to JSON: {item}, error: {e}")
                     except Exception as e:
                         logging.error(f"Unexpected error deserializing array item: {item}, error: {e}")

                 return ArrayOrUnknown(value=deserialized_list)
             else:
                logging.warning(f"Unexpected Array value format: {raw_value}. Expected a list.")
                return ArrayOrUnknown(value=None) # Treat as Unknown

        else:
            logging.warning(f"Unknown SPValue type encountered during deserialization: {value_type}")
            return None # Or represent as a generic unknown type

    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON string: '{json_str}'. Error: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during deserialization of '{json_str}': {e}")
        return None

# --- Data Reading Function ---
def read_all_data() -> Tuple[Dict[str, Optional[SPValueType]], List[str]]:
    """Reads all keys from Redis and deserializes their SPValue data."""
    data: Dict[str, Optional[SPValueType]] = {}
    all_keys: List[str] = []
    try:
        all_keys = sorted(r.keys('*')) # Use scan_iter for large databases
        if not all_keys:
            logging.info("No keys found in Redis.")
            return {}, []

        for key in all_keys:
            try:
                value_str = r.get(key)
                if value_str:
                    deserialized_value = deserialize_spvalue(value_str)
                    data[key] = deserialized_value
                    if deserialized_value is None:
                         logging.warning(f"Deserialization returned None for key '{key}'. Value string: '{value_str[:100]}...'") # Log snippet
                else:
                    logging.warning(f"Key '{key}' exists but has no value (or failed to retrieve).")
                    data[key] = None # Represent missing value explicitly
            except redis.RedisError as e:
                 logging.error(f"Redis error reading key '{key}': {e}")
                 data[key] = None # Mark as failed
            except Exception as e:
                 logging.error(f"Unexpected error processing key '{key}': {e}")
                 data[key] = None # Mark as failed


    except redis.RedisError as e:
        logging.error(f"Redis error retrieving keys: {e}")
        st.error(f"Error retrieving keys from Redis: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during data reading: {e}")
        st.error(f"An unexpected error occurred reading data: {e}")

    # Log a summary of what was read
    logging.info(f"Read {len(data)} values out of {len(all_keys)} keys found.")
    successful_reads = sum(1 for v in data.values() if v is not None)
    logging.info(f"Successfully deserialized {successful_reads} values.")


    return data, all_keys


# --- Tab 1: State Viewer ---
def state_viewer(data: Dict[str, Optional[SPValueType]]) -> None:
    """Displays the current state read from Redis in a table."""
    st.header("Current State from Redis")

    if not data:
        st.warning("No data found or retrieved from Redis.")
        return

    display_data = []
    for key, value_obj in data.items():
        type_str = "Error/Unknown Deserialization"
        value_str = "N/A"

        if value_obj is None:
            value_str = "Failed to deserialize or missing value"
            type_str = "Unknown"
        elif isinstance(value_obj, BoolOrUnknown):
            type_str = "Bool"
            value_str = str(value_obj.value) if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, FloatOrUnknown):
            type_str = "Float64"
            value_str = str(value_obj.value) if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, IntOrUnknown):
            type_str = "Int64"
            value_str = str(value_obj.value) if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, StringOrUnknown):
            type_str = "String"
            value_str = f'"{value_obj.value}"' if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, TimeOrUnknown):
            type_str = "Time"
            value_str = value_obj.value.isoformat() if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, ArrayOrUnknown):
            type_str = "Array"
            if value_obj.value is None:
                 value_str = "UNKNOWN"
            else:
                 # Simple representation for array elements for brevity in table
                 value_str = f"[{len(value_obj.value)} items]"
                 # Could make this expandable later if needed
        else:
             # Should not happen with SPValueType union, but good fallback
             type_str = "Unexpected Type"
             value_str = str(value_obj)


        display_data.append({"Key": key, "Type": type_str, "Value": value_str})

    st.dataframe(display_data, use_container_width=True)

    # Optionally add a section for detailed view of arrays or complex data
    st.subheader("Detailed View (Example for Arrays)")
    for key, value_obj in data.items():
         if isinstance(value_obj, ArrayOrUnknown) and value_obj.value is not None:
              with st.expander(f"Details for Key: {key} (Array)"):
                   st.json(value_obj.value) # Display raw array content as JSON


# --- Tab 2: State Setter ---
def state_setter(data: Dict[str, Optional[SPValueType]], all_keys: List[str]) -> None:
    """Placeholder for UI elements to set state in Redis."""
    st.header("Set State in Redis (Placeholder)")

    if not all_keys:
        st.warning("No keys found in Redis to potentially modify.")
        return

    selected_key = st.selectbox("Select Key to Modify/Set:", options=[""] + all_keys)

    if selected_key:
        st.write(f"Current Value for '{selected_key}':")
        current_value = data.get(selected_key)
        if current_value:
             # Display the Python object representation
             st.write(current_value)
             # Display the raw JSON string from Redis for comparison/debugging
             try:
                  raw_json = r.get(selected_key)
                  st.text_area("Raw JSON in Redis:", value=raw_json, height=100, disabled=True)
             except Exception as e:
                  st.error(f"Could not retrieve raw JSON for {selected_key}: {e}")
        else:
             st.write("No current value or failed to deserialize.")


        st.warning("Note: Setting state functionality is not implemented in this example.")
        st.write("To implement this, you would need:")
        st.markdown("""
        1.  Input fields corresponding to the SPValue types (Bool, Float, Int, String, Time, Array).
        2.  Logic to construct the correct Python `XOrUnknown` object based on user input.
        3.  Serialization logic to convert the Python object back into the expected JSON format (`{"type": ..., "value": ...}`).
        4.  A button to trigger the `r.set(selected_key, serialized_json_string)` command.
        5.  Appropriate error handling and user feedback.
        """)
    else:
        st.info("Select a key to see its current value.")


# --- Main Application ---
def main() -> None:
    """Runs the Streamlit application."""
    st.set_page_config(layout="wide")
    st.title("Redis SPValue Viewer/Setter (micro_sp_ui)")

    # Add a button to refresh data manually
    if st.button("Refresh Data from Redis"):
        st.cache_data.clear() # Clear Streamlit's data cache if used
        st.rerun() # Rerun the script to fetch fresh data

    # Read data - consider caching if Redis calls are slow and data doesn't change extremely rapidly
    # Use st.cache_data for caching
    # @st.cache_data(ttl=60) # Cache for 60 seconds
    # def cached_read_all_data():
    #     return read_all_data()
    # data, all_keys = cached_read_all_data()

    # Without caching for simplicity now:
    data, all_keys = read_all_data()

    logging.info(f"Data read for UI: {len(data)} items.") # Use info level for normal operation logs

    tab1, tab2 = st.tabs(["View State", "Set State"])

    with tab1:
        state_viewer(data)
    with tab2:
        state_setter(data, all_keys) # Pass current data and keys

if __name__ == "__main__":
    main()