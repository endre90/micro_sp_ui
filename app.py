import streamlit as st
import redis
import json
import logging
import sys
from typing import List, Dict, Optional, Union, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone, date, time

# --- Configuration ---
REDIS_HOST: str = "redis"
REDIS_PORT: int = 6379

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr,
)

# --- Python Equivalents for Rust SPValue ---
# (Keep the existing dataclass definitions: BoolOrUnknown, FloatOrUnknown, etc.)
@dataclass
class BoolOrUnknown:
    value: Optional[bool] = None

@dataclass
class FloatOrUnknown:
    value: Optional[float] = None

@dataclass
class IntOrUnknown:
    value: Optional[int] = None

@dataclass
class StringOrUnknown:
    value: Optional[str] = None

@dataclass
class TimeOrUnknown:
    value: Optional[datetime] = None

@dataclass
class ArrayOrUnknown:
    value: Optional[List['SPValueType']] = field(default_factory=list)

SPValueType = Union[
    BoolOrUnknown,
    FloatOrUnknown,
    IntOrUnknown,
    StringOrUnknown,
    TimeOrUnknown,
    ArrayOrUnknown,
]

# --- Redis Connection ---
# (Keep the existing Redis connection logic)
try:
    r: redis.Redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    # Removed connection log from here, will log inside read_all_data on first successful read attempt
except redis.exceptions.ConnectionError as e:
    # Log critical connection error and exit
    logging.critical(f"CRITICAL: Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {e}")
    st.error(f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}. Please ensure Redis is running and accessible. Error: {e}")
    sys.exit(1) # Stop the app if Redis isn't available
except Exception as e:
    logging.critical(f"CRITICAL: An unexpected error occurred during Redis connection: {e}")
    st.error(f"An unexpected error occurred during Redis connection: {e}")
    sys.exit(1)


# --- Deserialization Function ---
# (Keep the existing deserialize_spvalue function)
def deserialize_spvalue(json_str: str) -> Optional[SPValueType]:
    # (No changes needed in this function)
    try:
        data: Dict[str, Any] = json.loads(json_str)
        value_type = data.get("type")
        raw_value = data.get("value")

        # --- Handle UNKNOWN ---
        if raw_value == "UNKNOWN":
            type_map_unknown = {
                "Bool": BoolOrUnknown, "Float64": FloatOrUnknown, "Int64": IntOrUnknown,
                "String": StringOrUnknown, "Time": TimeOrUnknown, "Array": ArrayOrUnknown,
            }
            cls = type_map_unknown.get(value_type)
            if cls: return cls(value=None)
            else: logging.warning(f"Unknown SPValue type for UNKNOWN value: {value_type}"); return None

        # --- Handle Actual Values ---
        if value_type == "Bool":
            val = raw_value if isinstance(raw_value, bool) else (raw_value.get("Bool") if isinstance(raw_value, dict) else None)
            return BoolOrUnknown(value=bool(val) if val is not None else None)
        elif value_type == "Float64":
            val = raw_value if isinstance(raw_value, (float, int)) else (raw_value.get("Float64") if isinstance(raw_value, dict) else None)
            try: return FloatOrUnknown(value=float(val) if val is not None else None)
            except (ValueError, TypeError): logging.warning(f"Could not convert Float64 value to float: {val}"); return FloatOrUnknown(value=None)
        elif value_type == "Int64":
            val = raw_value if isinstance(raw_value, int) else (raw_value.get("Int64") if isinstance(raw_value, dict) else None)
            try: return IntOrUnknown(value=int(val) if val is not None else None)
            except (ValueError, TypeError): logging.warning(f"Could not convert Int64 value to int: {val}"); return IntOrUnknown(value=None)
        elif value_type == "String":
            val = raw_value if isinstance(raw_value, str) else (raw_value.get("String") if isinstance(raw_value, dict) else None)
            return StringOrUnknown(value=str(val) if val is not None else None)
        elif value_type == "Time":
            time_val = raw_value if not isinstance(raw_value, dict) else raw_value.get("Time")
            parsed_time: Optional[datetime] = None
            if isinstance(time_val, str):
                try:
                    if time_val.endswith('Z'): time_val = time_val[:-1] + '+00:00'
                    parsed_time = datetime.fromisoformat(time_val)
                    if parsed_time.tzinfo is None: parsed_time = parsed_time.replace(tzinfo=timezone.utc)
                except ValueError: logging.warning(f"Could not parse time string: {time_val}")
            elif isinstance(time_val, (int, float)):
                try: parsed_time = datetime.fromtimestamp(time_val, tz=timezone.utc)
                except (ValueError, TypeError): logging.warning(f"Could not parse time from timestamp: {time_val}")
            else: logging.warning(f"Unexpected Time value format: {time_val}")
            return TimeOrUnknown(value=parsed_time)
        elif value_type == "Array":
            list_val = raw_value if isinstance(raw_value, list) else (raw_value.get("Array") if isinstance(raw_value, dict) else None)
            if isinstance(list_val, list):
                deserialized_list: List[SPValueType] = []
                for item in list_val:
                    try:
                        item_json = json.dumps(item); deserialized_item = deserialize_spvalue(item_json)
                        if deserialized_item: deserialized_list.append(deserialized_item)
                    except Exception as e: logging.error(f"Error deserializing array item: {item}, error: {e}")
                return ArrayOrUnknown(value=deserialized_list)
            else: logging.warning(f"Array value was not a list: {raw_value}"); return ArrayOrUnknown(value=None)
        else: logging.warning(f"Unknown SPValue type encountered: {value_type}"); return None
    except json.JSONDecodeError as e: logging.error(f"Failed to decode JSON: '{json_str}'. Error: {e}"); return None
    except Exception as e: logging.error(f"Error during deserialization of '{json_str}': {e}", exc_info=True); return None


# --- Serialization Function ---
# (Keep the existing serialize_spvalue function)
def serialize_spvalue(value_obj: SPValueType) -> Optional[str]:
    # (No changes needed in this function)
    data = {}
    raw_value: Any = "UNKNOWN" # Default to UNKNOWN
    try:
        if isinstance(value_obj, BoolOrUnknown): data["type"] = "Bool"; raw_value = value_obj.value if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, FloatOrUnknown): data["type"] = "Float64"; raw_value = value_obj.value if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, IntOrUnknown): data["type"] = "Int64"; raw_value = value_obj.value if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, StringOrUnknown): data["type"] = "String"; raw_value = value_obj.value if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, TimeOrUnknown):
            data["type"] = "Time"
            if value_obj.value is not None: raw_value = value_obj.value.isoformat().replace('+00:00', 'Z')
            else: raw_value = "UNKNOWN"
        elif isinstance(value_obj, ArrayOrUnknown):
            data["type"] = "Array"
            if value_obj.value is not None:
                 serialized_items = []
                 for item in value_obj.value:
                      item_str = serialize_spvalue(item)
                      if item_str:
                           try: serialized_items.append(json.loads(item_str))
                           except json.JSONDecodeError: logging.error(f"Failed to re-parse serialized array item: {item_str}"); return None
                      else: logging.error(f"Failed to serialize an array item: {item}"); return None
                 raw_value = serialized_items
            else: raw_value = "UNKNOWN"
        else: logging.error(f"Cannot serialize unknown SPValueType: {type(value_obj)}"); return None

        data["value"] = raw_value
        return json.dumps(data)
    except Exception as e: logging.error(f"Error during serialization of {value_obj}: {e}", exc_info=True); return None

# --- Parsing Input String to SPValueType ---
# (Keep the existing parse_input_to_spvalue function)
def parse_input_to_spvalue(input_str: str, target_type: Optional[type] = None) -> Optional[SPValueType]:
    # (No changes needed in this function)
    input_str = input_str.strip()
    if input_str.upper() == "UNKNOWN":
        if target_type: return target_type(value=None)
        logging.warning("Cannot parse 'UNKNOWN' without target type context."); return None

    if target_type == BoolOrUnknown:
        low_str = input_str.lower()
        if low_str in ["true", "t", "yes", "y", "1"]: return BoolOrUnknown(value=True)
        if low_str in ["false", "f", "no", "n", "0"]: return BoolOrUnknown(value=False)
        logging.warning(f"Could not parse '{input_str}' as Bool."); return None
    if target_type == IntOrUnknown:
        try: return IntOrUnknown(value=int(input_str))
        except ValueError: logging.warning(f"Could not parse '{input_str}' as Int."); return None
    if target_type == FloatOrUnknown:
        try: return FloatOrUnknown(value=float(input_str))
        except ValueError: logging.warning(f"Could not parse '{input_str}' as Float."); return None
    if target_type == TimeOrUnknown:
        try:
            dt_val = input_str
            if dt_val.endswith('Z'): dt_val = dt_val[:-1] + '+00:00'
            parsed_dt = datetime.fromisoformat(dt_val)
            if parsed_dt.tzinfo is None: parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            return TimeOrUnknown(value=parsed_dt)
        except ValueError: logging.warning(f"Could not parse '{input_str}' as ISO DateTime."); return None
    if target_type == ArrayOrUnknown:
        logging.warning("Setting Arrays via text input is not supported."); return None
    if target_type is None or target_type == StringOrUnknown:
        return StringOrUnknown(value=input_str)
    logging.error(f"Unhandled parsing case for input '{input_str}' with target type {target_type}"); return None

# --- Data Reading Function ---
# Use @st.cache_data for better performance
# @st.cache_data(ttl=30) # Cache for 30 seconds
def read_all_data(_dummy_param=None) -> Tuple[Dict[str, Optional[SPValueType]], List[str]]:
    """Reads all keys from Redis and deserializes their SPValue data."""
    # This function now runs only when cache is empty/expired
    logging.info(f"Attempting to read data from Redis at {REDIS_HOST}:{REDIS_PORT}...")
    data: Dict[str, Optional[SPValueType]] = {}
    all_keys: List[str] = []
    try:
        all_keys = sorted([key for key in r.scan_iter('*')]) # scan_iter is efficient
        if not all_keys:
            logging.warning("No keys found in Redis.") # Changed to warning
            return {}, []

        values_str = r.mget(all_keys) # Efficiently get all values

        successful_reads = 0
        for key, value_str in zip(all_keys, values_str):
            if value_str is not None:
                deserialized_value = deserialize_spvalue(value_str)
                data[key] = deserialized_value
                if deserialized_value is not None:
                    successful_reads += 1
                else:
                    logging.warning(f"Deserialization returned None for key '{key}'. Value string: '{value_str[:100]}...'")
            else:
                logging.warning(f"MGET returned None for key '{key}' which was found by SCAN (might have been deleted between scan and mget).")
                data[key] = None # Treat as missing/error

        # **** Log results INSIDE the cached function ****
        logging.info(f"Read {len(values_str)} values for {len(all_keys)} keys found.")
        logging.info(f"Successfully deserialized {successful_reads} values.")
        # ************************************************

    except redis.RedisError as e:
        logging.error(f"Redis error retrieving data: {e}")
        st.error(f"Error retrieving data from Redis: {e}") # Show error in UI too
        # Return empty data on error to prevent crash, but log indicates failure
        return {}, []
    except Exception as e:
        logging.error(f"An unexpected error occurred during data reading: {e}", exc_info=True)
        st.error(f"An unexpected error occurred reading data: {e}")
        return {}, []

    return data, all_keys

# --- Tab 1: State Viewer ---
# (Keep the existing state_viewer function, no changes needed)
def state_viewer(data: Dict[str, Optional[SPValueType]]) -> None:
    # ... (function content remains the same) ...
    st.header("Current State from Redis")
    if not data: st.warning("No data found or retrieved from Redis."); return
    display_data = []
    for key, value_obj in data.items():
        type_str, value_str = "Error/Unknown", "N/A"
        if value_obj is None: value_str = "Failed to deserialize or missing value"
        elif isinstance(value_obj, BoolOrUnknown): type_str, value_str = "Bool", str(value_obj.value) if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, FloatOrUnknown): type_str, value_str = "Float64", str(value_obj.value) if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, IntOrUnknown): type_str, value_str = "Int64", str(value_obj.value) if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, StringOrUnknown): type_str, value_str = "String", f'"{value_obj.value}"' if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, TimeOrUnknown): type_str, value_str = "Time", value_obj.value.isoformat() if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, ArrayOrUnknown): type_str, value_str = "Array", f"[{len(value_obj.value)} items]" if value_obj.value is not None else "UNKNOWN"
        else: type_str = f"Unexpected ({type(value_obj)})"
        display_data.append({"Key": key, "Type": type_str, "Value": value_str})
    st.dataframe(display_data, use_container_width=True)


# --- Tab 2: State Setter Implementation ---
def state_setter_impl(current_data: Dict[str, Optional[SPValueType]], all_keys: List[str]) -> None:
    """Tab for setting multiple variable values in Redis."""
    st.header("Set Multiple State Variables")
    st.caption("Select variables, enter new values, and click 'Set Selected Values'. "
               "Enter 'UNKNOWN' to set a variable to the unknown state. "
               "Parsing attempts type inference based on current value, defaulting to String.")
    st.warning("Setting Array values via this text input is not currently supported.")

    if 'items_to_set' not in st.session_state:
        st.session_state.items_to_set = [{"id": 0, "key": "", "value_str": ""}]

    items = st.session_state.items_to_set
    keys_with_blank = [""] + all_keys

    for i, item in enumerate(items):
        cols = st.columns([3, 3, 1])
        with cols[0]:
            selected_key = st.selectbox(
                "Variable Key", keys_with_blank,
                index=keys_with_blank.index(item["key"]) if item["key"] in keys_with_blank else 0,
                key=f"select_{item['id']}"
            )
            items[i]["key"] = selected_key
            if selected_key and selected_key in current_data:
                current_val_obj = current_data[selected_key]
                if current_val_obj:
                    type_name = type(current_val_obj).__name__
                    current_val_str = getattr(current_val_obj, 'value', 'ERROR')
                    if current_val_str is None: current_val_str = "UNKNOWN"
                    st.caption(f"Current: {type_name} = {current_val_str}")
                else: st.caption("Current value unknown/failed.")
            elif selected_key: st.caption("New key (will be String).")

        with cols[1]:
            input_value = st.text_input(
                "New Value (or 'UNKNOWN')", value=item["value_str"],
                key=f"input_{item['id']}"
            )
            items[i]["value_str"] = input_value

        with cols[2]:
             # CHANGE: Removed emoji, use text "Remove"
             if st.button("Remove", key=f"remove_{item['id']}", help="Remove this variable"):
                  st.session_state.items_to_set.pop(i)
                #   st.rerun()

    # CHANGE: Removed emoji, use text "Add Variable"
    if st.button("Add Variable"):
        new_id = max(item['id'] for item in items) + 1 if items else 0
        st.session_state.items_to_set.append({"id": new_id, "key": "", "value_str": ""})
        # st.rerun()

    st.divider()

    if st.button("Set Selected Values", type="primary"):
        success_count, error_count = 0, 0
        errors, keys_to_set = [], {}

        for item in st.session_state.items_to_set:
            key, value_str = item["key"], item["value_str"]
            if not key: continue
            current_value_obj = current_data.get(key)
            target_type = type(current_value_obj) if current_value_obj else StringOrUnknown
            parsed_spvalue = parse_input_to_spvalue(value_str, target_type)
            if parsed_spvalue is None:
                error_count += 1; errors.append(f"Key '{key}': Failed parse '{value_str}' for {target_type.__name__ if target_type else 'Unknown'}.")
                continue
            serialized_json = serialize_spvalue(parsed_spvalue)
            if serialized_json is None:
                error_count += 1; errors.append(f"Key '{key}': Failed serialize: {parsed_spvalue}")
                continue
            keys_to_set[key] = serialized_json

        if not keys_to_set and not errors:
            st.warning("No variables selected for setting.")
        elif keys_to_set:
            try:
                pipe = r.pipeline()
                for key, json_val in keys_to_set.items(): pipe.set(key, json_val)
                results = pipe.execute()
                failed_sets = []
                for i, (key, result) in enumerate(zip(keys_to_set.keys(), results)):
                    if result: success_count += 1
                    else: error_count += 1; failed_sets.append(key); errors.append(f"Key '{key}': Redis SET failed.")
                if success_count > 0: st.success(f"Successfully set {success_count} variable(s).")
                if failed_sets: st.error(f"Failed SET for {len(failed_sets)} variable(s): {', '.join(failed_sets)}")
                if errors and not failed_sets: # Show prior errors if sets didn't fail but parsing/serialization did
                     for error in errors: st.error(error)
                # st.cache_data.clear(); 
                # st.rerun() # Refresh on success or partial success
            except redis.RedisError as e: st.error(f"Redis error during SET: {e}"); logging.error(f"Redis SET error: {e}", exc_info=True)
            except Exception as e: st.error(f"Unexpected error during SET: {e}"); logging.error(f"Unexpected SET error: {e}", exc_info=True)
        # Display only parsing/serialization errors if nothing was attempted to be set
        elif errors:
             st.error(f"Found {error_count} error(s) preventing setting:")
             for error in errors: st.error(error)


# --- Tab 3: State Details Viewer ---
def state_details(data: Dict[str, Optional[SPValueType]], all_keys: List[str]) -> None:
    """Displays detailed information about a selected state variable."""
    st.header("View Variable Details")
    if not all_keys: st.warning("No keys found in Redis."); return

    keys_with_blank = [""] + all_keys
    selected_key = st.selectbox("Select Key to View Details:", options=keys_with_blank, key="details_select") # Added key

    if selected_key:
        st.subheader(f"Details for: `{selected_key}`")
        current_value_obj = data.get(selected_key)
        if current_value_obj:
            st.write("**Parsed Value (Python Object):**")
            st.write(current_value_obj)
            st.write("**Raw JSON String in Redis:**")
            try:
                raw_json = r.get(selected_key)
                # CHANGE: Added proper label and collapsed visibility
                st.text_area(
                    "Raw JSON Value", # Meaningful label
                    value=raw_json or "Key exists but value is empty/null in Redis",
                    height=150,
                    disabled=True,
                    key=f"raw_json_{selected_key}",
                    label_visibility="collapsed" # Hide the label visually
                 )
            except Exception as e: st.error(f"Error retrieving raw JSON for '{selected_key}': {e}")
            if isinstance(current_value_obj, ArrayOrUnknown) and current_value_obj.value is not None:
                st.write("**Array Content (Parsed):**")
                if current_value_obj.value:
                    for i, item in enumerate(current_value_obj.value): st.text(f"  [{i}]: {item}")
                else: st.text("  (Empty Array)")
        elif selected_key in data:
             st.warning("Could not deserialize value in Redis for this key.")
             # CHANGE: Added proper label and collapsed visibility here too
             try:
                raw_json = r.get(selected_key)
                st.text_area("Raw JSON Value", value=raw_json or "Key exists but value is empty/null in Redis", height=150, disabled=True, key=f"raw_json_fail_{selected_key}", label_visibility="collapsed")
             except Exception as e: st.error(f"Error retrieving raw JSON for '{selected_key}': {e}")
        else: st.error(f"Key '{selected_key}' not found (may have been deleted).")
    else: st.info("Select a key to view details.")


# --- Main Application ---
def main() -> None:
    """Runs the Streamlit application."""
    st.set_page_config(layout="wide")
    st.title("micro_sp_ui") # CHANGE: Removed emoji

    # CHANGE: Removed emoji
    if st.button("Refresh Data from Redis"):
        st.cache_data.clear()
        st.rerun()

    current_time_for_cache = datetime.now().timestamp()
    data, all_keys = read_all_data(current_time_for_cache)

    # Log only once per script run after data is attempted read
    logging.debug(f"UI data refreshed/retrieved. Keys: {len(all_keys)}, Deserialized items: {len(data)}") # Use debug level

    # CHANGE: Removed emojis from tab titles
    tab1, tab2, tab3 = st.tabs(["View State", "Set State", "Details"])

    with tab1: state_viewer(data)
    with tab2: state_setter_impl(data, all_keys)
    with tab3: state_details(data, all_keys)

if __name__ == "__main__":
    main()