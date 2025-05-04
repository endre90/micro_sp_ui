import streamlit as st
import redis
import json
import logging
import sys
from typing import List, Dict, Optional, Union, Any, Tuple # Added Tuple
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

# Forward declaration for recursive types like Array and Map
SPValueType = Any

@dataclass
class ArrayOrUnknown:
    value: Optional[List[SPValueType]] = field(default_factory=list)

# --- NEW: Dataclasses for Transform ---
@dataclass
class SPTranslation:
    x: Optional[float] = None
    y: Optional[float] = None
    z: Optional[float] = None

@dataclass
class SPRotation:
    x: Optional[float] = None
    y: Optional[float] = None
    z: Optional[float] = None
    w: Optional[float] = None

@dataclass
class SPTransform:
    translation: Optional[SPTranslation] = None
    rotation: Optional[SPRotation] = None

@dataclass
class SPTransformStamped:
    active: Optional[bool] = None
    time_stamp: Optional[datetime] = None
    parent_frame_id: Optional[str] = None
    child_frame_id: Optional[str] = None
    transform: Optional[SPTransform] = None
    metadata: Optional['MapOrUnknown'] = None # Use forward reference

@dataclass
class TransformOrUnknown:
    value: Optional[SPTransformStamped] = None

# --- NEW: Dataclass for Map ---
# Note: Map keys and values can be any SPValueType themselves
@dataclass
class MapOrUnknown:
    value: Optional[List[Tuple[SPValueType, SPValueType]]] = field(default_factory=list)


# --- Update SPValueType Union ---
SPValueType = Union[
    BoolOrUnknown,
    FloatOrUnknown,
    IntOrUnknown,
    StringOrUnknown,
    TimeOrUnknown,
    ArrayOrUnknown,
    MapOrUnknown,         # Added
    TransformOrUnknown,   # Added
]

# --- Redis Connection ---
try:
    r: redis.Redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
except redis.exceptions.ConnectionError as e:
    logging.critical(f"CRITICAL: Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {e}")
    st.error(f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}. Please ensure Redis is running and accessible. Error: {e}")
    sys.exit(1)
except Exception as e:
    logging.critical(f"CRITICAL: An unexpected error occurred during Redis connection: {e}")
    st.error(f"An unexpected error occurred during Redis connection: {e}")
    sys.exit(1)


# Add near the top with other dataclasses/types
SIMPLE_TYPE_NAMES = ["String", "Int64", "Float64", "Bool", "Time"]
TYPE_NAME_MAP = {
    "String": StringOrUnknown,
    "Int64": IntOrUnknown,
    "Float64": FloatOrUnknown,
    "Bool": BoolOrUnknown,
    "Time": TimeOrUnknown,
    # We handle UNKNOWN separately usually via text input 'UNKNOWN'
}

def get_target_type_class(type_name: str) -> Optional[type]:
    """Maps a simple type name string to its corresponding class."""
    return TYPE_NAME_MAP.get(type_name)

# --- Deserialization Function ---
def deserialize_spvalue(json_str: str) -> Optional[SPValueType]:
    try:
        data: Dict[str, Any] = json.loads(json_str)
        value_type = data.get("type")
        raw_value = data.get("value") # This can be "UNKNOWN" or a nested dict {"Type": actual_value}

        # --- Handle UNKNOWN ---
        if raw_value == "UNKNOWN":
            type_map_unknown = {
                "Bool": BoolOrUnknown, "Float64": FloatOrUnknown, "Int64": IntOrUnknown,
                "String": StringOrUnknown, "Time": TimeOrUnknown, "Array": ArrayOrUnknown,
                "Map": MapOrUnknown, "Transform": TransformOrUnknown, # Added
            }
            cls = type_map_unknown.get(value_type)
            if cls: return cls(value=None)
            else: logging.warning(f"Unknown SPValue type for UNKNOWN value: {value_type}"); return None

        # --- Handle Actual Values ---
        nested_value_dict = raw_value if isinstance(raw_value, dict) else {}

        if value_type == "Bool":
            val = nested_value_dict.get("Bool")
            return BoolOrUnknown(value=bool(val) if val is not None else None)
        elif value_type == "Float64":
            val = nested_value_dict.get("Float64")
            try: return FloatOrUnknown(value=float(val) if val is not None else None)
            except (ValueError, TypeError): logging.warning(f"Could not convert Float64 value to float: {val}"); return FloatOrUnknown(value=None)
        elif value_type == "Int64":
            val = nested_value_dict.get("Int64")
            try: return IntOrUnknown(value=int(val) if val is not None else None)
            except (ValueError, TypeError): logging.warning(f"Could not convert Int64 value to int: {val}"); return IntOrUnknown(value=None)
        elif value_type == "String":
            val = nested_value_dict.get("String")
            return StringOrUnknown(value=str(val) if val is not None else None)
        elif value_type == "Time":
            time_val = nested_value_dict.get("Time")
            parsed_time: Optional[datetime] = None
            if isinstance(time_val, str):
                try:
                    if time_val.endswith('Z'): time_val = time_val[:-1] + '+00:00'
                    parsed_time = datetime.fromisoformat(time_val)
                    if parsed_time.tzinfo is None: parsed_time = parsed_time.replace(tzinfo=timezone.utc)
                except ValueError: logging.warning(f"Could not parse time string: {time_val}")
            elif isinstance(time_val, (int, float)): # Less common for SPValue::Time, but possible
                try: parsed_time = datetime.fromtimestamp(time_val, tz=timezone.utc)
                except (ValueError, TypeError): logging.warning(f"Could not parse time from timestamp: {time_val}")
            else: logging.warning(f"Unexpected Time value format: {time_val}")
            return TimeOrUnknown(value=parsed_time)
        elif value_type == "Array":
            list_val = nested_value_dict.get("Array")
            if isinstance(list_val, list):
                deserialized_list: List[SPValueType] = []
                for item_dict in list_val: # Items should already be dicts like {"type": T, "value": V}
                    try:
                        # Recursively deserialize each item
                        item_json = json.dumps(item_dict) # Convert item dict back to json string for deserialize_spvalue
                        deserialized_item = deserialize_spvalue(item_json)
                        if deserialized_item: deserialized_list.append(deserialized_item)
                    except Exception as e: logging.error(f"Error deserializing array item: {item_dict}, error: {e}")
                return ArrayOrUnknown(value=deserialized_list)
            else: logging.warning(f"Array value was not a list: {raw_value}"); return ArrayOrUnknown(value=None)

        # --- NEW: Handle Map ---
        elif value_type == "Map":
            map_val_list = nested_value_dict.get("Map")
            if isinstance(map_val_list, list):
                deserialized_map_list: List[Tuple[SPValueType, SPValueType]] = []
                for pair in map_val_list:
                    if isinstance(pair, list) and len(pair) == 2:
                        key_dict, value_dict = pair[0], pair[1]
                        try:
                            # Recursively deserialize key and value
                            key_json = json.dumps(key_dict)
                            value_json = json.dumps(value_dict)
                            deserialized_key = deserialize_spvalue(key_json)
                            deserialized_value = deserialize_spvalue(value_json)
                            if deserialized_key is not None and deserialized_value is not None:
                                deserialized_map_list.append((deserialized_key, deserialized_value))
                            else:
                                logging.warning(f"Failed to deserialize key or value in Map pair: Key={key_json}, Value={value_json}")
                        except Exception as e:
                            logging.error(f"Error deserializing Map pair: {pair}, error: {e}")
                    else:
                        logging.warning(f"Invalid format for Map pair: {pair}. Expected a list of two elements.")
                return MapOrUnknown(value=deserialized_map_list)
            else:
                logging.warning(f"Map value was not a list: {raw_value}"); return MapOrUnknown(value=None)

        # --- NEW: Handle Transform ---
        elif value_type == "Transform":
            tf_stamped_dict = nested_value_dict.get("Transform")
            if isinstance(tf_stamped_dict, dict):
                try:
                    # Parse timestamp
                    ts_val = tf_stamped_dict.get("time_stamp")
                    parsed_ts: Optional[datetime] = None
                    if isinstance(ts_val, str):
                        try:
                            if ts_val.endswith('Z'): ts_val = ts_val[:-1] + '+00:00'
                            parsed_ts = datetime.fromisoformat(ts_val)
                            if parsed_ts.tzinfo is None: parsed_ts = parsed_ts.replace(tzinfo=timezone.utc)
                        except ValueError: logging.warning(f"Could not parse Transform timestamp string: {ts_val}")
                    elif isinstance(ts_val, (int, float)): # Handle numeric timestamps if they occur
                        try: parsed_ts = datetime.fromtimestamp(ts_val, tz=timezone.utc)
                        except (ValueError, TypeError): logging.warning(f"Could not parse Transform timestamp from number: {ts_val}")

                    # Parse nested Transform
                    tf_dict = tf_stamped_dict.get("transform", {})
                    trans_dict = tf_dict.get("translation", {})
                    rot_dict = tf_dict.get("rotation", {})

                    translation = SPTranslation(
                        x=float(trans_dict['x']) if 'x' in trans_dict else None,
                        y=float(trans_dict['y']) if 'y' in trans_dict else None,
                        z=float(trans_dict['z']) if 'z' in trans_dict else None,
                    )
                    rotation = SPRotation(
                        x=float(rot_dict['x']) if 'x' in rot_dict else None,
                        y=float(rot_dict['y']) if 'y' in rot_dict else None,
                        z=float(rot_dict['z']) if 'z' in rot_dict else None,
                        w=float(rot_dict['w']) if 'w' in rot_dict else None,
                    )
                    transform = SPTransform(translation=translation, rotation=rotation)

                    # Recursively parse metadata (which is MapOrUnknown)
                    metadata_dict = tf_stamped_dict.get("metadata")
                    deserialized_metadata = None
                    if isinstance(metadata_dict, dict): # Should be {"type":"Map", "value":...}
                       try:
                            metadata_json = json.dumps(metadata_dict)
                            deserialized_metadata = deserialize_spvalue(metadata_json)
                            if not isinstance(deserialized_metadata, MapOrUnknown):
                                logging.warning(f"Metadata field deserialized unexpectedly: {type(deserialized_metadata)}")
                                deserialized_metadata = MapOrUnknown(value=None) # Default to unknown map
                       except Exception as e:
                            logging.error(f"Error deserializing Transform metadata: {metadata_dict}, error: {e}")
                            deserialized_metadata = MapOrUnknown(value=None) # Default to unknown map
                    else:
                        # Handle case where metadata might be missing or not a dict
                         deserialized_metadata = MapOrUnknown(value=None)


                    stamped = SPTransformStamped(
                        active=bool(tf_stamped_dict.get("active")) if "active" in tf_stamped_dict else None,
                        time_stamp=parsed_ts,
                        parent_frame_id=str(tf_stamped_dict.get("parent_frame_id")) if "parent_frame_id" in tf_stamped_dict else None,
                        child_frame_id=str(tf_stamped_dict.get("child_frame_id")) if "child_frame_id" in tf_stamped_dict else None,
                        transform=transform,
                        metadata=deserialized_metadata # Assign the deserialized MapOrUnknown object
                    )
                    return TransformOrUnknown(value=stamped)
                except (ValueError, TypeError, KeyError) as e:
                    logging.error(f"Error parsing Transform structure: {tf_stamped_dict}, error: {e}", exc_info=True)
                    return TransformOrUnknown(value=None)
            else:
                logging.warning(f"Transform value was not a dictionary: {raw_value}"); return TransformOrUnknown(value=None)

        else:
            logging.warning(f"Unknown SPValue type encountered during deserialization: {value_type}"); return None
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON: '{json_str}'. Error: {e}"); return None
    except Exception as e:
        logging.error(f"Error during deserialization of '{json_str}': {e}", exc_info=True); return None

# --- Serialization Function ---
def serialize_spvalue(value_obj: SPValueType) -> Optional[str]:
    data = {}
    nested_value: Any

    try:
        if isinstance(value_obj, BoolOrUnknown):
            data["type"] = "Bool"
            nested_value = {"Bool": value_obj.value} if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, FloatOrUnknown):
            data["type"] = "Float64"
            nested_value = {"Float64": value_obj.value} if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, IntOrUnknown):
            data["type"] = "Int64"
            nested_value = {"Int64": value_obj.value} if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, StringOrUnknown):
            data["type"] = "String"
            nested_value = {"String": value_obj.value} if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, TimeOrUnknown):
            data["type"] = "Time"
            if value_obj.value is not None:
                iso_time = value_obj.value.isoformat().replace('+00:00', 'Z')
                nested_value = {"Time": iso_time}
            else:
                nested_value = "UNKNOWN"
        elif isinstance(value_obj, ArrayOrUnknown):
            data["type"] = "Array"
            if value_obj.value is not None:
                 serialized_items = []
                 for item in value_obj.value:
                      item_str = serialize_spvalue(item)
                      if item_str:
                           try: serialized_items.append(json.loads(item_str)) # Load string back to dict
                           except json.JSONDecodeError: logging.error(f"Failed to re-parse serialized array item: {item_str}"); return None
                      else: logging.error(f"Failed to serialize an array item: {item}"); return None
                 nested_value = {"Array": serialized_items}
            else:
                 nested_value = "UNKNOWN"

        # --- NEW: Serialize Map ---
        elif isinstance(value_obj, MapOrUnknown):
            data["type"] = "Map"
            if value_obj.value is not None:
                serialized_pairs = []
                for key_obj, value_obj_map in value_obj.value:
                    key_str = serialize_spvalue(key_obj)
                    value_str = serialize_spvalue(value_obj_map)
                    if key_str and value_str:
                        try:
                            # Load strings back to dicts for the final structure
                            parsed_key = json.loads(key_str)
                            parsed_value = json.loads(value_str)
                            serialized_pairs.append([parsed_key, parsed_value])
                        except json.JSONDecodeError:
                            logging.error(f"Failed to re-parse serialized map key/value: K='{key_str}', V='{value_str}'")
                            return None
                    else:
                        logging.error(f"Failed to serialize a map key or value: K={key_obj}, V={value_obj_map}")
                        return None
                nested_value = {"Map": serialized_pairs}
            else:
                nested_value = "UNKNOWN"

        # --- NEW: Serialize Transform ---
        elif isinstance(value_obj, TransformOrUnknown):
            data["type"] = "Transform"
            stamped_obj = value_obj.value
            if stamped_obj is not None:
                stamped_dict = {}
                if stamped_obj.active is not None: stamped_dict["active"] = stamped_obj.active
                if stamped_obj.time_stamp is not None:
                    stamped_dict["time_stamp"] = stamped_obj.time_stamp.isoformat().replace('+00:00', 'Z')
                if stamped_obj.parent_frame_id is not None: stamped_dict["parent_frame_id"] = stamped_obj.parent_frame_id
                if stamped_obj.child_frame_id is not None: stamped_dict["child_frame_id"] = stamped_obj.child_frame_id

                # Serialize Transform
                if stamped_obj.transform and stamped_obj.transform.translation and stamped_obj.transform.rotation:
                    trans = stamped_obj.transform.translation
                    rot = stamped_obj.transform.rotation
                    stamped_dict["transform"] = {
                        "translation": {
                            "x": trans.x if trans.x is not None else None,
                            "y": trans.y if trans.y is not None else None,
                            "z": trans.z if trans.z is not None else None,
                        },
                        "rotation": {
                            "x": rot.x if rot.x is not None else None,
                            "y": rot.y if rot.y is not None else None,
                            "z": rot.z if rot.z is not None else None,
                            "w": rot.w if rot.w is not None else None,
                        }
                    }
                else:
                    # Include empty transform if parts are missing? Or omit? Let's omit for cleaner JSON
                    pass # stamped_dict["transform"] = {}

                # Serialize Metadata (MapOrUnknown)
                metadata_obj = stamped_obj.metadata
                if metadata_obj is not None:
                     metadata_str = serialize_spvalue(metadata_obj)
                     if metadata_str:
                         try:
                             # Parse serialized metadata back into object
                             stamped_dict["metadata"] = json.loads(metadata_str)
                         except json.JSONDecodeError:
                             logging.error(f"Failed to re-parse serialized Transform metadata: {metadata_str}")
                             return None
                     else:
                         # Handle case where metadata exists but fails serialization
                         logging.error(f"Failed to serialize Transform metadata: {metadata_obj}")
                         # Optionally set to known-bad value, or fail completely
                         stamped_dict["metadata"] = {"type": "Map", "value": "UNKNOWN"} # Indicate error state

                else:
                     # If metadata field is None in the object, represent it as UNKNOWN Map
                     stamped_dict["metadata"] = {"type": "Map", "value": "UNKNOWN"}


                nested_value = {"Transform": stamped_dict}
            else:
                nested_value = "UNKNOWN"

        else:
            logging.error(f"Cannot serialize unknown SPValueType: {type(value_obj)}")
            return None

        data["value"] = nested_value
        return json.dumps(data, separators=(',', ':'))

    except Exception as e:
        logging.error(f"Error during serialization of {value_obj}: {e}", exc_info=True)
        return None

# --- Parsing Input String to SPValueType ---
def parse_input_to_spvalue(input_str: str, target_type: Optional[type] = None) -> Optional[SPValueType]:
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
    # --- NEW: Disallow complex types from text input ---
    if target_type == ArrayOrUnknown:
        logging.warning("Setting Arrays via text input is not supported."); return None
    if target_type == MapOrUnknown:
        logging.warning("Setting Maps via text input is not supported."); return None
    if target_type == TransformOrUnknown:
        logging.warning("Setting Transforms via text input is not supported."); return None
    # --- End New ---
    if target_type is None or target_type == StringOrUnknown: # Default to string
        return StringOrUnknown(value=input_str)

    logging.error(f"Unhandled parsing case for input '{input_str}' with target type {target_type}"); return None


# --- Data Reading Function ---
# @st.cache_data(ttl=30) # Cache for 30 seconds - Keep commented out until needed
def read_all_data(_dummy_param=None) -> Tuple[Dict[str, Optional[SPValueType]], List[str]]:
    """Reads all keys from Redis and deserializes their SPValue data."""
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

        logging.info(f"Read {len(values_str)} values for {len(all_keys)} keys found.")
        logging.info(f"Successfully deserialized {successful_reads} values.")

    except redis.RedisError as e:
        logging.error(f"Redis error retrieving data: {e}")
        st.error(f"Error retrieving data from Redis: {e}") # Show error in UI too
        return {}, []
    except Exception as e:
        logging.error(f"An unexpected error occurred during data reading: {e}", exc_info=True)
        st.error(f"An unexpected error occurred reading data: {e}")
        return {}, []

    return data, all_keys


# --- Tab 1: State Viewer ---
def state_viewer(data: Dict[str, Optional[SPValueType]]) -> None:
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
        # --- NEW: Display for Map and Transform ---
        elif isinstance(value_obj, MapOrUnknown):
             type_str = "Map"
             value_str = f"[{len(value_obj.value)} pairs]" if value_obj.value is not None else "UNKNOWN"
        elif isinstance(value_obj, TransformOrUnknown):
             type_str = "Transform"
             tf_stamped = value_obj.value
             if tf_stamped is not None:
                  value_str = f"{tf_stamped.parent_frame_id or '?'} -> {tf_stamped.child_frame_id or '?'}"
                  value_str += f" (Active: {tf_stamped.active})" if tf_stamped.active is not None else ""
             else: value_str = "UNKNOWN"
        # --- End New ---
        else: type_str = f"Unexpected ({type(value_obj)})"
        display_data.append({"Key": key, "Type": type_str, "Value": value_str})
    st.dataframe(display_data, use_container_width=True)

# --- Tab 2: State Setter Implementation ---
def state_setter_impl(current_data: Dict[str, Optional[SPValueType]], all_keys: List[str]) -> None:
    """Tab for setting multiple variable values in Redis with interactive Map/Array editing."""
    st.header("Set Multiple State Variables")
    st.caption("Select variables. For simple types, enter the value. For Maps/Arrays, use the 'Add' buttons. Enter 'UNKNOWN' in value fields to set that specific item/key/value to unknown.")
    # Keep JSON as fallback? Maybe remove for clarity if interactive is primary.
    # st.info("Advanced: For complex nested Maps/Arrays, you can still type the raw JSON list of pairs/items if needed.")
    st.warning("Interactive editing currently supports simple types (String, Int, Float, Bool, Time) within Maps/Arrays.")


    # Initialize session state structure if needed
    if 'items_to_set' not in st.session_state:
        st.session_state.items_to_set = [] # Start empty, add first item below

    # Ensure there's always at least one item row for the user to start with
    if not st.session_state.items_to_set:
         # Add a default, empty item structure
         new_id = 0
         st.session_state.items_to_set.append({
             "id": new_id,
             "key": "",
             "value_str": "", # For simple types
             "current_type_name": "Unknown", # Track detected type
             "map_pairs": [],   # For Map editing
             "array_items": [], # For Array editing
         })


    items = st.session_state.items_to_set
    keys_with_blank = [""] + all_keys
    variable_rows_to_remove = [] # Keep track of indices to remove

    # --- Input Loop for Each Variable Row ---
    for i, item in enumerate(items):
        item_id = item['id']
        st.markdown("---") # Separator for clarity between variables

        # --- Variable Key Selection ---
        cols_key = st.columns([3, 1])
        with cols_key[0]:
            selected_key = st.selectbox(
                f"Variable Key #{i+1}", keys_with_blank,
                index=keys_with_blank.index(item["key"]) if item["key"] in keys_with_blank else 0,
                key=f"select_{item_id}"
            )
            # Update state if key changed
            if item["key"] != selected_key:
                 item["key"] = selected_key
                 item["map_pairs"] = [] # Reset substructure if key changes
                 item["array_items"] = []
                 item["value_str"] = ""
                 # Force rerun ONLY if key actually changed to update UI correctly
                 st.rerun()


            # Determine target type based on selection
            current_val_obj = current_data.get(selected_key) if selected_key else None
            target_type = type(current_val_obj) if current_val_obj else StringOrUnknown # Default new keys to String
            item["current_type_name"] = target_type.__name__ if target_type else "Unknown"

            # Display current value hint
            # ...(You can add the caption display logic here as before if desired)

        with cols_key[1]:
             # Button to remove this entire variable row configuration
             st.write("") # Alignment
             st.write("")
             if st.button("Remove Variable", key=f"remove_var_{item_id}", help="Remove this entire variable configuration row"):
                   variable_rows_to_remove.append(i) # Mark for removal


        # --- Value Input Area (Conditional UI) ---
        if target_type == MapOrUnknown:
            st.markdown("**Editing Map:**")
            item['value_str'] = "" # Clear simple value field when map is active

            # Initialize map pairs list in state if needed
            if 'map_pairs' not in item: item['map_pairs'] = []

            # Display existing pairs for editing
            pair_indices_to_remove = []
            for pair_idx, pair_data in enumerate(item['map_pairs']):
                pair_id = pair_data['id']
                st.markdown(f"*Pair {pair_idx+1}*")
                cols_pair = st.columns([1, 2, 1, 2, 1]) # TypeK, ValK, TypeV, ValV, Remove
                with cols_pair[0]:
                    pair_data['key_type'] = st.selectbox("Key Type", SIMPLE_TYPE_NAMES,
                                                         index=SIMPLE_TYPE_NAMES.index(pair_data.get('key_type', 'String')),
                                                         key=f"map_{item_id}_pair_{pair_id}_ktype")
                with cols_pair[1]:
                    pair_data['key_str'] = st.text_input("Key Value", value=pair_data.get('key_str', ''),
                                                         key=f"map_{item_id}_pair_{pair_id}_kval")
                with cols_pair[2]:
                     pair_data['value_type'] = st.selectbox("Value Type", SIMPLE_TYPE_NAMES,
                                                            index=SIMPLE_TYPE_NAMES.index(pair_data.get('value_type', 'String')),
                                                            key=f"map_{item_id}_pair_{pair_id}_vtype")
                with cols_pair[3]:
                     pair_data['value_str'] = st.text_input("Value", value=pair_data.get('value_str', ''),
                                                            key=f"map_{item_id}_pair_{pair_id}_vval")
                with cols_pair[4]:
                     st.write("") # Alignment
                     st.write("")
                     if st.button("X", key=f"map_{item_id}_pair_{pair_id}_rem", help="Remove this pair"):
                           pair_indices_to_remove.append(pair_idx)

            # Remove pairs marked for deletion (iterate in reverse)
            for pair_idx in sorted(pair_indices_to_remove, reverse=True):
                 del item['map_pairs'][pair_idx]
                 st.rerun() # Rerun immediately on removal

            # Button to add a new pair
            if st.button("Add Map Pair", key=f"map_{item_id}_addpair"):
                new_pair_id = max((p['id'] for p in item['map_pairs']), default=-1) + 1
                item['map_pairs'].append({'id': new_pair_id, 'key_type':'String', 'key_str':'', 'value_type':'String', 'value_str':''})
                st.rerun() # Rerun to show the new pair fields

        elif target_type == ArrayOrUnknown:
            st.markdown("**Editing Array:**")
            item['value_str'] = "" # Clear simple value field when array is active

            # Initialize array items list in state if needed
            if 'array_items' not in item: item['array_items'] = []

            # Display existing items for editing
            item_indices_to_remove = []
            for item_idx, item_data in enumerate(item['array_items']):
                 item_elem_id = item_data['id'] # Renamed from id to avoid confusion
                 st.markdown(f"*Item {item_idx+1}*")
                 cols_item = st.columns([1, 3, 1]) # Type, Value, Remove
                 with cols_item[0]:
                     item_data['item_type'] = st.selectbox("Item Type", SIMPLE_TYPE_NAMES,
                                                           index=SIMPLE_TYPE_NAMES.index(item_data.get('item_type', 'String')),
                                                           key=f"arr_{item_id}_item_{item_elem_id}_type")
                 with cols_item[1]:
                      item_data['item_str'] = st.text_input("Item Value", value=item_data.get('item_str', ''),
                                                            key=f"arr_{item_id}_item_{item_elem_id}_val")
                 with cols_item[2]:
                      st.write("") # Alignment
                      st.write("")
                      if st.button("X", key=f"arr_{item_id}_item_{item_elem_id}_rem", help="Remove this item"):
                           item_indices_to_remove.append(item_idx)

            # Remove items marked for deletion (iterate in reverse)
            for item_idx in sorted(item_indices_to_remove, reverse=True):
                 del item['array_items'][item_idx]
                 st.rerun() # Rerun immediately on removal

            # Button to add a new item
            if st.button("Add Array Item", key=f"arr_{item_id}_additem"):
                 new_item_id = max((it['id'] for it in item['array_items']), default=-1) + 1
                 item['array_items'].append({'id': new_item_id, 'item_type':'String', 'item_str':''})
                 st.rerun() # Rerun to show the new item fields

        else: # Simple types or new key
            st.markdown("**Set Value:**")
            item['map_pairs'] = [] # Clear map/array fields if type changes back
            item['array_items'] = []
            item["value_str"] = st.text_input(
                f"New Value for {item['current_type_name']} (or 'UNKNOWN')",
                value=item.get("value_str", ""), # Use .get for safety
                key=f"input_{item_id}"
            )

    # --- Process Variable Row Removals ---
    if variable_rows_to_remove:
        new_items_to_set = [item for i, item in enumerate(st.session_state.items_to_set) if i not in variable_rows_to_remove]
        st.session_state.items_to_set = new_items_to_set
        st.rerun() # Rerun after removing variable rows


    st.markdown("---")
    # --- Add New Variable Row Button ---
    if st.button("Add Another Variable"):
        new_id = max(item['id'] for item in st.session_state.items_to_set) + 1 if st.session_state.items_to_set else 0
        st.session_state.items_to_set.append({
             "id": new_id, "key": "", "value_str": "",
             "current_type_name": "Unknown", "map_pairs": [], "array_items": []
        })
        st.rerun() # Rerun to show the new row

    st.divider()

    # --- Set Values Button Logic ---
    if st.button("Set Values", type="primary"): # Matched button label from your provided code
        # Initialize counts and lists for this run
        errors, keys_to_set = [], {}
        # Note: success_count and error_count from outer scope are not directly used for Redis results here
        # We will calculate success based on Redis pipeline results specifically

        # --- Parsing and Serialization Loop ---
        for item in st.session_state.items_to_set:
            key = item["key"]
            if not key: continue # Skip items where no key is selected

            target_type_name = item["current_type_name"]
            parsed_spvalue = None # Initialize

            try:
                # --- PARSE based on detected type ---
                if target_type_name == "MapOrUnknown":
                    # Parse the interactive map pairs
                    parsed_pairs_list: List[Tuple[SPValueType, SPValueType]] = []
                    valid_map = True
                    for pair_data in item['map_pairs']:
                        key_type_str = pair_data['key_type']
                        key_str_val = pair_data['key_str']
                        value_type_str = pair_data['value_type']
                        value_str_val = pair_data['value_str']

                        key_type_cls = get_target_type_class(key_type_str)
                        value_type_cls = get_target_type_class(value_type_str)

                        if not key_type_cls or not value_type_cls:
                             errors.append(f"Key '{key}': Invalid type specified in map pair: KType={key_type_str}, VType={value_type_str}")
                             valid_map = False; break

                        parsed_key = parse_input_to_spvalue(key_str_val, key_type_cls)
                        parsed_value = parse_input_to_spvalue(value_str_val, value_type_cls)

                        if parsed_key is None or parsed_value is None:
                             errors.append(f"Key '{key}': Failed to parse map pair K='{key_str_val}' as {key_type_str}, V='{value_str_val}' as {value_type_str}")
                             valid_map = False; break

                        parsed_pairs_list.append((parsed_key, parsed_value))

                    if valid_map:
                        parsed_spvalue = MapOrUnknown(value=parsed_pairs_list)
                    else:
                         # error_count += 1 # Accumulate errors in the 'errors' list instead
                         continue # Skip serialization/setting for this key

                elif target_type_name == "ArrayOrUnknown":
                    # Parse the interactive array items
                    parsed_items_list: List[SPValueType] = []
                    valid_array = True
                    for item_data in item['array_items']:
                        item_type_str = item_data['item_type']
                        item_str_val = item_data['item_str']
                        item_type_cls = get_target_type_class(item_type_str)

                        if not item_type_cls:
                             errors.append(f"Key '{key}': Invalid type specified in array item: Type={item_type_str}")
                             valid_array = False; break

                        parsed_item = parse_input_to_spvalue(item_str_val, item_type_cls)
                        if parsed_item is None:
                             errors.append(f"Key '{key}': Failed to parse array item V='{item_str_val}' as {item_type_str}")
                             valid_array = False; break

                        parsed_items_list.append(parsed_item)

                    if valid_array:
                        parsed_spvalue = ArrayOrUnknown(value=parsed_items_list)
                    else:
                        # error_count += 1
                        continue # Skip serialization/setting

                else: # Simple types or new key (defaulting to String)
                    value_str = item["value_str"]
                    target_cls = globals().get(target_type_name) if target_type_name != "Unknown" else StringOrUnknown
                    parsed_spvalue = parse_input_to_spvalue(value_str, target_cls)
                    if parsed_spvalue is None:
                         errors.append(f"Key '{key}': Failed to parse input '{value_str[:50]}...' for type {target_type_name}")
                         # error_count += 1
                         continue

                # --- SERIALIZE ---
                if parsed_spvalue is not None:
                    serialized_json = serialize_spvalue(parsed_spvalue)
                    if serialized_json is None:
                        errors.append(f"Key '{key}': Failed to serialize parsed value: {parsed_spvalue}")
                        # error_count += 1
                        continue
                    keys_to_set[key] = serialized_json
                # else: Error handled during parsing phase

            except Exception as e:
                 logging.error(f"Unexpected error processing variable '{key}': {e}", exc_info=True)
                 errors.append(f"Key '{key}': Unexpected error during processing: {e}")
                 # error_count += 1
                 continue # Skip this key

        # --- Execution Logic (Redis Pipeline) ---
        if not keys_to_set and not errors:
             st.warning("No valid variable configurations to set.")
        elif keys_to_set:
             # Initialize success count specific to this Redis operation
             current_set_success_count = 0
             try:
                 # Setup and execute the pipeline
                 pipe = r.pipeline()
                 for key, json_val in keys_to_set.items():
                    pipe.set(key, json_val)
                    logging.info(f"Pipeline SET: Key='{key}', Value='{json_val[:100]}...'")
                 results = pipe.execute()

                 # --- Check Redis Results ---
                 failed_sets = []
                 set_keys = list(keys_to_set.keys())
                 for i, result in enumerate(results):
                     key = set_keys[i]
                     if result: # Redis SET returns True on success
                         current_set_success_count += 1 # Increment actual success count
                     else:
                         failed_sets.append(key)
                         # Add error message only if not already present from parsing stage
                         error_msg = f"Key '{key}': Redis SET command failed."
                         if error_msg not in errors:
                             errors.append(error_msg)
                         logging.error(f"Redis SET command failed for key '{key}'. Check Redis logs.")

                 # --- Display Feedback ---
                 if current_set_success_count > 0:
                     # Clear cache *before* showing success message if not rerunning
                     if 'st' in sys.modules and hasattr(st, 'cache_data') and hasattr(st.cache_data, 'clear'):
                          st.cache_data.clear() # Clear cache so next manual refresh gets new data

                     # Display the success message - this will now persist
                     st.success(f"Successfully set {current_set_success_count} variable(s).")

                     # --- REMOVED st.rerun() ---
                     # No automatic rerun here, message will stay visible.

                 # Displaying failures (if any)
                 if failed_sets:
                     # Ensure we show the specific SET failures distinctly if needed
                     set_failure_errors = [e for e in errors if "Redis SET command failed" in e]
                     if set_failure_errors: # Check if there are actual SET failures to report
                          st.error(f"Failed to execute Redis SET for {len(failed_sets)} variable(s): {', '.join(failed_sets)}")

                 # Display prior parsing/serialization errors if they existed (and weren't SET errors)
                 prior_errors = [e for e in errors if "Redis SET command failed" not in e]
                 if prior_errors:
                     st.error("Additional errors occurred before sending to Redis:")
                     for error in prior_errors: st.error(error)

             except redis.RedisError as e:
                  st.error(f"A Redis error occurred during the SET operation: {e}")
                  logging.error(f"Redis pipeline SET error: {e}", exc_info=True)
             except Exception as e:
                  st.error(f"An unexpected error occurred during the SET operation: {e}")
                  logging.error(f"Unexpected pipeline SET error: {e}", exc_info=True)

        # --- Display Errors if no set was attempted but errors occurred during parsing ---
        elif errors:
             st.error(f"Found {len(errors)} error(s) preventing setting values:")
             # Use set to avoid potential duplicates if error logic added them multiple times
             displayed_errors = set()
             for error in errors:
                 if error not in displayed_errors:
                     st.error(error)
                     displayed_errors.add(error)
        # If not keys_to_set and no errors, the warning above handles it.

# --- Tab 3: State Details Viewer ---
# Helper function to display nested SPValues (like in Maps/Arrays)
def display_spvalue_detail(sp_value: SPValueType, indent: int = 0) -> None:
    prefix = "  " * indent
    if sp_value is None:
        st.text(f"{prefix}Error/Null")
        return

    type_name = type(sp_value).__name__
    val = getattr(sp_value, 'value', None)

    if isinstance(sp_value, (BoolOrUnknown, IntOrUnknown, FloatOrUnknown, StringOrUnknown, TimeOrUnknown)):
        val_str = str(val) if val is not None else "UNKNOWN"
        if isinstance(sp_value, StringOrUnknown) and val is not None: val_str = f'"{val_str}"'
        if isinstance(sp_value, TimeOrUnknown) and val is not None: val_str = val.isoformat()
        st.text(f"{prefix}{type_name}: {val_str}")

    elif isinstance(sp_value, ArrayOrUnknown):
        st.text(f"{prefix}Array:")
        if val is not None:
            if val:
                for i, item in enumerate(val):
                    st.text(f"{prefix}  [{i}]:")
                    display_spvalue_detail(item, indent + 2)
            else:
                st.text(f"{prefix}  (Empty Array)")
        else:
            st.text(f"{prefix}  UNKNOWN")

    elif isinstance(sp_value, MapOrUnknown):
        st.text(f"{prefix}Map:")
        if val is not None:
            if val:
                for i, (key, map_val) in enumerate(val):
                    st.text(f"{prefix}  Pair {i}:")
                    st.text(f"{prefix}    Key:")
                    display_spvalue_detail(key, indent + 3)
                    st.text(f"{prefix}    Value:")
                    display_spvalue_detail(map_val, indent + 3)
            else:
                st.text(f"{prefix}  (Empty Map)")
        else:
            st.text(f"{prefix}  UNKNOWN")

    elif isinstance(sp_value, TransformOrUnknown):
        st.text(f"{prefix}Transform:")
        tf_stamped = val
        if tf_stamped is not None:
            st.text(f"{prefix}  Active: {tf_stamped.active}")
            st.text(f"{prefix}  Timestamp: {tf_stamped.time_stamp.isoformat() if tf_stamped.time_stamp else 'None'}")
            st.text(f"{prefix}  Parent Frame: {tf_stamped.parent_frame_id or 'None'}")
            st.text(f"{prefix}  Child Frame: {tf_stamped.child_frame_id or 'None'}")
            st.text(f"{prefix}  Transform Details:")
            if tf_stamped.transform and tf_stamped.transform.translation and tf_stamped.transform.rotation:
                trans = tf_stamped.transform.translation
                rot = tf_stamped.transform.rotation
                st.text(f"{prefix}    Translation: x={trans.x:.4f}, y={trans.y:.4f}, z={trans.z:.4f}")
                st.text(f"{prefix}    Rotation:    x={rot.x:.4f}, y={rot.y:.4f}, z={rot.z:.4f}, w={rot.w:.4f}")
            else:
                 st.text(f"{prefix}    (Missing Transform Details)")
            st.text(f"{prefix}  Metadata:")
            # Metadata is MapOrUnknown, display it recursively
            display_spvalue_detail(tf_stamped.metadata, indent + 2) # Metadata itself is a MapOrUnknown
        else:
            st.text(f"{prefix}  UNKNOWN")

    else:
        st.text(f"{prefix}Unexpected Type: {type_name}")

# --- Tab 3: State Details Viewer (Reverted and Updated) ---
def state_details(data: Dict[str, Optional[SPValueType]], all_keys: List[str]) -> None:
    """Displays detailed information about a selected state variable, preserving original layout."""
    st.header("View Variable Details")
    if not all_keys:
        st.warning("No keys found in Redis.")
        return

    keys_with_blank = [""] + all_keys
    # Use a unique key for the selectbox to avoid conflicts
    selected_key = st.selectbox("Select Key to View Details:", options=keys_with_blank, key="details_view_select")

    if selected_key:
        st.subheader(f"Details for: `{selected_key}`")
        current_value_obj = data.get(selected_key)

        # --- Display Parsed Value (Original Style) ---
        if current_value_obj is not None:
            st.write("**Parsed Value:**")
            # Display the object's default representation
            st.write(current_value_obj)

            # --- Display Raw JSON (Original Style) ---
            st.write("**Raw JSON String in Redis:**")
            try:
                raw_json = r.get(selected_key)
                # Use the original text_area setup
                st.text_area(
                    "Raw JSON Value", # Label (hidden by visibility)
                    value=raw_json or "Key exists but value is empty/null in Redis",
                    height=150,
                    disabled=True,
                    key=f"raw_json_details_{selected_key}", # Unique key
                    label_visibility="collapsed" # Hide the label visually
                 )
            except Exception as e:
                st.error(f"Error retrieving raw JSON for '{selected_key}': {e}")

            # --- NEW/UPDATED: Display Content for Complex Types (Original Style) ---
            if isinstance(current_value_obj, ArrayOrUnknown) and current_value_obj.value is not None:
                st.write("**Array Content (Parsed):**")
                if current_value_obj.value:
                    # Simple text representation for each item
                    for i, item in enumerate(current_value_obj.value):
                        st.text(f"  [{i}]: {item}")
                else:
                    st.text("  (Empty Array)")

            elif isinstance(current_value_obj, MapOrUnknown) and current_value_obj.value is not None:
                st.write("**Map Content (Parsed):**")
                if current_value_obj.value:
                     # Simple text representation for each key-value pair
                     for i, (key, value) in enumerate(current_value_obj.value):
                         st.text(f"  Pair {i}:")
                         st.text(f"    Key: {key}")
                         st.text(f"    Value: {value}")
                else:
                     st.text("  (Empty Map)")

            elif isinstance(current_value_obj, TransformOrUnknown) and current_value_obj.value is not None:
                 st.write("**Transform Content (Parsed):**")
                 tf_stamped = current_value_obj.value
                 # Display fields using simple text
                 st.text(f"  Active: {tf_stamped.active}")
                 st.text(f"  Timestamp: {tf_stamped.time_stamp.isoformat() if tf_stamped.time_stamp else 'None'}")
                 st.text(f"  Parent Frame: {tf_stamped.parent_frame_id or 'None'}")
                 st.text(f"  Child Frame: {tf_stamped.child_frame_id or 'None'}")
                 if tf_stamped.transform and tf_stamped.transform.translation and tf_stamped.transform.rotation:
                    trans = tf_stamped.transform.translation
                    rot = tf_stamped.transform.rotation
                    st.text(f"  Translation: x={trans.x}, y={trans.y}, z={trans.z}")
                    st.text(f"  Rotation:    x={rot.x}, y={rot.y}, z={rot.z}, w={rot.w}")
                 else:
                    st.text("  Transform Details: (Missing)")
                 st.text(f"  Metadata (Map):")
                 if tf_stamped.metadata and tf_stamped.metadata.value is not None:
                     if tf_stamped.metadata.value:
                         st.text(f"    [{len(tf_stamped.metadata.value)} pairs] - See Parsed Value above for full Map details")
                         # Avoid full recursive display here to keep it simple like original Array
                         # for i, (key, value) in enumerate(tf_stamped.metadata.value):
                         #    st.text(f"      Pair {i}: K={key}, V={value}")
                     else:
                         st.text("    (Empty Map)")
                 else:
                     st.text("    UNKNOWN or None")
            # --- End New/Updated ---

        # --- Handle Deserialization Failure (Original Style) ---
        elif selected_key in data: # Key exists in dict keys (meaning SCAN found it) but value is None (MGET failed or deserialize failed)
             st.warning("Could not deserialize value in Redis for this key.")
             # Attempt to show raw JSON even on deserialization failure
             st.write("**Raw JSON String in Redis:**")
             try:
                raw_json = r.get(selected_key)
                st.text_area(
                    "Raw JSON Value",
                    value=raw_json or "Key exists but value is empty/null in Redis",
                    height=150,
                    disabled=True,
                    key=f"raw_json_fail_details_{selected_key}", # Unique key
                    label_visibility="collapsed"
                )
             except Exception as e:
                st.error(f"Error retrieving raw JSON for '{selected_key}': {e}")
        else:
             # This case means the key wasn't even found in the initial read_all_data
             st.error(f"Key '{selected_key}' not found (may have been deleted).")
    else:
        st.info("Select a key to view details.")

# --- Tab 4: Robot Tab ---
def robot_tab(data: Dict[str, Optional[SPValueType]], all_keys: List[str]) -> None:
    """Displays robot-specific information, including transform selections."""
    st.header("Robot Control / View")

    # Filter keys that correspond to TransformOrUnknown values
    transform_keys = [
        key for key, value_obj in data.items()
        if isinstance(value_obj, TransformOrUnknown)
    ]

    if not transform_keys:
        st.info("No variables with type 'Transform' found in the current state.")
        return

    # Add a blank option to allow deselection or default state
    transform_keys_with_blank = [""] + sorted(transform_keys)

    # --- First Dropdown: Move to Frame ---
    selected_move_frame_key = st.selectbox(
        "Move to Frame:", # Renamed label
        options=transform_keys_with_blank,
        key="robot_move_frame_select" # Unique key
    )

    if selected_move_frame_key:
        st.write(f"Selected target frame: `{selected_move_frame_key}`")
        # Details display removed as requested
    else:
        st.info("Select a target frame from the list above.")

    st.divider() # Add a separator between dropdowns

    # --- Second Dropdown: With TCP ---
    selected_tcp_key = st.selectbox(
        "With TCP:", # New dropdown label
        options=transform_keys_with_blank, # Use the same list of transforms
        key="robot_tcp_select" # Unique key for the second dropdown
    )

    if selected_tcp_key:
        st.write(f"Selected TCP: `{selected_tcp_key}`")
    else:
        st.info("Select a TCP from the list above.")


# --- Main Application (Modified) ---
def main() -> None:
    """Runs the Streamlit application."""
    st.set_page_config(layout="wide")
    st.title("micro_sp_ui")

    if st.button("Refresh Data from Redis"):
        if 'st' in sys.modules and hasattr(st, 'cache_data') and hasattr(st.cache_data, 'clear'):
             st.cache_data.clear() # Clear cache if used
        st.rerun()

    current_time_for_cache = datetime.now().timestamp()
    data, all_keys = read_all_data(current_time_for_cache)

    logging.debug(f"UI data refreshed/retrieved. Keys: {len(all_keys)}, Deserialized items: {len(data)}")

    tab1, tab2, tab3, tab4 = st.tabs(["View State", "Set State", "Details", "Robot"])

    with tab1: state_viewer(data)
    with tab2: state_setter_impl(data, all_keys)
    with tab3: state_details(data, all_keys)
    with tab4: robot_tab(data, all_keys) # Call the updated robot_tab function

if __name__ == "__main__":
    main()