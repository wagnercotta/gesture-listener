import argparse
import json

from google.protobuf.empty_pb2 import Empty
from google.protobuf.struct_pb2 import Struct
from is_wire.core import Message
from is_wire.enhancement.is_wire_enhanced import IsWireEnhanced

NUM_TO_CLASS = {
    0: "None",
    1: "Ask for help",
    2: "Follow",
    3: "Abort",
    4: "Give away",
    5: "Pointing",
    6: "Doubt",
    7: "Silence",
}


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return json.load(f)


def parse_args():
    parser = argparse.ArgumentParser(description="Gesture listener service")
    parser.add_argument(
        "config", type=str, help="Path to the configuration JSON file"
    )
    return parser.parse_args()


def main():
    # Parse command-line arguments
    args = parse_args()

    # Load configuration
    config = load_config(args.config)

    amqp_config = config["amqp"]
    topic = config["topic"]
    gesture_rpc_mapping = config["gesture_rpc_mapping"]

    # Initialize IsWireEnhanced
    is_wire = IsWireEnhanced(
        user=amqp_config["user"],
        password=amqp_config["password"],
        host=amqp_config["host"],
        port=amqp_config["port"],
    )

    print(f"Listening on topic: {topic}")
    print(f"Gesture RPC mapping: {gesture_rpc_mapping}")

    # Subscribe and consume messages in a loop
    is_wire.subscriptions.subscribe(topic)

    while True:
        msg = is_wire.channel.consume()
        print(f"Message received on topic '{msg}'")
        # Try to unpack as Struct

        rcvd_msg = msg.unpack(Struct)

        # Check if "gesture" field exists
        if "class" in rcvd_msg:
            raw_gesture = rcvd_msg["class"]
            print(f"Received gesture code: {raw_gesture}")

            try:
                gesture_code = int(raw_gesture)
            except (TypeError, ValueError):
                print(f"Invalid gesture code: {raw_gesture}")
                continue

            gesture = NUM_TO_CLASS.get(gesture_code)
            if gesture is None:
                print(f"Unknown gesture code: {gesture_code}")
                continue

            print(f"Translated gesture: {gesture}")

            # Check if gesture is mapped to an RPC
            if gesture in gesture_rpc_mapping:
                rpc_topic = gesture_rpc_mapping[gesture]
                print(f"Calling RPC: {rpc_topic}")

                # Create request message with Empty protobuf
                request = Message(content=Empty())
                request.topic = rpc_topic
                request.reply_to = is_wire.subscriptions

                # Publish the RPC request
                is_wire.channel.publish(message=request, topic=rpc_topic)

                # Wait for reply (optional, with timeout)
                try:
                    reply = is_wire.channel.consume(timeout=5.0)
                    if reply:
                        print(f"RPC response received from {rpc_topic}")
                except Exception as e:
                    print(f"RPC call timeout or error: {e}")
            else:
                print(f"No RPC mapping for gesture: {gesture}")


if __name__ == "__main__":
    main()
