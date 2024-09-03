# import asyncio
# import websockets
# import json
# import boto3
# import decimal
#
# # Initialize DynamoDB client
# dynamodb = boto3.resource("dynamodb", region_name="us-east-1")  # Update the region as necessary
# table = dynamodb.Table("final_table")
#
# # Initialize the last processed timestamp
# last_processed_timestamp = None
#
#
# class DecimalEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, decimal.Decimal):
#             return str(o)
#         return super().default(o)
#
#
# async def fetch_data_from_dynamodb():
#     global last_processed_timestamp
#     try:
#         # Scan the table with a filter expression if needed
#         scan_kwargs = {}
#         if last_processed_timestamp:
#             scan_kwargs["FilterExpression"] = "timestamp > :last_timestamp"
#             scan_kwargs["ExpressionAttributeValues"] = {":last_timestamp": last_processed_timestamp}
#
#         response = table.scan(**scan_kwargs)
#         items = response["Items"]
#
#         # Handle pagination
#         while "LastEvaluatedKey" in response:
#             response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
#             items.extend(response["Items"])
#
#         if not items:
#             return {}
#
#         # Update last_processed_timestamp to the latest item's timestamp
#         latest_item = max(items, key=lambda x: int(x.get("timestamp", 0)))
#         last_processed_timestamp = int(latest_item.get("timestamp", 0))
#
#         # Prepare data for sending
#         payload = latest_item.get("payload", {})
#         data = {
#             "SensorData": {
#                 "beatsPerMinute": payload.get("beatsPerMinute", 0),
#                 "beatAvg": payload.get("beatAvg", 0),
#                 "SpO2": payload.get("SpO2", 0),
#                 "bodyTemperature": payload.get("bodyTemperature", 0),
#             }
#         }
#
#         return data
#     except Exception as e:
#         print(f"Error fetching data from DynamoDB: {e}")
#         return {}
#
#
# async def send_data_from_dynamodb(websocket, path):
#     while True:
#         data = await fetch_data_from_dynamodb()
#
#         # Print data to the console
#         print("Fetched data from DynamoDB:", json.dumps(data, indent=4))
#
#         if data:
#             await websocket.send(json.dumps(data, cls=DecimalEncoder))
#         else:
#             print("No new data to send.")
#
#         await asyncio.sleep(1)  # Send data every second
#
#
# start_server = websockets.serve(send_data_from_dynamodb, "127.0.0.1", 5000)
#
# asyncio.get_event_loop().run_until_complete(start_server)
# asyncio.get_event_loop().run_forever()


# import asyncio
# import websockets
# import json
# import boto3
# import decimal
# from boto3.dynamodb.conditions import Attr
#
# # Initialize DynamoDB client
# dynamodb = boto3.resource("dynamodb", region_name="us-east-1")  # Update the region as necessary
# table = dynamodb.Table("final_table")
#
# # Initialize the last processed payload (to detect changes)
# last_processed_payload = None
#
#
# class DecimalEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, decimal.Decimal):
#             # Convert Decimal to a string or float
#             return float(o)
#         return super().default(o)
#
#
# async def fetch_data_from_dynamodb():
#     global last_processed_payload
#     try:
#         # Scan the table to get all items
#         response = table.scan()
#         items = response.get("Items", [])
#
#         if not items:
#             return {}
#
#         # Find the item with the latest timestamp
#         latest_item = max(items, key=lambda x: int(x.get("timestamp", 0)))
#
#         # Get the current payload
#         current_payload = latest_item.get("payload", {})
#
#         # Check if the payload has changed
#         if current_payload != last_processed_payload:
#             last_processed_payload = current_payload
#
#             # Prepare data for sending
#             data = {
#                 "SensorData": {
#                     "beatsPerMinute": current_payload.get("beatsPerMinute", 0),
#                     "beatAvg": current_payload.get("beatAvg", 0),
#                     "SpO2": current_payload.get("SpO2", 0),
#                     "bodyTemperature": current_payload.get("bodyTemperature", 0),
#                 }
#             }
#
#             return data
#         else:
#             return {}  # No new data
#     except Exception as e:
#         print(f"Error fetching data from DynamoDB: {e}")
#         return {}
#
#
# async def send_data_from_dynamodb(websocket, path):
#     while True:
#         data = await fetch_data_from_dynamodb()
#
#         # Print data to the console if it exists
#         if data:
#             print("Fetched updated data from DynamoDB:", json.dumps(data, indent=4, cls=DecimalEncoder))
#             await websocket.send(json.dumps(data, cls=DecimalEncoder))
#         else:
#             print("No new data to send.")
#
#         await asyncio.sleep(1)  # Poll every second
#
#
# start_server = websockets.serve(send_data_from_dynamodb, "127.0.0.1", 5000)
#
# asyncio.get_event_loop().run_until_complete(start_server)
# asyncio.get_event_loop().run_forever()

# import asyncio
# import websockets
# import json
# import boto3
# import decimal
# from fastapi import FastAPI, WebSocket
# from boto3.dynamodb.conditions import Attr
# from pydantic import BaseModel
#
# app = FastAPI()
#
# # Initialize DynamoDB client
# dynamodb = boto3.resource("dynamodb", region_name="us-east-1")  # Update the region as necessary
# table = dynamodb.Table("final_table")
#
# # Initialize the last processed payload (to detect changes)
# last_processed_payload = None
#
#
# class DecimalEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, decimal.Decimal):
#             # Convert Decimal to a string or float
#             return float(o)
#         return super().default(o)
#
#
# async def fetch_data_from_dynamodb():
#     global last_processed_payload
#     try:
#         # Scan the table to get all items
#         response = table.scan()
#         items = response.get("Items", [])
#
#         if not items:
#             return {}
#
#         # Find the item with the latest timestamp
#         latest_item = max(items, key=lambda x: int(x.get("timestamp", 0)))
#
#         # Get the current payload
#         current_payload = latest_item.get("payload", {})
#
#         # Check if the payload has changed
#         if current_payload != last_processed_payload:
#             last_processed_payload = current_payload
#
#             # Prepare data for sending
#             data = {
#                 "SensorData": {
#                     "beatsPerMinute": current_payload.get("beatsPerMinute", 0),
#                     "beatAvg": current_payload.get("beatAvg", 0),
#                     "SpO2": current_payload.get("SpO2", 0),
#                     "bodyTemperature": current_payload.get("bodyTemperature", 0),
#                 }
#             }
#
#             return data
#         else:
#             return {}  # No new data
#     except Exception as e:
#         print(f"Error fetching data from DynamoDB: {e}")
#         return {}
#
#
# @app.websocket("/127.0.0.1:8000")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     try:
#         while True:
#             data = await fetch_data_from_dynamodb()
#
#             # Print data to the console if it exists
#             if data:
#                 print("Fetched updated data from DynamoDB:", json.dumps(data, indent=4, cls=DecimalEncoder))
#                 await websocket.send_text(json.dumps(data, cls=DecimalEncoder))
#             else:
#                 print("No new data to send.")
#
#             await asyncio.sleep(1)  # Poll every second
#     except Exception as e:
#         print(f"WebSocket connection error: {e}")
#     finally:
#         await websocket.close()
#
#
# # Run the FastAPI application using Uvicorn
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="127.0.0.1", port=8000)

import asyncio
import json
import boto3
import decimal
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("final_table")

# Initialize the last processed payload (to detect changes)
last_processed_payload = None


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)


async def fetch_data_from_dynamodb():
    global last_processed_payload
    try:
        response = table.scan()
        items = response.get("Items", [])

        if not items:
            return {}

        latest_item = max(items, key=lambda x: int(x.get("timestamp", 0)))
        current_payload = latest_item.get("payload", {})

        if current_payload != last_processed_payload:
            last_processed_payload = current_payload

            data = {
                "SensorData": {
                    "beatsPerMinute": current_payload.get("beatsPerMinute", 0),
                    "beatAvg": current_payload.get("beatAvg", 0),
                    "SpO2": current_payload.get("SpO2", 0),
                    "bodyTemperature": current_payload.get("bodyTemperature", 0),
                }
            }

            return data
        else:
            return {}
    except Exception as e:
        print(f"Error fetching data from DynamoDB: {e}")
        return {}


@app.get("/")
async def read_root():
    return {"message": "Welcome to the FastAPI WebSocket server"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await fetch_data_from_dynamodb()
            if data:
                print("Fetched updated data from DynamoDB:", json.dumps(data, indent=4, cls=DecimalEncoder))
                await websocket.send_text(json.dumps(data, cls=DecimalEncoder))
            else:
                print("No new data to send.")
            await asyncio.sleep(1)
    except Exception as e:
        print(f"WebSocket connection error: {e}")
    finally:
        await websocket.close()


# Run the FastAPI application using Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
