import asyncio
import websockets
import json
import random
import time

import datetime

def generate_random_list(n, start, end):
    return [random.randint(start, end) for _ in range(n)]

def generate_random_float_list(n, start, end):
    return [random.uniform(start, end) for _ in range(n)]

async def send_request():
    YOUR_PING_TIMEOUT = 36000

    while True:
        matrix_size = random.randint(1, 100) 
        uri = "ws://localhost:8000/ws"  # Replace with your server's address
        async with websockets.connect(uri, ping_timeout=YOUR_PING_TIMEOUT) as websocket:
            # Define your parameters here
            # Usage:
            S_list = generate_random_list(matrix_size, 80, 90)  # List of underlying asset prices
            K_list = generate_random_list(matrix_size, 100, 110)  # List of strike prices
            T_list = generate_random_list(matrix_size, 1, 2) # List of times to maturity (in years)
            r_list = generate_random_float_list(matrix_size, 0.01, 0.06)  # List of risk-free interest rates
            price_list = generate_random_list(matrix_size, 10, 11)  # List of observed option prices
            option_type = 'call'  # Option type (ql.Option.Call for call, ql.Option.Put for put)
            
            options_param = {
                'S_list': S_list,  # List of underlying asset prices
                'K_list': K_list,  # List of strike prices
                'T_list': T_list,  # List of times to maturity (in years)
                'rist_list': r_list,  # List of risk-free interest rates
                'price_list': price_list,  # List of observed option prices
                'option_type':  option_type # Option type ('call' for call, 'put' for put)
            }
            await websocket.send(json.dumps(options_param))
            response = await websocket.recv()
            app_receive_time = time.time() #.datetime.now()
            print(f"{app_receive_time} {response}")
            time.sleep(10)


# Run the function
asyncio.run(send_request())
