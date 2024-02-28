import dask
from dask.distributed import Client, LocalCluster
from dask_kubernetes.operator import KubeCluster
import QuantLib as ql
import random
import timeit
import time #epoc
import multiprocessing
import psutil
from fastapi import FastAPI, WebSocket
from typing import Any
import json

def calculate_implied_volatility(S, K, T, r, price, option_type):

    if (option_type == 'call'):
        option_type = ql.Option.Call
    elif (option_type == 'put'):
        option_type = ql.Option.Put
    else:
        raise ('invalide option_type {option_type}')
    # Define the underlying asset
    day_count = ql.Actual365Fixed()
    calendar = calendar = ql.UnitedStates(ql.UnitedStates.Settlement)
    calculation_date = ql.Date().todaysDate()

    ql.Settings.instance().evaluationDate = calculation_date

    spot_handle = ql.QuoteHandle(ql.SimpleQuote(S))
    flat_ts = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date, r, day_count))
    dividend_yield = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date, 0.0, day_count))
    flat_vol_ts = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(calculation_date, calendar, 0.20, day_count))
    bsm_process = ql.BlackScholesMertonProcess(spot_handle, dividend_yield, flat_ts, flat_vol_ts)

    # Define the option
    payoff = ql.PlainVanillaPayoff(option_type, K)
    exercise = ql.EuropeanExercise(calculation_date + int(T*365))
    european_option = ql.VanillaOption(payoff, exercise)

    # Calculate implied volatility
    implied_vol = european_option.impliedVolatility(price, bsm_process)
    
    return implied_vol

async def calculate_implied_volatilities(S_list, K_list, T_list, r_list, price_list, option_type):
    # Ensure all lists are of the same length
    assert len(S_list) == len(K_list) == len(T_list) == len(r_list) == len(price_list), "All input lists must be of the same length."
    print(f"quantlib receive {S_list}.")
    # Define your pod spec
   

    # Now you can use this client to compute tasks
    # Use Dask to distribute the calculations
    # find number of CPUs on the machine
    total_cores = multiprocessing.cpu_count()
    num_real_cores = psutil.cpu_count(logical=False)  

    print(f"{total_cores} total cores found on the pc")
    print(f"{num_real_cores} pyhsical cores found on the pc")

    max_workers = 6
    cluster = LocalCluster()
    client = cluster.get_client()
    cluster.adapt(minimum=1, maximum=max_workers)  # scale between 0 and 100 workers
    print(f"Run with a claster with auto horizontal scaling on your local machine.")
        

    tasks = [dask.delayed( obj=calculate_implied_volatility)(S, K, T, r, price, option_type) for S, K, T, r, price in zip(S_list, K_list, T_list, r_list, price_list)]
    
    implied_vols = client.compute(tasks, sync=True, scheduler='processes')

    return implied_vols


async def run(options_param):
     # Usage:
    S_list = options_param['S_list']  # List of underlying asset prices
    K_list = options_param['K_list']  # List of strike prices
    T_list = options_param['T_list'] # List of times to maturity (in years)
    r_list = options_param['rist_list'] # List of risk-free interest rates
    price_list = options_param['price_list']  # List of observed option prices
    option_type = options_param['option_type']  # Option type (ql.Option.Call for call, ql.Option.Put for put)

    return (await calculate_implied_volatilities(S_list, K_list, T_list, r_list, price_list, option_type));

async def distribute(options_param):
    print(f"distribute received {options_param}.")

    # Time the function
    start_time: float = timeit.default_timer()
    implied_vols = await run(options_param)
    end_time = timeit.default_timer()
    # Calculate the average run time
    run_time = (end_time - start_time)
    print(f"The function took an average of {run_time:.10f} seconds to complete.")
    return (implied_vols, run_time)


app = FastAPI()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        data = await websocket.receive_text()
        json_data = json.loads(data)  # Parse JSON input
        websocket.send_text("Operation started. Please wait...")
        # Now you can use json_data as a Python dictionary
        # For example, let's echo it back to the client
        implied_vols_response, run_time = await distribute(json_data)
        result_param = {
            'server_sent_timestamp': int (time.time()),
            'run_time': run_time,
            'implied_vols': implied_vols_response
        }
        print(f"quantlib complete {result_param}.")

        await websocket.send_json(json.dumps(result_param))
