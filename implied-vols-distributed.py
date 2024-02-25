import dask
from dask.distributed import Client, LocalCluster
from dask_kubernetes import KubeCluster
import QuantLib as ql
import random
import timeit
import multiprocessing
import psutil
import matplotlib.pyplot as plt

def generate_random_list(n, start, end):
    return [random.randint(start, end) for _ in range(n)]

def generate_random_float_list(n, start, end):
    return [random.uniform(start, end) for _ in range(n)]

def calculate_implied_volatility(S, K, T, r, price, option_type):
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

def calculate_implied_volatilities(S_list, K_list, T_list, r_list, price_list, option_type):
    # Ensure all lists are of the same length
    assert len(S_list) == len(K_list) == len(T_list) == len(r_list) == len(price_list), "All input lists must be of the same length."

    # Start a Dask client with a specific number of workers
    
    # Use Dask to distribute the calculations
    tasks = [dask.delayed(calculate_implied_volatility)(S, K, T, r, price, option_type) for S, K, T, r, price in zip(S_list, K_list, T_list, r_list, price_list)]
    implied_vols = client.compute(tasks, sync=True, scheduler='processes')
    
    return implied_vols


def run(matrix_size):
     # Usage:
    S_list = generate_random_list(matrix_size, 80, 90)  # List of underlying asset prices
    K_list = generate_random_list(matrix_size, 100, 110)  # List of strike prices
    T_list = generate_random_list(matrix_size, 1, 2) # List of times to maturity (in years)
    r_list = generate_random_float_list(matrix_size, 0.01, 0.06)  # List of risk-free interest rates
    price_list = generate_random_list(matrix_size, 10, 11)  # List of observed option prices
    option_type = ql.Option.Call  # Option type (ql.Option.Call for call, ql.Option.Put for put)
    
    return (calculate_implied_volatilities(S_list, K_list, T_list, r_list, price_list, option_type));
     

def generate_powers_of_two(x):
    return [n for n in range(1,x+1)]

def run_with_benchmark_and_plot(num_runs, matrix_size):
    num_of_run = []  # Number of CPUs
    times = []  # Time to complete
    # genearte cpu list <= num_cpus
    num_of_run: list[int] = generate_powers_of_two(num_runs)
    # Number of iterations
    # Time the function
    # calculate the average timedefault_timer
    for _ in range(num_runs):
        start_time: float = timeit.default_timer()
        run(matrix_size)
        end_time = timeit.default_timer()
        # Calculate the average run time
        run_time = (end_time - start_time)
        times.append(run_time)
    # Create the plot
    plt.figure(figsize=(10, 6))
    plt.plot(num_of_run, times, marker='o')

    # Add labels and title
    plt.xlabel('Number of runs')
    plt.ylabel('Time to Complete (s)')
    plt.title('Benchmark Results')

    # Show the plot
    plt.show()

def run_without_benchmark(matrix_size):
    
    # Time the function
    start_time: float = timeit.default_timer()
    run(matrix_size)
    end_time = timeit.default_timer()
    # Calculate the average run time
    run_time = (end_time - start_time)
    print(f"The function took an average of {run_time:.10f} seconds to complete.")

if __name__ == '__main__':
    
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
    matrix_size = 20000
    # run with benchmark and plot
    #run_with_benchmark_and_plot(num_real_cores, matrix_size)
    # run without benchmark
    run_without_benchmark(matrix_size)

    