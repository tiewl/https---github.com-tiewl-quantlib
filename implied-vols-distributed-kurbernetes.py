import dask
from dask.distributed import Client, LocalCluster
from dask_kubernetes.classic import KubeCluster, make_pod_spec
import QuantLib as ql
import random
import timeit
import multiprocessing
import psutil
import matplotlib.pyplot as plt

def generate_random_list(n, start, end):
    """
    This function return a list of random float numbers.

    Parameters:
        n (int): max number.
        start (int): start number.
        end (int): end number.
    Returns:
    a list of int
    """
    return [random.randint(start, end) for _ in range(n)]

def generate_random_float_list(n, start, end):
    """
    This function return a list of random float numbers.

    Parameters:
        n (int): max number.
        start (int): start number.
        end (int): end number.
    Returns:
    a list of float
    """
    return [random.uniform(start, end) for _ in range(n)]

def calculate_implied_volatility(S, K, T, r, price, option_type):
    """
    This function calculate the implied volitility using quantlib and returns the result.

    Parameters:
        S (loat): List of underlying asset prices
        K (float): strike price
        T (int): times to maturity (in years)
        r (float): risk-free interest rate
        price (float): observed option price
        option_type (int): Option type (ql.Option.Call for call, ql.Option.Put for put)
    Returns:
        int or float: List of implied volitility for each expire and strik pair
    """
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
    """
    This function iterate distribute the lists to calculate the implied volitility in parallels
    Parameters:
        S_list (list of float): List of underlying asset prices
        K_list (list of float): List of strike prices
        T_list (list of int): List of times to maturity (in years)
        r_list (list of float): List of risk-free interest rates
        price_list (list of float): List of observed option prices
        option_type (list of int): Option type (ql.Option.Call for call, ql.Option.Put for put)
    Returns:
        int or float: List of implied volitility for each expire and strik pair
    """
    # Ensure all lists are of the same length
    assert len(S_list) == len(K_list) == len(T_list) == len(r_list) == len(price_list), "All input lists must be of the same length."

    # Use Dask to distribute the calculations
    tasks = [dask.delayed(calculate_implied_volatility)(S, K, T, r, price, option_type) for S, K, T, r, price in zip(S_list, K_list, T_list, r_list, price_list)]
    implied_vols = client.compute(tasks, sync=True)
    return implied_vols


def run():
    """
    This function calculate the implied vols by generate the follwoing fictitious:-
        List of underlying asset prices
        List of strike prices
        List of times to maturity (in years)
        List of risk-free interest rates
        List of observed option prices
        Option type (ql.Option.Call for call, ql.Option.Put for put)

        Note: the above list is generated with random number
    Parameters:
        none

    Returns:
        list of implied volitility
    """
     # Usage:
    S_list = generate_random_list(15000, 80, 90)  # List of underlying asset prices
    K_list = generate_random_list(15000, 100, 110)  # List of strike prices
    T_list = generate_random_list(15000, 1, 2) # List of times to maturity (in years)
    r_list = generate_random_float_list(15000, 0.01, 0.06)  # List of risk-free interest rates
    price_list = generate_random_list(15000, 10, 11)  # List of observed option prices
    option_type = ql.Option.Call  # Option type (ql.Option.Call for call, ql.Option.Put for put)
    
    implied_vols = calculate_implied_volatilities(S_list, K_list, T_list, r_list, price_list, option_type)
    #print(f"The implied volatilities are {implied_vols}")

def generate_powers_of_two(x):
    """
    This function generate a list from 1 to x.

    Parameters:
    x (int): max number.

    Returns:
    a list with 1 to x
    """
    return [n for n in range(1,x)]

def plot(num_cpus_list, times):
    """
    This function plot a time vs number of cpu.

    Parameters:
    num_cpus_list (list of int): The first number to add.
    times (list of timeit): The second number to add.

    Returns:
    graph of time vs cpu cores
    """
    # Create the plot
    plt.figure(figsize=(10, 6))
    plt.plot(num_cpus_list, times, marker='o')

    # Add labels and title
    plt.xlabel('Number of CPUs')
    plt.ylabel('Time to Complete (s)')
    plt.title('Benchmark Results')

    # Show the plot
    plt.show()

if __name__ == '__main__':
    
    # find number of CPUs on the machine

    num_cpus = multiprocessing.cpu_count()
    print(f"{num_cpus} pyhsical + virtual cores found on the pc")
    num_cpu_real_cores = psutil.cpu_count(logical=False)  
    print(f"{num_cpus} pyhsical cores found on the pc")

    # Define your worker pod specification
    pod_spec = make_pod_spec(image='ghcr.io/dask/dask:latest', memory_limit='4G', memory_request='4G', cpu_limit=1, cpu_request=1)

    # Create a KubeCluster with the pod spec
    cluster = KubeCluster(pod_spec)
    with Client(cluster) as client:
        num_cpus_list = [1]  # Number of CPUs
        times = []  # Time to complete

        # genearte cpu list <= num_cpus
        num_cpus_list = generate_powers_of_two(num_cpus//2)
        print(f"{num_cpus_list}")
        
        # Number of iterations
        num_runs = 1

        # Time the function
        start_time = timeit.default_timer()
        for _ in range(num_runs):
            run()
        end_time = timeit.default_timer()
        client.close()
        # Calculate the average run time
        average_run_time = (end_time - start_time) / num_runs
        times.append(average_run_time)
        print(f"The function took an average of {average_run_time:.10f} seconds over {num_runs} runs.")

    plot(num_cpus_list, times)