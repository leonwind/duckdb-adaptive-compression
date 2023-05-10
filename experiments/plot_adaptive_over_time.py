import matplotlib.pyplot as plt


def read_file(filename):
    with open(filename) as f:
        content = f.read().splitlines()

    qps_per_s = []
    memory_per_s = []
    for t, l in enumerate(content):
        qps, memory = l.split(", ")
        qps_per_s.append((t, int(qps)))
        memory_per_s.append((t, int(memory)))
    
    return qps_per_s, memory_per_s


def plot_qps_memory_over_time(qps_per_s, memory_per_s):
    fig, ax1 = plt.subplots()
    
    color = "blue"
    ax1.set_xlabel("time [s]")
    ax1.set_ylabel("QPS", color=color)
    ax1.plot(*zip(*qps_per_s), color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()  
    color = "orange"

    ax2.set_ylabel("Memory", color=color)
    ax2.plot(*zip(*memory_per_s), color=color)
    ax2.tick_params(axis='y', labelcolor=color)

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()


def compare_qps(results):
    for result in results:
        plt.plot(*zip(*result))
    plt.show()


if __name__ == "__main__":
    vanilla_duckdb = read_file("../vanilla_duckdb.log")
    compare_qps([vanilla_duckdb[0]])

    short_data = read_file("../kemper_adaptive.log")
    #plot_qps_memory_over_time(short_data[0], short_data[1])

    non_succinct = read_file("../kemper_non_succinct.log")

    succinct_adaptive_70 = read_file("../kemper_succinct_70.log")

    succinct_0_compression = read_file("../succinct_0_compression.log")
    #plot_qps_memory_over_time(short_data[0], short_data[1])
    compare_qps([short_data[0], non_succinct[0], succinct_adaptive_70[0], succinct_0_compression[0]]) 
    compare_qps([short_data[1], non_succinct[1], succinct_adaptive_70[1]])
    

    #adaptive_succinct_data = read_file("../build/changing_distribution.log")
    #plot_qps_memory_over_time(adaptive_succinct_data[0], adaptive_succinct_data[1])

    #adaptive_succinct_data = read_file("../build/changing_distribution_no_lock.log")
    #plot_qps_memory_over_time(adaptive_succinct_data[0], adaptive_succinct_data[1])

    #adaptive_succinct_data = read_file("../build/changing_distribution_8_threads.log")
    #plot_qps_memory_over_time(adaptive_succinct_data[0], adaptive_succinct_data[1])

    #adaptive_succinct_data = read_file("../build/changing_distribution_large.log")
    #plot_qps_memory_over_time(adaptive_succinct_data[0], adaptive_succinct_data[1])

    #not_adaptive_succinct_data = read_file("data/succinct_not_adaptive_over_time_large.txt")
    #plot_qps_memory_over_time(not_adaptive_succinct_data[0], not_adaptive_succinct_data[1])

    #not_succinct_data = read_file("data/not_succinct_over_time_large.txt")
    #plot_qps_memory_over_time(not_succinct_data[0], not_succinct_data[1])
