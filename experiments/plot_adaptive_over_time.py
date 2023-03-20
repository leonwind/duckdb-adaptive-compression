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


if __name__ == "__main__":
    non_succinct_data = read_file("data/non_adaptive_succinct_over_time.txt")
    succinct_data = read_file("data/adaptive_succinct_over_time.txt")
    
    plot_qps_memory_over_time(succinct_data[0], succinct_data[1])
    plot_qps_memory_over_time(non_succinct_data[0], non_succinct_data[1])
