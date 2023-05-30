import matplotlib.pyplot as plt
from benchmark_plotter import style, texify, colors


texify.latexify(3.39, 1.4)
style.set_custom_style()


def read_file(filename):
    with open(filename) as f:
        content = f.read().splitlines()

    qps_per_s = []
    memory_per_s = []
    t = 0
    for l in content:
        if l.startswith("Curr qps:"):
            continue
        qps, memory = l.split(", ")
        qps_per_s.append((t, int(qps)))
        memory_per_s.append((t, int(memory) / 10**9))
        t += 1
    
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


def compare(results, ylabel):
    _colors = [colors.colors["green"], colors.colors["red"], colors.colors["blue"]]
    for i, result in enumerate(results):
        plt.plot(*zip(*result), label=str(i), color=_colors[i])

    plt.ylabel(ylabel)
    plt.xlabel("Time [s]")
    #plt.legend()
    plt.tight_layout()
    filename = ylabel.replace(" ", "_")
    plt.savefig(f"{filename}.pdf", dpi=400)
    plt.show()


if __name__ == "__main__":
    succinct_adaptive_timeseries = read_file("data/adaptive/delta/succinct_adaptive_zipf.log")
    succinct_not_adaptive_timeseries = read_file("data/adaptive/delta/succinct_non_adaptive_zipf.log")
    not_succinct_timeseries = read_file("data/adaptive/delta/non_succinct_zipf.log")
    #compare([not_succinct_timeseries[0], succinct_adaptive_timeseries[0], succinct_not_adaptive_timeseries[0]], "QPS")
    #compare([not_succinct_timeseries[1], succinct_adaptive_timeseries[1], succinct_not_adaptive_timeseries[1]], "Memory [GB]")

    #succinct_adaptive_timeseries = read_file("../succinct_adaptive.log")
    #succinct_not_adaptive_timeseries = read_file("../succinct_not_adaptive.log")
    #not_succinct_timeseries = read_file("../non_succinct.log")
    compare([not_succinct_timeseries[0], succinct_adaptive_timeseries[0], succinct_not_adaptive_timeseries[0]], "QPS")
    compare([not_succinct_timeseries[1], succinct_adaptive_timeseries[1], succinct_not_adaptive_timeseries[1]], "Size [GB]")

    #compare([not_succinct_timeseries[1], succinct_adaptive_timeseries[1], succinct_not_adaptive_timeseries[1]], "Size [Bytes]")

    #not_succinct = read_file("data/non_succinct_zipf_changing.log")
    #succinct_adaptive = read_file("data/succinct_zipf_changing.log")
    #succinct_not_adaptive = read_file("data/succinct_not_adaptive_zipf_changing.log")
    #test = read_file("data/test_succinct_not_adaptive_zipf_changing.log")
    #compare_qps([not_succinct[0], succinct_adaptive[0], succinct_not_adaptive[0]])