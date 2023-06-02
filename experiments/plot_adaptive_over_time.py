import matplotlib.pyplot as plt
from benchmark_plotter import style, texify, colors
import os


texify.latexify(3.39, 1.3)
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
        qps_per_s.append((t / 60, int(qps)))
        memory_per_s.append((t / 60, int(memory) / 10**9))
        t += 1

    qps = [q[1] for q in qps_per_s]
    print("Avg qps:", sum(qps) / len(qps)) 
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
    plt.xlabel("Time [min]")
    plt.legend()
    plt.tight_layout()
    
    filename = ylabel.replace(" ", "_") + ".pdf"
    plt.savefig(filename, dpi=400)
    os.system(f"pdfcrop {filename} {filename}")

    plt.show()


if __name__ == "__main__":
    succinct_adaptive_timeseries = read_file("data/adaptive/update/succinct_adaptive_zipf.log")
    succinct_not_adaptive_timeseries = read_file("data/adaptive/update/succinct_non_adaptive_zipf.log")
    not_succinct_timeseries = read_file("data/adaptive/update/non_succinct_zipf.log")

    #succinct_adaptive_timeseries = read_file("data/adaptive/delta/succinct_adaptive_zipf.log")
    #succinct_not_adaptive_timeseries = read_file("data/adaptive/delta/succinct_non_adaptive_zipf.log")
    #not_succinct_timeseries = read_file("data/adaptive/delta/non_succinct_zipf.log")

    compare([not_succinct_timeseries[0], succinct_adaptive_timeseries[0], succinct_not_adaptive_timeseries[0]], "QPS")
    compare([not_succinct_timeseries[1], succinct_adaptive_timeseries[1], succinct_not_adaptive_timeseries[1]], "Size [GB]")
