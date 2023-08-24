import matplotlib.pyplot as plt
from benchmark_plotter import style, texify, colors
import os


texify.latexify(3.39, 1.3)
style.set_custom_style()

GB = 10**9
MB = 10**6


def read_file(filename):
    with open(filename) as f:
        content = f.read().splitlines()

    qps_per_s = []
    memory_per_s = []
    workload_times = []

    t = 0
    for l in content:
        if l.startswith("Start workload"):
            workload_times.append(t / 60)
            continue

        qps, memory = l.split(", ")
        qps_per_s.append((t / 60, int(qps)))
        memory_per_s.append((t / 60, int(memory) / MB))
        t += 1

    qps = [q[1] for q in qps_per_s]
    print("Avg qps:", sum(qps) / len(qps)) 
    return qps_per_s, memory_per_s, workload_times


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


def compare(results, workload_times, ylabel):
    _colors = [colors.colors["green"], colors.colors["red"], colors.colors["blue"]]
    for i, result in enumerate(results):
        plt.plot(*zip(*result), label=str(i), color=_colors[i])

    for i, workload in enumerate(workload_times):
        for phase, time in enumerate(workload[1:]):
            plt.axvline(x=time, color=_colors[(i + 1) % 3], ls="dashed")

            #plt.text(time, 3000, str(phase + 1),
            #    ha="center",
            #    color=_colors[(i + 2) % 3],
            #    size=7,
            #    bbox=dict(boxstyle='round,pad=0.2', linewidth=0.4, fc="w", ec=_colors[(i + 2) % 3]))

    plt.ylabel(ylabel)
    plt.xlabel("Time [min]")
    #plt.legend()
    plt.tight_layout()

    
    filename = "fb_workloads_" + ylabel.replace(" ", "_") + ".pdf"
    plt.savefig(filename, dpi=400)
    os.system(f"pdfcrop {filename} {filename}")

    plt.show()


def _add_cache_label(color, label, x, y):
    plt.text(x, y, label,
              ha="center",
              color=color,
              size=7,
              bbox=dict(boxstyle='round,pad=0.2', linewidth=0.4, fc="w", ec=color))


if __name__ == "__main__":
    succinct_adaptive_timeseries = read_file("data/fb_workloads/fb_adaptive.txt")
    succinct_not_adaptive_timeseries = read_file("data/fb_workloads/fb_succinct.txt")
    not_succinct_timeseries = read_file("data/fb_workloads/fb_non_succinct.txt")

    workload_switching_times = [succinct_adaptive_timeseries[2], succinct_not_adaptive_timeseries[2], not_succinct_timeseries[2]]

    compare(
        [not_succinct_timeseries[0], succinct_adaptive_timeseries[0], succinct_not_adaptive_timeseries[0]], 
        workload_switching_times,
        "QPS")

    compare(
        [not_succinct_timeseries[1], succinct_adaptive_timeseries[1], succinct_not_adaptive_timeseries[1]], 
        workload_switching_times, 
        "Size [MB]")
