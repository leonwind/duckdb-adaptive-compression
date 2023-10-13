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
    workload_times.append((t - 1) / 60)

    qps = [q[1] for q in qps_per_s]
    print("Avg qps:", sum(qps) / len(qps)) 
    return qps_per_s, memory_per_s, workload_times


def compare(results, workload_times, ylabel):
    _colors = [colors.colors["green"], colors.colors["red"], colors.colors["blue"]]
    labels = ["Uncompressed", "Adaptive", "Succinct"]

    for i, result in enumerate(results):
        plt.plot(*zip(*result), color=_colors[i], label=labels[i])

    for i, workload in enumerate(workload_times):
        for phase, time in enumerate(workload[1:]):
            plt.axvline(x=time, color=_colors[(i + 1) % 3], ls="dashed")

    #plt.text(1.45, 2800, 
    #    "$w_{1}$",
    #    ha="center",
    #    color="black",
    #    size=4,
    #    bbox=dict(boxstyle='round,pad=0.2', linewidth=0.4, fc="w", ec="black"))

    #plt.text(2.9, 2800, 
    #    "$w_{2}$",
    #    ha="center",
    #    color="black",
    #    size=4,
    #    bbox=dict(boxstyle='round,pad=0.2', linewidth=0.4, fc="w", ec="black"))

    #plt.text(4.4, 2800, 
    #    "$w_{3}$",
    #    ha="center",
    #    color="black",
    #    size=4,
    #    bbox=dict(boxstyle='round,pad=0.2', linewidth=0.4, fc="w", ec="black"))


    #plt.legend(loc="upper center", bbox_to_anchor=(0.5, 1.15), ncol=3)
    plt.legend(bbox_to_anchor=(0, 1.02, 1, 0.2), loc="lower left",
                mode="expand", borderaxespad=0.2, ncol=3, prop={'size': 8})

    plt.tick_params(bottom=False)
    plt.xticks(range(6), [])

    plt.ylabel(ylabel)
    #plt.xlabel("Time [min]")
    #plt.legend()
    plt.tight_layout()

    
    filename = "fb_workloads_" + ylabel.replace(" ", "_") + ".pdf"
    plt.savefig(filename, dpi=400)
    os.system(f"pdfcrop {filename} {filename}")

    #plt.show()


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

    #compare(
    #    [not_succinct_timeseries[1], succinct_adaptive_timeseries[1], succinct_not_adaptive_timeseries[1]], 
    #    workload_switching_times, 
    #    "Size [MB]")
