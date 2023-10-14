import csv

import matplotlib.pyplot as plt
from benchmark_plotter import style, texify, colors
import os

texify.latexify(3.39, 1)
style.set_custom_style()

GROUPS = ["Succinct", "Succinct Padded", "Uncompressed"]
COLORS = {
    "Succinct": colors.colors["blue"],
    "Succinct Padded": colors.colors["orange"],
    "Uncompressed": colors.colors["green"]
}
PLOT_DIR = "micro_benchmark_plots"

LABEL_NAMES = {
    "Succinct": "Succinct bit packed",
    "Succinct Padded": "Succinct byte\npacked",
    "Uncompressed": "Uncompressed"
}


def _read_csv_file(filename):
    with open(filename) as f:
        reader = csv.reader(f, delimiter=',')
        csv_data = [row for row in reader]
    return csv_data    


def _parse_csv_data(data):
    runtimes_for_benchmark = {}
    initial_memory_per_benchmark = {}
    total_memory_per_benchmark = {}
    
    for benchmark in data[1:]:
        name, _, time_str, initial_memory_str, total_memory_str = benchmark 
        time = float(time_str)
        initial_memory = int(initial_memory_str)
        total_memory = int(total_memory_str)

        if name in runtimes_for_benchmark:
            runtimes_for_benchmark[name].append(time)
        else:
            runtimes_for_benchmark[name] = [time]
        
        initial_memory_per_benchmark[name] = initial_memory
        total_memory_per_benchmark[name] = total_memory

    avg_runtime_per_benchmark = {}
    for name, runtimes in runtimes_for_benchmark.items():
        avg_runtime_per_benchmark[name] = sum(runtimes) / len(runtimes)

    categories = [avg_runtime_per_benchmark, initial_memory_per_benchmark, total_memory_per_benchmark]
    return [_rename_groups(category) for category in categories]


def _extract_benchmark_groups(data):
    prefices = ["NonSuccinct", "SuccinctPadded", "Succinct"]
    benchmark_groups = {}
    for name, value in data.items():
        for prefix in prefices:
            if name.startswith(prefix):
                benchmark_type = prefix
                benchmark_groups[benchmark_type] = value
                break
    return benchmark_groups


def _rename_groups(data):
    data = _extract_benchmark_groups(data)

    if "NonSuccinct" in data:
        data["Uncompressed"] = data.pop("NonSuccinct")
    
    if "SuccinctPadded" in data:
        data["Succinct Padded"] = data.pop("SuccinctPadded")

    return data


def _plot(data, ylabel, name):
    fig = plt.figure()
    for group in GROUPS:
        if group not in data:
            continue
        plt.bar(group, data[group], color="dimgrey")

    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/{name}.pdf", dpi=400,)
    plt.show()


def _plot_side_by_side(perf_data, mem_data, name):
    fig, ax = plt.subplots(nrows=1, ncols=2)
    print(perf_data, mem_data)
    for group in GROUPS:
        if group in perf_data:
            ax[0].bar(group, perf_data[group], color=COLORS[group], label=group)
        
        if group in mem_data:
            ax[1].bar(group, mem_data[group], color=COLORS[group], label=LABEL_NAMES[group])

    ax[0].set_ylabel("Time [s]")
    ax[1].set_ylabel("Size [GB]")
    
    ax[0].set_xticks([])
    ax[1].set_xticks([])

    #ax[0].legend(loc="upper left", fontsize="4.3")

    #"""
    for i, bars in enumerate(ax[0].containers):
        if i == 0 and False:
            ax[0].bar_label(bars, fmt='%.1f')
        else:
            ax[0].bar_label(bars, label_type='center', fmt='%.1f')

    for bars in ax[1].containers:
        ax[1].bar_label(bars, label_type='center', fmt='%.1f')
    #"""
    
    handles, labels = ax[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.56, 1.01),
            borderaxespad=0.2, ncol=3, prop={'size': 8})
    
    print(fig.get_size_inches()) 
    fig.tight_layout()
    fig.set_size_inches(3.39, 1.3)
    plt.subplots_adjust(top=0.84)
    print(fig.get_size_inches()) 

    #plt.tight_layout()
    path = f"{PLOT_DIR}/{name}.pdf"
    plt.savefig(path, dpi=400)
    os.system(f"pdfcrop {path} {path}")
    #plt.show()


def plot_sequential_scan():
    runtime, _, memory = _parse_csv_data(_read_csv_file("data/succinct_sequential_scan.csv"))
    for name, mem in memory.items():
        memory[name] = mem / 10**9

    #_plot(runtime, "Time [s]", "runtime_sequential_scan")
    #_plot(memory, "Memory [GB]", "memory_sequential_scan")
    _plot_side_by_side(runtime, memory, "sequential_scan_side_by_side")


def plot_zipf_lookup():
    runtime, _, memory = _parse_csv_data(_read_csv_file("data/zipf_lookup.csv"))
    for name, mem in memory.items():
        memory[name] = mem / 10**9
    
    _plot_side_by_side(runtime, memory, "zipf_side_by_side")
    #_plot(runtime, "Time [s]", "runtime_zipf")
    #_plot(memory, "Memory [GB]", "memory_zipf")


def plot_scan_oom():
    runtime, _, memory = _parse_csv_data(_read_csv_file("data/scan_oom.csv"))
    for name, mem in memory.items():
        memory[name] = mem / 10**9
    
    _plot_side_by_side(runtime, memory, "oom_side_by_side")
    #_plot(runtime, "Time [s]", "runtime_oom_scan")
    #_plot(memory, "Memory [GB]", "memory_oom_scan")


if __name__ == "__main__":
    plot_sequential_scan()
    #plot_zipf_lookup()
    #plot_scan_oom()