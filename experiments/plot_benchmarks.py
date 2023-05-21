import csv
# Use pgf backend to directly export to LaTeX.
# Must be set before importing pyplot.
#mpl.use('pgf')


import matplotlib.pyplot as plt
from benchmark_plotter import style, texify, colors

# Use LaTeX font
#plt.rcParams.update({
#    "font.family": "serif",  # use serif/main font for text elements
#    "text.usetex": True,     # use inline math for ticks
#    "pgf.rcfonts": False     # don't setup fonts from rc parameters
#})

texify.latexify(3.39, 1.4)
style.set_custom_style()


#LATEX_FRAME_WIDTH =  307.28987


def read_csv_file(filename):
    with open(filename) as f:
        reader = csv.reader(f, delimiter='\t')
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

    return avg_runtime_per_benchmark, initial_memory_per_benchmark, total_memory_per_benchmark


def plot_benchmarks(csv_data):
    avg_runtime_per_benchmark, initial_memory_per_benchmark, total_memory_per_benchmark = _parse_csv_data(csv_data)

    _plot_benchmark_group_runtime(avg_runtime_per_benchmark)
    _plot_benchmark_group_initial_memory(initial_memory_per_benchmark)
    _plot_benchmark_group_total_memory(total_memory_per_benchmark)
    _plot_initial_and_total_memory(initial_memory_per_benchmark, total_memory_per_benchmark)

    #_plot_list_as_barchart(avg_runtime_per_benchmark.items(), ylabel="Time in [s]")
    #_plot_list_as_barchart(initial_memory_per_benchmark.items(), ylabel="Memory in Bytes")
    #_plot_list_as_barchart(total_memory_per_benchmark.items(), ylabel="Memory in Bytes")

def _plot_list_as_barchart(data, ylabel="", title=""):
    fig = plt.figure()
    #plt.title(title)
    plt.bar(*zip(*data))
    plt.ylabel(ylabel)

    plt.tight_layout()
    filename = title.replace(" ", "_")
    plt.savefig(f"plots/{filename}.pdf", dpi=fig.dpi)

    #plt.show()
    #plt.close()


def _extract_benchmark_groups(data):
    prefices = ["NonSuccinct", "SuccinctPadded", "Succinct"]
    benchmark_groups = {}
    for name, value in data.items():
        for prefix in prefices:
            if name.startswith(prefix):
                benchmark_type = name[len(prefix):]
                if benchmark_type in benchmark_groups:
                    benchmark_groups[benchmark_type].append((prefix, value))
                else:
                    benchmark_groups[benchmark_type] = [(prefix, value)]
                break
    return benchmark_groups


def _plot_benchmark_group_runtime(runtimes_per_benchmark):
    benchmark_groups = _extract_benchmark_groups(runtimes_per_benchmark)
    for name, values in benchmark_groups.items():
        _plot_list_as_barchart(values, ylabel="Time [s]", title=f"Runtime of {name}")


def _plot_benchmark_group_initial_memory(initial_memory_per_benchmark):
    benchmark_groups = _extract_benchmark_groups(initial_memory_per_benchmark)
    for name, values in benchmark_groups.items():
        _plot_list_as_barchart(values, ylabel="Size [Bytes]", title=f"Initial Memory of {name}")


def _plot_benchmark_group_total_memory(total_memory_per_benchmark):
    benchmark_groups = _extract_benchmark_groups(total_memory_per_benchmark)
    for name, values in benchmark_groups.items():
        _plot_list_as_barchart(values, ylabel="Size [Bytes]", title=f"Total Memory of {name}")


def _plot_initial_and_total_memory(initial_memory_per_benchmark, total_memory_per_benchmark):
    benchmark_groups_initial = _extract_benchmark_groups(initial_memory_per_benchmark)
    benchmark_groups_total = _extract_benchmark_groups(total_memory_per_benchmark)
    for name, initial_values in benchmark_groups_initial.items():
        total_values = benchmark_groups_total[name]
        initial_and_total = []

        for i in range(len(initial_values)):
            initial_and_total.append((initial_values[i][0] + "Initial", initial_values[i][1]))
            initial_and_total.append((total_values[i][0] + "Total", total_values[i][1]))
        print(initial_and_total)
        
        _plot_list_as_barchart(initial_and_total, ylabel="Size [Bytes]", title=f"Initial and Total Memory of {name}")


if __name__ == "__main__":
    data = read_csv_file("../benchmarks.csv")
    for l in data:
        print(l)
    #print(data)
    plot_benchmarks(data)
