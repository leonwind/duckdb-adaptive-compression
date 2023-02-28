import csv
import matplotlib as mpl
# Use pgf backend to directly export to LaTeX.
# Must be set before importing pyplot.
mpl.use('pgf')

import matplotlib.pyplot as plt

# Use LaTeX font
plt.rcParams.update({
    "font.family": "serif",  # use serif/main font for text elements
    "text.usetex": True,     # use inline math for ticks
    "pgf.rcfonts": False     # don't setup fonts from rc parameters
})


LATEX_FRAME_WIDTH =  307.28987


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

def set_size(width_pt, fraction=1, subplots=(1, 1)):
    """Set figure dimensions to sit nicely in our document.

    Parameters
    ----------
    width_pt: float
            Document width in points
    fraction: float, optional
            Fraction of the width which you wish the figure to occupy
    subplots: array-like, optional
            The number of rows and columns of subplots.
    Returns
    -------
    fig_dim: tuple
            Dimensions of figure in inches
    """
    # Width of figure (in pts)
    fig_width_pt = width_pt * fraction
    # Convert from pt to inches
    inches_per_pt = 1 / 72.27

    # Golden ratio to set aesthetic figure height
    golden_ratio = (5**.5 - 1) / 2

    # Figure width in inches
    fig_width_in = fig_width_pt * inches_per_pt
    # Figure height in inches
    fig_height_in = fig_width_in * golden_ratio * (subplots[0] / subplots[1])

    return (fig_width_in, fig_height_in)

def _plot_list_as_barchart(data, ylabel="", title=""):
    #fig = plt.figure()
    fig = plt.figure(figsize=set_size(LATEX_FRAME_WIDTH))
    plt.title(title)
    plt.bar(*zip(*data))
    plt.ylabel(ylabel)
    filename = title.replace(" ", "_")
    plt.savefig(f"plots/pgf/{filename}.pgf", format="pgf")
    plt.savefig(f"plots/{filename}.png", dpi=fig.dpi)
    plt.close()
    #plt.show()


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
        _plot_list_as_barchart(values, ylabel="Time in [s]", title=f"Runtime of {name}")


def _plot_benchmark_group_initial_memory(initial_memory_per_benchmark):
    benchmark_groups = _extract_benchmark_groups(initial_memory_per_benchmark)
    for name, values in benchmark_groups.items():
        _plot_list_as_barchart(values, ylabel="Memory in Bytes", title=f"Initial Memory of {name}")


def _plot_benchmark_group_total_memory(total_memory_per_benchmark):
    benchmark_groups = _extract_benchmark_groups(total_memory_per_benchmark)
    for name, values in benchmark_groups.items():
        _plot_list_as_barchart(values, ylabel="Memory in Bytes", title=f"Total Memory of {name}")


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
        
        _plot_list_as_barchart(initial_and_total, ylabel="Memory in Bytes", title=f"Initial and Total Memory of {name}")


if __name__ == "__main__":
    data = read_csv_file("../benchmarks.csv")
    print(data)
    plot_benchmarks(data)
