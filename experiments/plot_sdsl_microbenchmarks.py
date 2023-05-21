import matplotlib.pyplot as plt 
from benchmark_plotter import style, texify, colors

texify.latexify(3.39, 1.4)
style.set_custom_style()

def read_data(filename):
    with open(filename) as f:
        content = f.read().splitlines()
    return _parse_data(content) 
    

def _parse_data(file_content):
    headers = [
        "i", "num_elements", 
        "sdsl_size_uncompressed", "sdsl_size_compressed", "sdsl_duration",
        "sdsl_size_uncompressed_padded", "sdsl_size_compressed_padded", "sdsl_duration_padded",
        "std_size", "std_duration"
    ]

    data = {}
    for header in headers:
        data[header] = []

    values = []
    for line in file_content:
        if line.startswith("Sum: "):
            continue

        if line.startswith("i:"):
            idx_sub = line.split(",")[0]
            idx = idx_sub.split(" ")[1]
            num_elements = line.split("elements: ")[1]
            values.extend([idx, num_elements])
        else:
            value = line.split(": ")[1]
            values.append(value)

    for i, val in enumerate(values):
        data[headers[i % len(headers)]].append(int(val))

    return data


def _find_number_elements_fitting_cache(data, cache_size):
    sdsl_compressed_sizes = data["sdsl_size_compressed"]
    sdsl_idx = 0

    while sdsl_compressed_sizes[sdsl_idx + 1] < cache_size:
        sdsl_idx += 1
    print(sdsl_compressed_sizes[sdsl_idx])

    std_sizes = data["std_size"]
    std_idx = 0
    while std_sizes[std_idx + 1] < cache_size:
        std_idx += 1
    print(std_sizes[std_idx]) 

    return data["num_elements"][sdsl_idx], data["num_elements"][std_idx]
    


def create_plots(data):
    """
    _plot(data["num_elements"], [
        ("SDSL Duration", data["sdsl_duration"]),
        ("SDSL Duration Padded", data["sdsl_duration_padded"]),
        ("STD Duration", data["std_duration"])
        ], "sdsl_std_duration", ylabel="Duration [ns]", show_cache_borders=True)
    """

    """
    _plot(data["num_elements"], [
        ("SDSL Compressed Size", data["sdsl_size_compressed"]),
        ("SDSL Compressed Size Padded", data["sdsl_size_compressed_padded"]),
        ("STD Size", data["std_size"])
        ], "sdsl_std_memory", ylabel="Size [Bytes]")
    """

    #"""
    _plot_ratios_side_by_side(
        data["num_elements"],
        data["sdsl_duration"],
        data["std_duration"],
        data["sdsl_size_compressed"],
        data["std_size"]
    ) 
    #"""


def _plot(num_elements, data_per_ds, name, xlabel="", ylabel="", show_cache_borders=False):

    fig, ax = plt.subplots()

    #ratio = [] 
    #for idx in range(len(data_per_ds[0][1])):
        #print(data_per_ds[0][1][idx])
        #ratio.append(data_per_ds[0][1][idx] / data_per_ds[2][1][idx])


    for data in data_per_ds:
        ax.plot(num_elements, data[1], label=data[0])

    #ax2 = ax.twinx()
    #ax2.plot(num_elements, ratio, color="red", ls=":")

    if show_cache_borders:
        ax.axvline(x=131072, ls='-.', color="blue")
        ax.axvline(x=32768, ls='-.', color="green")

        ax.axvline(x=8388608, ls='-.', color="blue")
        ax.axvline(x=2097152, ls='-.', color="green")

    #plt.legend()
    ax.set_xlabel("Number of Elements")
    ax.set_ylabel(ylabel)
    ax.set_xscale("log") 
    ax.set_yscale("log")
    plt.tight_layout()
    plt.savefig(f"plots/{name}.pdf", dpi=400)
    plt.show()


def _plot_ratios_side_by_side(num_elements, sdsl_performance, std_performance, sdsl_size, std_size):
    fig, axis = plt.subplots(nrows=1, ncols=2)

    performance_ratio = []
    size_ratio = []

    for i in range(len(sdsl_performance)):
        performance_ratio.append(sdsl_performance[i] / std_performance[i])
        size_ratio.append(sdsl_size[i] / std_size[i])
    
    axis[0].plot(num_elements, performance_ratio, color="red")
    axis[0].set_xscale("log") 
    axis[0].set_xlabel("Number of Elements")
    axis[0].set_ylabel("Performance Ratio")

    axis[0].axvline(x=131072, ls='-.', color="blue")
    axis[0].axvline(x=32768, ls='-.', color="green")
    axis[0].axvline(x=8388608, ls='-.', color="blue")
    axis[0].axvline(x=2097152, ls='-.', color="green")

    axis[1].plot(num_elements, size_ratio, color="red")
    axis[1].set_xscale("log")
    axis[1].set_xlabel("Number of Elements")
    axis[1].set_ylabel("Size Ratio")

    plt.tight_layout() 
    plt.savefig(f"plots/sdsl_std_ratios_side_by_side.pdf", dpi=400)
    plt.show()

if __name__ == "__main__":
    data = read_data("../sdsl_tests/sdsl_sum_benchmark.log")
    #print(data["sdsl_size_compressed"])
    #print(data["sdsl_size_compressed_padded"])
    
    # L1 Cache size:
    print(_find_number_elements_fitting_cache(data, 327680))
    # Total Cache size:
    print(_find_number_elements_fitting_cache(data, 25283788))

    create_plots(data)
