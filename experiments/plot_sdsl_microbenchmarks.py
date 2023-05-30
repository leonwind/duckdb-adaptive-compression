import matplotlib.pyplot as plt
from benchmark_plotter import style, texify, colors
import os
import math

# Intel(R) Core(TM) i9-7900X CPU @ 3.30GHz
L1_CACHE_SIZE = 320 << 10
L3_CACHE_SIZE = 1.447e7

texify.latexify(3.39, 1.25)
style.set_custom_style()


class VectorCaches:
    def __init__(self, l1_sdsl, l1_std, l3_sdsl, l3_std):
        self.l1_sdsl = l1_sdsl
        self.l1_std = l1_std
        self.l3_sdsl = l3_sdsl
        self.l3_std = l3_std


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
    l1_sdsl, l1_std = _find_number_elements_fitting_cache(data, L1_CACHE_SIZE)
    l3_sdsl, l3_std = _find_number_elements_fitting_cache(data, L3_CACHE_SIZE)
    caches = VectorCaches(l1_sdsl, l1_std, l3_sdsl, l3_std)

    # _plot(data["num_elements"], [
    #    ("SDSL Duration", data["sdsl_duration"]),
    #    ("SDSL Duration Padded", data["sdsl_duration_padded"]),
    #    ("STD Duration", data["std_duration"])
    # ], "sdsl_std_duration", caches, ylabel="Time [ms]", show_cache_borders=True, convert_data=1e6)

    # _plot(data["num_elements"], [
    #    ("SDSL Compressed Size", data["sdsl_size_compressed"]),
    #    ("SDSL Compressed Size Padded", data["sdsl_size_compressed_padded"]),
    #    ("STD Size", data["std_size"])
    # ], "sdsl_std_memory", caches, ylabel="Size [KiB]", convert_data=1024)

    _plot_ratios_side_by_side(
        data["num_elements"],
        data["sdsl_duration"],
        data["std_duration"],
        data["sdsl_size_compressed"],
        data["std_size"])


def _add_cache_label(axis, figure, color, label, x, y):
    axis.text(x, y, label,
              ha="center",
              color=color,
              size=7,
              transform=figure.transFigure,
              bbox=dict(boxstyle='round,pad=0.2', linewidth=0.4, fc="w", ec=color))


def _plot(num_elements, data_per_ds, name, caches: VectorCaches, xlabel="", ylabel="", show_cache_borders=False, convert_data=1.):
    fig, ax = plt.subplots()

    # ratio = []
    # for idx in range(len(data_per_ds[0][1])):
    # print(data_per_ds[0][1][idx])
    # ratio.append(data_per_ds[0][1][idx] / data_per_ds[2][1][idx])

    for data in data_per_ds:  # convert from ns to ms
        ax.plot(num_elements, [a / convert_data for a in data[1]], label=data[0])

    xlim = 9e9
    ax.set_xlim([1, xlim])
    # ax2 = ax.twinx()
    # ax2.plot(num_elements, ratio, color="red", ls=":")

    if show_cache_borders:
        ax.axvline(x=caches.l1_sdsl, linewidth=1, ls='--', color=colors.colors["blue"])
        ax.axvline(x=caches.l1_std, linewidth=1, ls='--', color=colors.colors["green"])
        _add_cache_label(ax, fig, colors.colors["blue"], "L1", math.log(caches.l1_sdsl) / math.log(xlim) + .065, .7)
        _add_cache_label(ax, fig, colors.colors["green"], "L1", math.log(caches.l1_std) / math.log(xlim) + .076, .7)

        ax.axvline(x=caches.l3_sdsl, linewidth=1, ls='--', color=colors.colors["blue"])
        ax.axvline(x=caches.l3_std, linewidth=1, ls='--', color=colors.colors["green"])
        _add_cache_label(ax, fig, colors.colors["blue"], "L3", math.log(caches.l3_sdsl) / math.log(xlim) + 0.026, .5)
        _add_cache_label(ax, fig, colors.colors["green"], "L3", math.log(caches.l3_std) / math.log(xlim) + 0.038, .5)

    # plt.legend()
    # ax.set_xlabel("Number of Elements")
    ax.set_ylabel(ylabel)
    ax.set_xscale("log")
    ax.set_yscale("log")
    plt.tight_layout()
    plt.savefig(f"plots/{name}.pdf")
    os.system(f"pdfcrop plots/{name}.pdf")


def _plot_ratios_side_by_side(num_elements, sdsl_performance, std_performance, sdsl_size, std_size):
    fig, axis = plt.subplots(nrows=1, ncols=2)

    performance_ratio = []
    size_ratio = []

    for i in range(len(sdsl_performance)):
        performance_ratio.append(sdsl_performance[i] / std_performance[i])
        size_ratio.append(sdsl_size[i] / std_size[i])

    axis[0].plot(num_elements, performance_ratio, color="red")
    axis[0].set_xscale("log")
    axis[0].set_xlabel("No.\ Elements")
    axis[0].set_ylabel("Perf.\ Ratio")

    axis[0].axvline(x=131072, ls='-', linewidth=1, color=colors.colors["blue"])
    axis[0].axvline(x=32768, ls='-', linewidth=1, color=colors.colors["green"])
    axis[0].axvline(x=8388608, ls='-', linewidth=1, color=colors.colors["blue"])
    axis[0].axvline(x=2097152, ls='-', linewidth=1, color=colors.colors["green"])

    axis[1].plot(num_elements, size_ratio, color="red")
    axis[1].set_xscale("log")
    axis[1].set_xlabel("No.\ Elements")
    axis[1].set_ylabel("Size Ratio")

    plt.tight_layout()
    plt.savefig("plots/sdsl_std_ratios_side_by_side.pdf")
    os.system("pdfcrop plots/sdsl_std_ratios_side_by_side.pdf plots/sdsl_std_ratios_side_by_side.pdf")
    # plt.show()


if __name__ == "__main__":
    data = read_data("../sdsl_tests/sdsl_sum_benchmark.log")
    # print(data["sdsl_size_compressed"])
    # print(data["sdsl_size_compressed_padded"])

    create_plots(data)
