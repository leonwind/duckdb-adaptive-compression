import os
import matplotlib.pyplot as plt

from benchmark_plotter import style, texify, colors

texify.latexify(3.39, 1.4)
style.set_custom_style()


DATA_PATH = "data/diff_zipf_skews/"

ADAPTIVE_IDX = 0
SUCCINCT_IDX = 1
DEFAULT_IDX = 2

COLORS = {
    "succinct": colors.colors["blue"],
    "adaptive": colors.colors["red"],
    "uncompressed": colors.colors["green"]
}

GB = 10**9
MB = 10**6


def read_data():
    data = {}

    for sub_dir, _, files in os.walk(DATA_PATH):
        for curr_file in files:
            dir_name = sub_dir.split("/")[-1]
            skew = int(dir_name.split("_")[-1]) / 10
            setting = curr_file.split(".")[0]

            curr_path = os.path.join(sub_dir, curr_file)
            with open(curr_path, 'r') as f:
                curr_data = f.read().splitlines()

            qps = []
            memory = []

            for entry in curr_data:
                curr_qps, curr_memory = entry.split(", ")
                qps.append(int(curr_qps))
                memory.append(int(curr_memory))

            avg_qps = sum(qps) / len(qps)
            final_memory = memory[-1]

            if skew not in data:
                data[skew] = [(), (), ()]

            if setting == "adaptive":
                data[skew][ADAPTIVE_IDX] = (avg_qps, final_memory)
            elif setting == "non_adaptive":
                data[skew][SUCCINCT_IDX] = (avg_qps, final_memory)
            elif setting == "non_succinct":
                data[skew][DEFAULT_IDX] = (avg_qps, final_memory)
            else:
                raise Exception("Unkown Setting found.")
    
    return data


def plot_data(data_dict):
    data = sorted(data_dict.items())

    adaptive = []
    succinct = []
    default = []

    for curr in data:
        adaptive.append((curr[0], curr[1][ADAPTIVE_IDX][0]))
        succinct.append((curr[0], curr[1][SUCCINCT_IDX][0]))
        default.append((curr[0], curr[1][DEFAULT_IDX][0]))

    fig, ax = plt.subplots(nrows=1, ncols=2)
    ax[0].plot(*zip(*default),color=COLORS["uncompressed"], label="Uncompressed")
    ax[0].plot(*zip(*adaptive), color=COLORS["adaptive"], label="Adaptive")
    ax[0].plot(*zip(*succinct), color=COLORS["succinct"], label="Succinct")

    ax[0].set_xlabel("Skew factor $k$")
    ax[0].set_ylabel("QPS")

    ax[0].legend(loc="lower right", fontsize="5.2")

    final_mem = data[-1]
    mem_data = [
        final_mem[1][DEFAULT_IDX][1] / MB,
        final_mem[1][ADAPTIVE_IDX][1] / MB,
        final_mem[1][SUCCINCT_IDX][1] / MB
    ]

    ax[1].bar("Uncompressed", mem_data[0], color=COLORS["uncompressed"])
    ax[1].bar("Adaptive", mem_data[1],     color=COLORS["adaptive"])
    ax[1].bar("Succinct", mem_data[2],     color=COLORS["succinct"])
    ax[1].set_xticks([])
    ax[1].set_ylabel("Size [MB]")

    for bars in ax[1].containers:
        ax[1].bar_label(bars, label_type='center', fmt='%.1f')

    fig.tight_layout()

    filename = "diff_zipf_skew.pdf"
    fig.savefig(filename, dpi=400)
    os.system(f"pdfcrop {filename} {filename}")

    plt.show()


if __name__ == "__main__":
    data = read_data()
    print(data)

    plot_data(data)