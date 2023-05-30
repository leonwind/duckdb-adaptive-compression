import csv
import matplotlib.pyplot as plt
import os

from benchmark_plotter import style, texify, colors
texify.latexify(3.39, 1.4)
style.set_custom_style()


SDSL_ID = "SDSL"
SDSL_BYTE_ALIGNED_ID = "SDSL-Byte-Aligned"
STD_ID = "STD"


def read_csv_file(filename):
    with open(filename) as f:
        reader = csv.reader(f, delimiter=",")

        csv_data = [row for row in reader if not row or not row[0].startswith("SUM")]
        #csv_data = [row for row in reader]
    
    # Delete tabs and white spaces    
    return [[c.replace("\t", "").replace(" ", "") for c in row] for row in csv_data]


def get_idx_for_columns(csv_data):
    headers = {}
    for i, param in enumerate(csv_data[0]):
        headers[param] = i

    return(headers)


def plot_instructions(data, column_idx):
    pass


def plot_cache_references(data, column_idx, name):
    fix, ax = plt.subplots(nrows=1, sharex=True, ncols=len(column_idx))

    for i in range(len(column_idx)):
        std_data = []
        sdsl_data = []
        sdsl_byte_aligned_data = []
        num_elements = []
        curr_elements = 2

        for row in data:
            entry = float(row[column_idx[i]]) 
            if row[0] == STD_ID:
                std_data.append(entry)
                num_elements.append(curr_elements)
                curr_elements *= 2
            elif row[0] == SDSL_ID:
                sdsl_data.append(entry)
            elif row[0] == SDSL_BYTE_ALIGNED_ID:
                sdsl_byte_aligned_data.append(entry)

        
        print(num_elements)
        print(std_data)
        print(sdsl_data)
        print(sdsl_byte_aligned_data)

        ax[i].plot(num_elements, sdsl_data, label="sdsl")
        ax[i].plot(num_elements, sdsl_byte_aligned_data, label="sdsl byte aligned")
        ax[i].plot(num_elements, std_data, label="std")

        ax[i].set_xscale("log")
        ax[i].set_ylabel(name[i])
        ax[i].set_xlabel("No.\ Elements")


    #ax.axvline(x=131072, ls='-.', color="blue")
    #ax.axvline(x=32768, ls='-.', color="green")

    #ax.axvline(x=8388608, ls='-.', color="blue")
    #ax.axvline(x=2097152, ls='-.', color="green")


    #ax.set_xscale("log")
    #ax.set_ylabel(name)
    #ax.set_xlabel("Number of Elements")
    #plt.legend()
    plt.tight_layout()
    #filename = name.lower().replace(" ", "_")
    plt.savefig(f"plots/llc-instructions-ipc.pdf")
    os.system("pdfcrop plots/llc-instructions-ipc.pdf plots/llc-instructions-ipc.pdf")
    #plt.show()


if __name__ == "__main__":
    csv_data = read_csv_file("../sdsl_tests/perf_results.csv")
    for row in csv_data:
        print(row)

    param_idx = get_idx_for_columns(csv_data)
    print(param_idx)
    plot_cache_references(
        csv_data[1:], 
        [param_idx["LLC-misses"], param_idx["instructions"], param_idx["IPC"]],
        ["LLC Misses", "Instructions", "IPC"]
    )

    #plot_cache_references(csv_data[1:], param_idx["cache-references"], "Cache References")
    #plot_cache_references(csv_data[1:], param_idx["LLC-misses"], "LLC Misses")
    #plot_cache_references(csv_data[1:], param_idx["instructions"], "Instructions")
    #plot_cache_references(csv_data[1:], param_idx["timesec"], "Timesec")
