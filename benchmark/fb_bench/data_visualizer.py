import matplotlib.pyplot as plt

QUERY_SIZE_BYTES = 32


def read_workload(path):
    u_ids = {}
    i = 0

    with open(path, "rb") as f:
        size = f.read(8)

        while (curr_bytes := f.read(QUERY_SIZE_BYTES)):

            #query = int.from_bytes(curr_bytes[0:8], byteorder="little")
            key = int.from_bytes(curr_bytes[8:16], byteorder="little")
            #key_range_upper = int.from_bytes(curr_bytes[16:24], byteorder="little")
            #value = int.from_bytes(curr_bytes[24:32], byteorder="little")

            #print(query, key, key_range_upper, value)

            #if i >= 200:
            #    break

            if key not in u_ids:
                u_ids[key] = 0
            u_ids[key] += 1

            i += 1
    
    return u_ids


def plot_histogram(data):
    plt.plot(*zip(*data))
    plt.show()


if __name__ == "__main__":
    #data = read_workload("prefix-random-parser/data/prefix-random-53M_uint64")
    for i in range(3):
        data = read_workload("prefix-random-parser/data/workload" + str(i))
        print(len(data))
        
        sorted_data = sorted(data.items(), key=lambda item: item[1], reverse=True)
        print(sorted_data[:100])

        data_to_plot = []
        for i, x in enumerate(sorted_data):
            data_to_plot.append((i, x[1]))

        plot_histogram(data_to_plot)
    