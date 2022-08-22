import numpy as np

input_filename = "/media/gdls/gdls-data/gdls-building10-test-track/2017-11-23/digitizer-data/2017-11-23-1445--20MSPS--800mV--round11-ecm"
num_samples_to_read_per_batch = 1000000
input_chunk_size = 500


def data_source():
    with open(input_filename, "rb") as input_file: 
        while True:
            bytes_to_read = num_samples_to_read_per_batch * 2
            bytes_read = input_file.read(bytes_to_read)

            if(len(bytes_read) < bytes_to_read):
                #input_file.seek(0)
                #bytes_read = input_file.read(bytes_to_read)
                break

            # load the data as unsigned short, convert to float32 in [-1,1]
            input_data_buffer = (np.frombuffer(bytes_read, '<H').astype(np.int32) - 32768).astype(np.float32) / 32768.0
            total_chunks = np.array(np.split(input_data_buffer, input_data_buffer.shape[0] / input_chunk_size))

            yield total_chunks
df = pd.DataFrame()
for x in data_source():
    temp = pd.DFataFrame(x)
    df = pd.concat([df, temp], axis=1)

df.to_csv("1511448286_traces.csv")
#print(x.shape) # do whatever here
