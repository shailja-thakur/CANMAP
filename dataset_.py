"""module to read data."""
import numpy
import collections
from tensorflow.python.framework import dtypes
import struct
import os
import numpy as np
import pandas as pd
from data_utils import filename, l2_norm
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn import preprocessing

class DataSet(object):
    """Dataset class object."""

    def __init__(self,
                 traces,
                 labels,
                 fake_data=False,
                 one_hot=False,
                 dtype=dtypes.float64,
                 reshape=True):
        """Initialize the class."""
        # if reshape:
        #     assert images.shape[3] == 1
        #     images = images.reshape(images.shape[0],
        #         images.shape[1] * images.shape[2])

        self._traces = traces
        self._num_examples = traces.shape[0]
        self._labels = labels
        self._epochs_completed = 0
        self._index_in_epoch = 0
        self.format = 'pkl'
    @property
    def traces(self):
        return self._traces

    @property
    def labels(self):
        return self._labels

    @property
    def num_examples(self):
        return self._num_examples

    @property
    def epochs_completed(self):
        return self._epochs_completed

    def next_batch(self, batch_size, fake_data=False):
        """Return the next `batch_size` examples from this data set."""
        start = self._index_in_epoch
        self._index_in_epoch += batch_size
        if self._index_in_epoch > self._num_examples:
            # Finished epoch
            self._epochs_completed += 1
            # Shuffle the data
            perm = numpy.arange(self._num_examples)
            numpy.random.shuffle(perm)
            self._traces = self._traces[perm]
            self._labels = self._labels[perm]
            # Start next epoch
            start = 0
            self._index_in_epoch = batch_size
            assert batch_size <= self._num_examples
        end = self._index_in_epoch

        return self._traces[start:end], self._labels[start:end]

def load_keil_traces(train_dir, date, number, format='csv'):

    filename = df_powertrace = pd.read_csv(os.path.join(train_dir, 
        filename(date, number, format, channel='segmented_traces')))
    traces = pd.read_csv(os.path.join(train_dir, '.'.join([name, format])))
    labels = pd.read_csv(os.path.join(train_dir, '.'.join([name, format])))

    return traces. labels

def load_kamal_powertraces(train_dir, format='pkl'):
    all_traces = pd.read_pickle(os.path.join(train_dir, '.'.join(['resampled_traces', format]))).as_matrix()
    all_traces = all_traces.reshape(all_traces.shape[0],
        all_traces.shape[1])
    train_labels_original = pd.read_pickle(os.path.join(train_dir, '.'.join(['resampled_labels', format])))['node_ids'].as_matrix()

    return all_traces, train_labels_original

def load_gdls_powertraces(df, format='bin'):

    #filename_signal = os.path.join(train_dir, number+'-'+module+'-'+'buffers.csv')
    #filename_labels = os.path.join(train_dir, number+'-'+'labels.csv')
    #print("Reading "+ filename_signal)
    
    #df = pd.read_csv(filename_signal)
    traces = df.as_matrix()
    print(df.head())
    #traces = l2_norm(traces)
    print(traces.shape)
    all_labels = [str(module)]*df.shape[0]
    print(len(all_labels))
    #all_traces = df.as_matrix()
    return traces, all_labels

def load_gdls_powertraces_db(train_dir, filename, format='bin'):

    filename_signal = os.path.join(train_dir, filename)
    #filename_labels = os.path.join(train_dir, number+'-'+'labels.csv')
    print("Reading "+ filename_signal)
    
    df = pd.read_csv(filename_signal)
    traces = df.iloc[:,0:1000].as_matrix()
    traces = l2_norm(traces)
    
    all_labels = df.iloc[:, -1]
   
    #all_traces = df.as_matrix()
    return traces, all_labels

def read_data_sets(train_dir, fake_data=False, one_hot=False,
                        dtype=dtypes.float64, reshape=True,
                        validation_size=5000, format='pkl', data='kamal'):
    """Set the images and labels."""
    num_training = 400
    num_validation = 121
    num_test = 100

    if data == "kamal":
        all_traces, all_labels = load_kamal_powertraces(train_dir)
    elif data == "gdls":
        all_traces= load_gdls_powertraces(train_dir)
    else:
        print("Enter a valid data source to read traces from")
        sys.exit(0)
    
    # all_labels = numpy.asarray(range(0, len(train_labels_original)))
    # all_labels = dense_to_one_hot(all_labels, len(np.unique(train_labels_original)))
    # all_labels = train_labels_original

    train_traces, test_traces, train_labels, test_labels = train_test_split(all_traces, all_labels, test_size=0.2, random_state=1)

    train_traces, validation_traces, train_labels, validation_labels = train_test_split(train_traces, train_labels, test_size=0.2, random_state=1)

    # mask = range(num_training)
    # train_traces = all_traces[mask]
    # train_labels = all_labels[mask]

    # mask = range(num_training, num_training + num_validation)
    # validation_traces = all_traces[mask]
    # validation_labels = all_labels[mask]

    # mask = range(num_training + num_validation, num_training + num_validation + num_test)
    # test_traces = all_traces[mask]
    # test_labels = all_labels[mask]

    train = DataSet(train_traces, train_labels, dtype=dtype, reshape=reshape)
    validation = DataSet(validation_traces, validation_labels, dtype=dtype,
        reshape=reshape)

    test = DataSet(test_traces, test_labels, dtype=dtype, reshape=reshape)
    ds = collections.namedtuple('Datasets', ['train', 'validation', 'test'])
    return ds(train=train, validation=validation, test=test)


def read_data_sets_keras(df, fake_data=False, one_hot=False,
                        dtype=dtypes.float64, reshape=True,
                        validation_size=5000, format='pkl', data='kamal'):
    """Set the images and labels."""
    num_training = 400
    num_validation = 121
    num_test = 100

    if data == "kamal":
        all_traces, all_labels = load_kamal_powertraces(df)
    elif data == "gdls":
        all_traces, all_labels = load_gdls_powertraces(df)
    else:
        print("Enter a valid data source to read traces from")
        sys.exit(0)
    
    # all_labels = numpy.asarray(range(0, len(train_labels_original)))
    #all_labels = dense_to_one_hot(all_labels, len(np.unique(all_labels)))
    # all_labels = train_labels_original

    train_traces, test_traces, train_labels, test_labels = train_test_split(all_traces, all_labels, test_size=0.2, random_state=1)

    return train_traces, train_labels, test_traces, test_labels


def read_data_sets_keras_gdls(train_dir, filename, fake_data=False, one_hot=False,
                        dtype=dtypes.float64, reshape=True,
                        validation_size=5000, format='pkl', data='kamal'):
    """Set the images and labels."""
    num_training = 400
    num_validation = 121
    num_test = 100

    if data == "kamal":
        all_traces, all_labels = load_kamal_powertraces(train_dir)
    elif data == "gdls":
        all_traces, all_labels = load_gdls_powertraces_db(train_dir, filename)
    else:
        print("Enter a valid data source to read traces from")
        sys.exit(0)
    
    # all_labels = numpy.asarray(range(0, len(train_labels_original)))
    #all_labels = dense_to_one_hot(all_labels, len(np.unique(all_labels)))
    # all_labels = train_labels_original
    lb = preprocessing.LabelBinarizer()
    lb.fit(all_labels.values)
    all_labels = lb.transform(all_labels.values)
    
    train_traces, test_traces, train_labels, test_labels = train_test_split(all_traces, all_labels, test_size=0.2, random_state=1)

    #train_traces, validation_traces, train_labels, validation_labels = train_test_split(train_traces, train_labels, test_size=0.2, random_state=1)

    # mask = range(num_training)
    # train_traces = all_traces[mask]
    # train_labels = all_labels[mask]

    # mask = range(num_training, num_training + num_validation)
    # validation_traces = all_traces[mask]
    # validation_labels = all_labels[mask]

    # mask = range(num_training + num_validation, num_training + num_validation + num_test)
    # test_traces = all_traces[mask]
    # test_labels = all_labels[mask]

    
    return train_traces, train_labels, test_traces, test_labels


def dense_to_one_hot(labels_dense, num_classes):
    print(num_classes)
    """Convert class labels from scalars to one-hot vectors."""
    num_labels = labels_dense.shape[0]
    index_offset = numpy.arange(num_labels) * num_classes
    labels_one_hot = numpy.zeros((num_labels, num_classes))
    labels_one_hot.flat[index_offset + labels_dense.ravel()] = 1
    print(labels_one_hot.shape)
    return labels_one_hot
