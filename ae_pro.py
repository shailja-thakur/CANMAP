''' This script demonstrates how to build a variational autoencoder with Keras.
 #Reference
 - Auto-Encoding Variational Bayes
   https://arxiv.org/abs/1312.6114
'''
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm
import h5py
from keras.layers import Input, Dense, Lambda, Layer
from keras.models import Model
from keras import backend as K
from keras import metrics, regularizers
from keras.models import model_from_json
import redis
import json
import pickle
from keras.models import load_model
from sklearn.metrics import mean_squared_error
from keras.datasets import mnist
import pandas as pd
import sys

number = sys.argv[1]
module = sys.argv[2]
batch_size = 64
#original_dim = 98
latent_dim = 10
intermediate_dim = 100
epochs = 500
epsilon_std = 1.0
PROCESSOR_NAME = 'AE'
PROCESSOR_KEY = 'module_'+PROCESSOR_NAME

def load_traces():
    import dataset
    #return dataset.read_data_sets_keras('/media/gdls/gdls-data/workspace', number, module, one_hot=True, data='gdls')
    return dataset.read_data_sets_keras( df, one_hot=True, data='gdls')

x_train, y_train, x_validation, y_validation, x_test, y_test = load_traces()
print("Trainig data mean", np.average(x_train, axis=0))
original_dim = x_train.shape[1]

def train(x_train, model_name='model'):

    original_dim = x_train.shape[1]
    x = Input(shape=(original_dim,))
    encoded = Dense(20, activation='relu')(x)
    encoded = Dense(10, activation='relu',activity_regularizer=regularizers.l1(10e-5))(encoded)
    encoded = Dense(5, activation='relu')(encoded)

    decoded = Dense(10, activation='relu')(encoded)
    decoded = Dense(20, activation='relu')(decoded)
    decoded =Dense(original_dim, activation='relu')(decoded)

    ae = Model(x, decoded)
    ae.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

    ae.fit(x_train, x_train, 
            shuffle=True,
            epochs=epochs,
            batch_size=batch_size,
            validation_data=(x_validation, x_validation))


    # # # # serialize model to JSON
    model_json = ae.to_json()
    with open("./" + model_name+".json", "w") as json_file:
        json_file.write(model_json)
    ae.save("./" + model_name + "ae.h5")
    print("Saved model to disk")

    return ae 
# Test
# load json and create model

def test():
    json_file = open("/home/s7thakur/av-daq/processors/autoencoder/ae.json", 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    model = model_from_json(loaded_model_json)
    # load weights into new model
    model.load_weights('/home/s7thakur/av-daq/processors/autoencoder/ae.h5')
    print("Loaded model from disk")

    # Test
    model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
    x_hat = model.predict(x=x_test)
    print('TRUE, ', x_hat)
    print('PREDICTED, ',x_test)
    errors = mean_squared_error(x_test, x_hat)
    print(errors)
    errorss = np.average((x_test - x_hat) ** 2, axis=0)
    print("Testing on the input power trace:")
    for err in errorss:
    	if err> 0.001:
    		print("Anomalous Trace")
    	else:
    		print("Non-Anomalous Trace", err)

    print("Everage error: ", np.average(errors))



if __name__ == "__main__":

    train()

    test()