'''Trains a simple deep NN on the MNIST dataset.
Gets to 98.40% test accuracy after 20 epochs
(there is *a lot* of margin for parameter tuning).
2 seconds per epoch on a K520 GPU.
'''

from __future__ import print_function

import keras
from keras.datasets import mnist
from keras.models import Sequential
from keras.layers import Dense, Dropout
from keras.optimizers import RMSprop
from sklearn import preprocessing
import numpy as np
import pandas as pd
from keras.models import model_from_json
from sklearn.metrics import confusion_matrix
import sys
import pickle
f = sys.argv[1]
batch_size = 50
#num_classes = 52
epochs = 100

def load_traces():

    import dataset
    return dataset.read_data_sets_keras_gdls('/media/gdls/gdls-data/workspace/',f, one_hot=True, data='gdls')

# the data, split between train and test sets
x_train, y_train, x_test, y_test = load_traces()
print(x_train.shape[0], 'train samples')
print(x_test.shape[0], 'test samples')
# convert class vectors to binary class matrices
print(y_train.shape[1], 'num of classes')
print(x_train.shape[1], 'input shape')
num_classes = y_train.shape[1]
classes = np.unique(y_train)
print("Classes: ", y_train[0:20])
#y_train = keras.utils.to_categorical(y_train, num_classes)
#y_test = keras.utils.to_categorical(y_test, num_classes)
print('Layer 1/2/3 -> ',x_train.shape[1]/2, x_train.shape[1]/4, x_train.shape[1]/8)

model = Sequential()
model.add(Dense(int(x_train.shape[1]/2), activation='relu', input_shape=(x_train.shape[1],)))
model.add(Dropout(0.2))
model.add(Dense(int(x_train.shape[1]/4), activation='relu'))
model.add(Dropout(0.2))
model.add(Dense(int(x_train.shape[1]/8), activation='relu'))
#model.add(Dropout(0.2))
model.add(Dense(num_classes, activation='softmax'))
model.summary()

model.compile(loss='categorical_crossentropy',
                optimizer='adam',
                metrics=['accuracy'])

history = model.fit(x_train, y_train,
                      batch_size=batch_size,
                      epochs=epochs,
                      verbose=1,
                      validation_data=(x_test, y_test))
# score = model.evaluate(x_test, y_test, verbose=0)


# # # serialize model to JSON
#model_json = model.to_json()
#with open("model.json", "w") as json_file:
#      json_file.write(model_json)

#pkl = open("history_1511379789_visit2.pickle", "wb") 
#pickle.dump(history.history, pkl)

# # # serialize weights to HDF5
#model.save_weights("model.h5")
#print("Saved model to disk")
 
 
# later...
 
# load json and create model
#json_file = open('model.json', 'r')
#loaded_model_json = json_file.read()
#json_file.close()
#loaded_model = model_from_json(loaded_model_json)
# load weights into new model
#loaded_model.load_weights("model.h5")
#print("Loaded model from disk")
 
# evaluate loaded model on test data
#model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
#classes = pd.read_csv('/media/gdls/gdls-data/workspace/classes.csv')
#classes = classes['label']
# classes = [0.0 ,61441.0, 61442.0, 61443.0, 61444.0, 65215.0,65247.0 ]

# msg = pd.read_csv("/media/gdls/gdls-data/workspace/SPNs_and_PGNs.csv", low_memory=False)
# print(classes[0], classes[1])
# predictions = []
# messages = []
# for test in x_test:
# 	#print(test[np.newaxis, :].shape)
# 	y_pred = loaded_model.predict(test[np.newaxis, :])
# 	iid = classes[np.argmax(y_pred)]
# 	idx = msg.index[int(iid) == msg["PGN"]][0]
# 	message = msg.iloc[idx]["SPN Name"]
# 	messages.append([iid, message])

# df_pred = pd.DataFrame(messages, columns =["id", "message"])
# df_pred.to_csv("1511448286_nn_predictions.csv")

#for test in x_test:
#    print(test)
#    y_hat = model.predict(test[np.newaxis, :])
#    print(np.argmax(y_hat, axis=1))
#    print(np.argmax(y_test, axis=1))
y_hat = model.predict(x_test)
pd.DataFrame(np.argmax(y_hat, axis=1)).to_csv('completed_predicted_400_length.csv', index=False)
pd.DataFrame(np.argmax(y_test, axis=1)).to_csv('complete_test_400_length.csv', index=False)
print(confusion_matrix(np.argmax(y_test, axis=1), np.argmax(y_hat, axis=1)))	
#print('Test loss:', score[0])
#print('Test accuracy:', score[1])


