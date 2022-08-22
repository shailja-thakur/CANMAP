import pandas as pd 
import tensorflow as tf 
import numpy as np 
import Dataset
from tensorflow.python.framework import dtypes
import os
import collections

LOG_DIR = "logs/tensorboard/summaries/"
batch_limit = 100
batch_size = 300
filename = "1511448286-atvp3-power2.csv"
traces = Dataset.read_data_sets_gdls("/home/s7thakur/", filename)
key = filename.split('-')[1]
input_dim = 1
#def variable_summaries(var):

#with tf.name_scope('final'):
#    print("final summaries")
    #variable_summaries(x_in)
    
# Read data , write summaries
x_in = tf.placeholder(tf.float32, shape=[None, input_dim])

#def variable_summaries(var):
#with tf.name_scope('summaries'):
mean = tf.reduce_mean(x_in)
stddev = tf.sqrt(tf.reduce_mean(tf.square(x_in - mean)))
                

# merge all the summaries and write them out to logs

init = tf.global_variables_initializer()
tf.summary.scalar(key + 'mean', mean)
tf.summary.scalar(key + 'stddev', stddev)
tf.summary.scalar(key + 'max', tf.reduce_max(x_in))
tf.summary.scalar(key + 'min', tf.reduce_min(x_in))
tf.summary.histogram(key + 'histogram', x_in)
merged = tf.summary.merge_all()
sess = tf.Session()
sess.run(init)
logger = tf.summary.FileWriter(LOG_DIR, tf.get_default_graph())
i = 0
while True:
                batch_xs, batch_ys = traces.train.next_batch(batch_size)
              
                if i%5 == 0:
       	                summ = sess.run(merged, feed_dict={x_in:batch_xs})
       	                logger.add_summary(summ, global_step=i)
       	                print('Summary logged for {}th step'.format(i))
                i = i+1

