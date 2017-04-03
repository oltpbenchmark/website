

import numpy as np
import tensorflow as tf
import time



def gp_workload(xs,ys,xt,ridge):

    batch_size = 3000
    sigma_cl = 1.07;
    xs = np.float32(xs)
    ys = np.float32(ys)
    xt = np.float32(xt)
    ridge = np.float32(ridge)

    y_best = tf.cast(tf.reduce_min(ys,0,True),tf.float32);   #array
    sample_size = xs.shape[0]
    nfeats = xs.shape[1]
    train_size = xt.shape[0]
    arr_offset = 0
    yhats = np.zeros([train_size,1]);
  #  sigmas = np.zeros([train_size,1]);


    v1 = tf.placeholder(tf.float32,name="v1")
    v2 = tf.placeholder(tf.float32,name="v2")
    dist = tf.sqrt(tf.reduce_sum(tf.pow(tf.subtract(v1, v2), 2),1))
    sess = tf.Session()
#    sess = tf.Session(config=tf.ConfigProto(log_device_placement=True))

    tmp = np.zeros([sample_size,sample_size])
    for i in range(sample_size):
        tmp[i] = sess.run(dist,feed_dict={v1:xs[i],v2:xs})
#    print "Finished euc matrix \n"


    tmp = tf.cast(tmp,tf.float32)
    K = tf.exp(-tmp/sigma_cl) + tf.diag(ridge);
 #   print "Finished K "


    K2 = tf.placeholder(tf.float32,name="K2")
    K3 = tf.placeholder(tf.float32,name="K3")

    x = tf.matmul(tf.matrix_inverse(K) , ys)
    yhat_ =  tf.cast(tf.matmul( tf.transpose(K2) ,x),tf.float32);
#    sig_val = tf.cast((tf.sqrt(tf.diag_part(K3 -  tf.matmul( tf.transpose(K2) ,tf.matmul(tf.matrix_inverse(K) , K2)) ))),tf.float32)




    while arr_offset < train_size:
        if arr_offset + batch_size > train_size:
            end_offset = train_size
        else:
            end_offset = arr_offset + batch_size;


        xt_ = xt[arr_offset:end_offset];
        batch_len = end_offset - arr_offset

        tmp = np.zeros([sample_size,batch_len])
        for i in range(sample_size):
            tmp[i] = sess.run(dist,feed_dict={v1:xs[i],v2:xt_})

        K2_ = tf.exp(-tmp/sigma_cl);
        K2_ = sess.run(K2_)

     #   tmp = np.zeros([batch_len,batch_len])
     #   for i in range(batch_len):
     #       tmp[i] = sess.run(dist,feed_dict={v1:xt_[i],v2:xt_})
     #   K3_ =tf.exp(-tmp/sigma_cl);
     #   K3_ = sess.run(K3_)


        yhat = sess.run(yhat_,feed_dict={K2:K2_})

  #      sigma = np.zeros([1,batch_len],np.float32)
   #     sigma[0] = (sess.run(sig_val,feed_dict={K2:K2_,K3:K3_}))
    #    sigma = np.transpose(sigma)

        yhats[arr_offset:end_offset] = yhat
      #  sigmas[arr_offset:end_offset] =  sigma;
        arr_offset = end_offset ;

    sess.close()

    return yhats
#,sigmas, eips

