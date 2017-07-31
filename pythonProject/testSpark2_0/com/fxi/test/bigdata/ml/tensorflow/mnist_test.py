#coding=utf-8
from tensorflow.examples.tutorials.mnist import input_data
mnist = input_data.read_data_sets('MNIST_data', one_hot=True)
print mnist
import tensorflow as tf
sess = tf.InteractiveSession()
#输入图像和目标输出类别创建节点，来开始构建计算图
x = tf.placeholder("float", shape=[None, 784])
y_ = tf.placeholder("float", shape=[None, 10])

#我们在调用tf.Variable的时候传入初始值。在这个例子里，我们把W和b都初始化为零向量。W是一个784x10的矩阵（因为我们有784个特征和10个输出值）。b是一个10维的向量（因为我们有10个分类）。
W = tf.Variable(tf.zeros([784,10]))
b = tf.Variable(tf.zeros([10]))



sess.run(tf.initialize_all_variables())

#我们把向量化后的图片x和权重矩阵W相乘，加上偏置b，然后计算每个分类的softmax概率值。
y = tf.nn.softmax(tf.matmul(x,W) + b)

#损失函数是目标类别和预测类别之间的交叉熵。
cross_entropy = -tf.reduce_sum(y_*tf.log(y))

#往计算图上添加一个新操作，其中包括计算梯度，计算每个参数的步长变化，并且计算出新的参数值。
train_step = tf.train.GradientDescentOptimizer(0.01).minimize(cross_entropy)

#每一步迭代，我们都会加载50个训练样本，然后执行一次train_step，并通过feed_dict将x 和 y_张量占位符用训练训练数据替代。
for i in range(1000):
    batch = mnist.train.next_batch(50)
    train_step.run(feed_dict={x: batch[0], y_: batch[1]})

correct_prediction = tf.equal(tf.argmax(y,1), tf.argmax(y_,1))

accuracy = tf.reduce_mean(tf.cast(correct_prediction, "float"))

print accuracy.eval(feed_dict={x: mnist.test.images, y_: mnist.test.labels})

