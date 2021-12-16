

import matplotlib.pyplot as plt
x =[1,2,3,4,5,6,7,8,9,10] 
y =[96.25, 114.69, 94.76, 80.51, 112.88, 81.97, 86.97, 90.41, 94.04, 93.89]

# plot input vs output
pyplot.scatter(x, y)
# define a sequence of inputs between the smallest and largest known inputs
x_line = arange(min(x), max(x), 1)

# create a line plot for the mapping function
pyplot.plot(x_line, y_line, '--', color='red')

# add title and labels
plt.title('RunTime vs NumMax Executors')
plt.xlabel('NumMax Executors')
plt.ylabel('RunTime')

plt.savefig('plot.png')