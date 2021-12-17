

import matplotlib.pyplot as plt
x =[1,2,3,4,5,6,7,8,9,10] 
y =[96.25, 114.69, 94.76, 80.51, 112.88, 81.97, 86.97, 90.41, 94.04, 93.89]

# plot input vs output
# define a sequence of inputs between the smallest and largest known inputs
#x_line = arange(min(x), max(x), 1)

# create a line plot for the mapping function
plt.plot(x, y, color='blue', marker='o', label='Performance')

#plt.scatter(x,y)
x1 = [x[0], x[-1]]
y1 = [y[0], y[-1]]

plt.plot(x1,y1, color='red', label='Trend')
# add title and labels
plt.title('RunTime vs NumMax Executors')
plt.xlabel('NumMax Executors')
plt.ylabel('RunTime(s)')
plt.legend()
plt.savefig('plot.png')

plt.show()