from pyspark import SparkContext
from math import sqrt
sc = SparkContext(appName="Assignment") #terminal does this automatically
sc.setLogLevel("ERROR")

df = spark.read.csv("/data/doina/xyvalue.csv")

def euclidean_distance(row1, row2):
	distance = 0.0
	for i in range(len(row1)-1):
		distance += (row1[i] - row2[i])**2
	return sqrt(distance)

def get_neighbors(train, test_row, num_neighbors):
	distances = list()
	for train_row in train:
		dist = euclidean_distance(test_row, train_row)
		distances.append((train_row, dist))
	distances.sort(key=lambda tup: tup[1])
	neighbors = list()
	for i in range(num_neighbors):
		neighbors.append(distances[i][0])
	return neighbors
def predict(train, test_row, num_neighbors):
	neighbors = get_neighbors(train, test_row, num_neighbors)
	output_values = [row[-1] for row in neighbors]
	prediction =mean(output_values)
	return prediction

test_point =[100.0, 100.0]
print(predict(df, test_point, 100))