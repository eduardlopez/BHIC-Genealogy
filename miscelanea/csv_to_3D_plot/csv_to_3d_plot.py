from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm
import matplotlib.pyplot as plt
import numpy as np


x, y, z = np.loadtxt('Q3.csv', delimiter=';', unpack=True, skiprows=1, dtype='int,float,int')


fig = plt.figure()
fig.set_size_inches(10, 10)
ax = fig.gca(projection='3d')
ax.plot_trisurf(x, y, z, cmap=cm.jet, linewidth=0.3)
ax.set_xlabel('YEAR')
ax.set_ylabel('DISTANCE')
ax.set_zlabel('FREQUENCY')

fig.savefig('Q3.png', dpi=1000)
#plt.show()
