import pycuda.driver as cuda
import pycuda.autoinit
from pycuda import curandom
from pycuda import gpuarray
from pycuda.elementwise import ElementwiseKernel
from pycuda.reduction import ReductionKernel
import time
import numpy as np

n_runs = 10**8

############################################################
# NumPy Solution
############################################################

t_np0 = time.time()

# Simulate Random Coordinates in Unit Square:
ran = np.random.uniform(low=-1, high=1, size=(2, n_runs))

# Identify Random Coordinates that fall within Unit Circle and count them
result = ran[0]**2 + ran[1]**2 <= 1
n_in_circle = np.sum(result)
pi_np = 4 * n_in_circle / n_runs
t_np1 = time.time()
print("Time Elapsed (CPU): ", t_np1 - t_np0)

############################################################
# Random Number Generation (GPU)
############################################################

t0 = time.time()
# generates ran numbers from [0, 1) (1/4 of unit circle)
rng = curandom.XORWOWRandomNumberGenerator()
ran_gpu = rng.gen_uniform((2, n_runs), dtype=np.float32)

t_mem = time.time() - t0
print("Time Elapsed (Random Number Generation on GPU): ", t_mem)

############################################################
# Pythonic Map Operation
############################################################

t1 = time.time()
map_result = (ran_gpu[0]**2 + ran_gpu[1]**2) <= 1
n_total = map_result.get()
n_in = np.sum(n_total)
pi_map = 4 * n_in / n_runs
t_map = time.time()
print("Time Elapsed (Pythonic Map): ", t_map - t1)

############################################################
# Pythonic Map + Reduce Operation
############################################################

t1 = time.time()
map_result = (ran_gpu[0]**2 + ran_gpu[1]**2) <= 1
n_in_dev = gpuarray.sum(map_result)
n_in_host = n_in_dev.get()
pi_map = 4 * n_in_host / n_runs
t_map = time.time()
print("Time Elapsed (Pythonic Map + Reduce): ", t_map - t1)

############################################################
# Elementwise Map, Followed by Reduction Kernel
############################################################

t2 = time.time()
mknl = ElementwiseKernel(
        "float *x, float *y, int *z",
        "z[i] = (x[i]*x[i] + y[i]*y[i]) <= 1 ? 1 : 0"
        )
rknl = ReductionKernel(
            dtype_out=np.float32,
            neutral="0",
            reduce_expr="a+b",
            map_expr="z[i]",
            arguments="int *z"
        )

z_gpu = gpuarray.empty_like(ran_gpu[0], dtype=np.int32)
mknl(ran_gpu[0], ran_gpu[1], z_gpu)
n_in = rknl(z_gpu).get()
pi_emap = 4 * n_in / n_runs
t_emap = time.time()
print("Time Elapsed (Elementwise Map + Reduction Kernel): ", t_emap - t2)

############################################################
# Combined Map/Reduce Kernel
############################################################

t3 = time.time()
rknl = ReductionKernel(
        dtype_out=np.float32,
        neutral="0",
        reduce_expr="a+b",
        map_expr="(x[i]*x[i] + y[i]*y[i]) <= 1 ? 1 : 0",
        arguments="float *x, float *y"
        )
n_in = rknl(ran_gpu[0], ran_gpu[1]).get()
pi_map_reduce = 4 * n_in / n_runs
t_map_reduce = time.time()
print("Time Elapsed (Combined Map + Reduce): ", t_map_reduce - t3)
