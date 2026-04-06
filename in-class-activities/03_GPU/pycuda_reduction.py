import pycuda.driver as cuda
import pycuda.autoinit
import pycuda.curandom as curandom
from pycuda.reduction import ReductionKernel
import pycuda.gpuarray as gpuarray
import numpy as np
import time
rng = curandom.XORWOWRandomNumberGenerator()

# Set up the ReductionKernel for CUDA: maps first, then reduces
rknl = ReductionKernel(
    dtype_out=np.float32,
    neutral="0",  # Neutral element for the reduction (sum)
    reduce_expr="a + b",  # The reduction expression (sum of squares)
    map_expr="x[i] * x[i]",  # Mapping expression (square of elements)
    arguments="float *x")  # The input array

# Generate arrays of length `n`
n = 10 ** 7

# Perform the reduction on random input array (transferred from CPU)
t0 = time.time()
x = np.random.rand(n).astype(np.float32)
x_gpu0 = gpuarray.to_gpu(x)
result_gpu0 = rknl(x_gpu0)
result0 = result_gpu0.get()

# report result and time
print("result:", result0,
      "time:", time.time() - t0)

# Perform the reduction on random input array (generated on GPU)
t1 = time.time()
x_gpu1 = rng.gen_uniform((n,), dtype=np.float32)
result_gpu1 = rknl(x_gpu1)
result1 = result_gpu1.get()

# report result and time
print("result:", result1,
      "time:", time.time() - t1)