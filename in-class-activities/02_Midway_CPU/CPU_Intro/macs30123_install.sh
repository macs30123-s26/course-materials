# Install packages we'll be using on Midway 3
module load python/anaconda-2022.05 mpich/3.2.1 cuda/11.7
pip install --user numba==0.57.1 numpy==1.22.4 mpi4py-mpich==3.1.5 \
                   pyopencl==2024.1 pycuda==2024.1 rasterio==1.3.9 \
                   scipy==1.13.0
