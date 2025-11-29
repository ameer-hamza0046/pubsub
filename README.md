# install latest version of libzmq

sudo apt remove libzmq3-dev
sudo apt install build-essential cmake libtool pkg-config autoconf automake

git clone https://github.com/zeromq/libzmq.git
cd libzmq
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
sudo make install
sudo ldconfig


# then

git clone https://github.com/zeromq/cppzmq.git && \
cd cppzmq && \
mkdir build && cd build && \
cmake .. && \
sudo make install && \
sudo ldconfig
