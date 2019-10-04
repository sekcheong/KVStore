To build after changing client python code
python setup.py build_ext --inplace

This will generate the header file client.h and a shared library client.so
The shared library needs to be renamed to libclient.so
mv client.so libclient.so

Compile the shared library along with test code
gcc -I/usr/include/python2.7 -o test test.c -lpython2.7 -lclient

Hack to detect libclient.so - To be solved
sudo cp libclient.so /usr/lib/

To run the test
./test

gcc -I/usr/include/python2.7 -o correctness_test_single_client correctness_test_single_client.c -lpython2.7 -lclient -lpthread -DKVSTORE_SIZE=1000 -DSLEEP_TIME_US=0
