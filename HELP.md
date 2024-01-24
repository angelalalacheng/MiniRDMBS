# How to run cmake

in root dir

``` bash
mkdir build

cmake ../

cd test

cd /go to the test file you want

make

```
# How to run Valgrind

``` bash
cd /go back to build dir

valgrind --leak-check=full ./pfmtest_public 
```