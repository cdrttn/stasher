#g++ -Wall -fno-inline -ggdb -DHAVE_STDINT -o bbuf test_bucketbuf.cpp pager.cpp fileio.cpp headerbuf.cpp freecache.cpp bucketbuf.cpp

CPP = g++
CFLAGS = -Wall -fno-inline -ggdb -DHAVE_STDINT -DCHECK_PAGES -D_LARGEFILE64_SOURCE=1 -DLARGEFILE_SOURCE=1 -D_FILE_OFFSET_BITS=64
LDFLAGS = -lm 

OBJ = pager.o lrucache.o freecache.o fileio.o headerbuf.o bucketbuf.o bucket.o overflow.o bucketarray.o hashfunc.o stasher.o
lruOBJ = lrucache.o fileio.o
bbOBJ = pager.o freecache.o fileio.o headerbuf.o bucketbuf.o 
bOBJ = $(bbOBJ) bucket.o overflow.o
oOBJ = $(bbOBJ) overflow.o
pOBJ = pager.o lrucache.o freecache.o fileio.o headerbuf.o

lru: test_lru.o $(lruOBJ)
	$(CPP) -o lru test_lru.o $(lruOBJ) $(LDFLAGS)

oflo: test_overflow.o $(oOBJ)
	$(CPP) -o oflo test_overflow.o $(oOBJ) $(LDFLAGS)

mem: test_mem.o $(OBJ)
	$(CPP) -o mem test_mem.o $(OBJ) $(LDFLAGS)

stasher: test_stasher.o $(OBJ)
	$(CPP) -o stasher test_stasher.o $(OBJ) $(LDFLAGS)

bucketarray: test_bucketarray.o $(OBJ)
	$(CPP) -o bucketarray test_bucketarray.o $(OBJ) $(LDFLAGS)

bucket: test_bucket.o $(bOBJ)
	$(CPP) -o bucket test_bucket.o $(bOBJ) $(LDFLAGS)

bbuf: test_bucketbuf.o $(bbOBJ)
	$(CPP) -o bbuf test_bucketbuf.o $(bbOBJ) $(LDFLAGS)

pager: test_pager.o $(pOBJ)
	$(CPP) -o pager test_pager.o $(pOBJ) $(LDFLAGS)

writebuf: test_writebuf.o $(OBJ)
	$(CPP) -o writebuf test_writebuf.o $(OBJ) $(LDFLAGS)

hashtest: 
	$(CPP) -o hashtest hashfunc.cpp -DHASH_TEST $(CFLAGS)

%.o: %.cpp
	$(CPP) -c $< $(CFLAGS)

clean:
	rm -f *.o
	rm -f pager bbuf bucket bucketarray
