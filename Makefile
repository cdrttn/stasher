#g++ -Wall -fno-inline -ggdb -DHAVE_STDINT -o bbuf test_bucketbuf.cpp pager.cpp fileio.cpp headerbuf.cpp freecache.cpp bucketbuf.cpp

CPP = g++
CFLAGS = -Wall -fno-inline -ggdb -DHAVE_STDINT -DCHECK_PAGES -D_LARGEFILE64_SOURCE=1 -DLARGEFILE_SOURCE=1 -D_FILE_OFFSET_BITS=64
LDFLAGS = -lm 

OBJ = pager.o fileio.o headerbuf.o freecache.o bucketbuf.o bucket.o overflowbuf.o bucketarray.o

bucketarray: test_bucketarray.o $(OBJ)
	$(CPP) -o bucketarray test_bucketarray.o $(OBJ) $(LDFLAGS)

bucket: test_bucket.o $(OBJ)
	$(CPP) -o bucket test_bucket.o $(OBJ) $(LDFLAGS)

bbuf: test_bucketbuf.o $(OBJ)
	$(CPP) -o bbuf test_bucketbuf.o $(OBJ) $(LDFLAGS)

pager: test_pager.o $(OBJ)
	$(CPP) -o pager test_pager.o $(OBJ) $(LDFLAGS)

writebuf: test_writebuf.o $(OBJ)
	$(CPP) -o writebuf test_writebuf.o $(OBJ) $(LDFLAGS)

%.o: %.cpp
	$(CPP) -c $< $(CFLAGS)

clean:
	rm -f *.o
	rm -f pager bbuf bucket bucketarray
