CC=g++-7
CFLAGS=-I. -O0 -ggdb

_OBJ=benchmark.o ColumnVector.o Allocator.o mremap.o 

OBJ=$(patsubst %,$(ODIR)/%,$(_OBJ))
ODIR=obj

$(ODIR)/%.o: %.cpp
	$(CC) -c -o $@ $< $(CFLAGS)

benchmark: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

all: benchmark

.PHONY: clean

clean:
	rm -f $(ODIR)/*.o benchmark
