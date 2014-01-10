INC=-I. -I/usr/local/include -I/usr/local/hiredis -I/usr/local/apr/include/apr-1
LIB=-L/usr/local/apr/lib -L/usr/local/lib
all:test_simple.c redis_operating.c
	gcc $^ -g -O0 -o test $(INC) $(LIB) -lhiredis -lapr-1 -laprutil-1 -losip2 -losipparser2
clean:  
	rm -rf test
