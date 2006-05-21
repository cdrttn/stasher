#include "stasher.h"
#include "hashfunc.h"
#include "fileio.h"
#include <stdio.h>
#include <assert.h>

using namespace std;
using namespace ST;

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        puts("stasher file");
        return 0;
    }

    if (FileIO::exists(argv[1]))
        FileIO::unlink(argv[1]);
    
    Stasher st;

    st.open(argv[1], Stasher::OPEN_WRITE);

    int i;

    for (i = 0; i < 299; i++)
    {
        char value[128];
        sprintf(value, "value for %d", i);
        assert(st.put(&i, sizeof i, value, strlen(value)+1));
        printf("PUT key(%d), bvalue(%s)\n", i, value);
    }

    for (i = 1; i < 299; i+=2)
    {
        st.remove(&i, sizeof i);
    }

    st.close();
    //st.open(argv[1]);
    //Stasher st2;
    st.open(argv[1]);
    printf("length -> %lld\n", st.length());

    for (i = 0; i < 299; i++)
    {
        buffer bvalue;
        if (st.get(&i, sizeof i, bvalue))
            printf("GET key(%d), bvalue(%s)\n", i, &bvalue[0]);
        else
            printf("GET key(%d) NOT FOUND\n", i);
    }

    return 0;
}
