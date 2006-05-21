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

    for (int i = 0; i < 29999; i++)
    {
        char value[128];
        sprintf(value, "value for %d", i);
        assert(st.put(&i, sizeof i, value, strlen(value)+1));
        printf("PUT key(%d), bvalue(%s)\n", i, value);
    }

    //st.close();
    //st.open(argv[1]);
    //Stasher st2;
    //st.open(argv[1]);
    printf("length -> %lld\n", st.length());

    for (int i = 0; i < 29999; i++)
    {
        buffer bvalue;
        assert(st.get(&i, sizeof i, bvalue));
   
        printf("GET key(%d), bvalue(%s)\n", i, &bvalue[0]);
    }

    return 0;
}
