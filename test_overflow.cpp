#include "overflow.h"
#include <assert.h>

using namespace ST;

int main(int argc, char **argv)
{
    
    if (argc < 3)
    {
        puts("oflo db input");
        return 0;
    }
    
    Pager pgr;
    pgr.open(argv[1], Pager::OPEN_WRITE);

    FileIO fp;
    buffer buf;
    
    fp.open(argv[2], O_RDONLY);

    buf.resize(fp.size());
    fp.read(&buf[0], buf.size());
    //printf("%.*s", buf.size(), &buf[0]);
    Overflow oflo(pgr);
    //oflo.init(buf);
    
    char *shit = "dinkie row";
    char *crap = "dinkie doe";
    oflo.add_item(buf);
    oflo.add_item(shit, strlen(crap));
    oflo.add_item(shit, strlen(crap));
    oflo.add_item(shit, strlen(crap));
    oflo.add_item(shit, strlen(crap));
    oflo.add_item(shit, strlen(crap));
    oflo.add_item(buf);
    oflo.add_item(shit, strlen(crap));
    oflo.add_item(buf);

    int i = 0;
    bool doo = false;
    oflo.rewind();
    while (oflo.get_next_item(buf))
    {
        printf("ITEM %d: %.*s\n", i++, buf.size(), &buf[0]);
        doo = !doo;
        //if (doo)
            //oflo.remove_item();
    }
  
    oflo.add_item(buf);
    oflo.add_item(shit, strlen(crap));
    i = 0;
    oflo.rewind();
    while (oflo.get_next_item(buf))
        printf("AFTER %d: %.*s\n", i++, buf.size(), &buf[0]);
    oflo.clear();
    oflo.add_item(buf);
    oflo.add_item(shit, strlen(crap));

    pgr.close();
    return 0;
}
