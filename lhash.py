import sys

def pow2(e):
    return 1 << e

class BucketItem:
    def __init__(self, k='', v=''):
        self.key = k
        self.hash = hash(k)
        self.value = v

    def __repr__(self):
        return 'k(%s) => v(%s)' % (self.key, self.value)

class Table:
    def __init__(self):
        #self.table = [] 
        self.table = [[]] 
        self.split = 0
        self.level = 0

        self.count = 0
        self.unique_hashes = 0

    def __getitem__(self, v):
        return self.get(v)

    def __setitem__(self, k, v):
        self.insert(k, v)

    def __delitem__(self, i):
        self.remove(i)

    def __len__(self):
        return self.count

    def address(self, key):
        """ find the bucket address for key """
        h = hash(key)
        partition = 2**self.level

        #choose h[i], or h[i+1] based on split position 
 
        if h % partition >= self.split:
            return h % partition

        return h % (2 * partition)

    def split_bucket(self):
        """ split (split) bucket referenced by split """
    
        old_level = self.level
        old_split = self.split
        self.split += 1

        if self.split == 2**self.level:
            self.level += 1
            self.split = 0
            print 'increasing level ->', self.level

        bucket = self.table[old_split]        

        #append a new bucket for splits
        move = []
        self.table.append(move) 

        for bi in bucket: 
            address = self.address(bi.key)         
            if address != old_split:
                move.append(bi)               
                print 'moving %d -> %d, partition + split -> %d' % \
                    (old_split, address, (1<<old_level) + old_split)            
 
        for rm in move:
            bucket.remove(rm)

    def join_bucket(self):
        """ remove last bucket, group """

        print 'Dehash level:', self.level
        print 'Dehash split:', self.split
        if self.split == 0:
            self.level -= 1
            print 'decreasing level ->', self.level
            self.split = (2**self.level) - 1
            print 'resetting split ->', self.split
        else:
            self.split -= 1

        for bi in self.table[-1]:
            print 'table -> address(%d), split(%d)' % \
                (self.address(bi.key), self.split)
            assert self.address(bi.key) == self.split, 'grouping error'

        bucket = self.table[self.split]        
        bucket += self.table[-1]
        del self.table[-1] 

    def insert(self, key, value):
        """ insert into hash table """

        bucket = self.table[self.address(key)]
        new = BucketItem(key, value)
        dup = False
        for bi in bucket:
            if bi.hash == new.hash:
                dup = True
        bucket.append(new)

        self.count += 1
        if not dup: 
            self.unique_hashes += 1        
            self.split_bucket()


    def remove(self, key):
        results = []

        try:
            bucket = self.table[self.address(key)]
            dup = False
            h = hash(key)
            for bi in bucket:
                if bi.hash == h:
                    if bi.key == key:
                        results.append(bi)
                    else:
                        dup = True
            
            print 'Dupped?', dup
 
            for bi in results:
                bucket.remove(bi)
                self.count -= 1
 
        except IndexError:
            pass

        if not results:
            raise KeyError, 'invalid key'

        if not dup:
            self.unique_hashes -= 1
            self.join_bucket()

        return [bi.value for bi in results] 

    def get(self, key):
        try:
            bucket = self.table[self.address(key)]
        except IndexError:
            raise KeyError, 'invalid key'
 
        for bi in bucket:
            if bi.key == key:
                return bi.value

        raise KeyError, 'invalid key'

    def get_all(self, key):
        results = []

        try:
            bucket = self.table[self.address(key)]
            for bi in bucket:
                if bi.key == key:
                    results.append(bi.value)

        except IndexError:
            pass

        return results

    def read_file(self, file):
        fp = open(file)
        test = []
    
        for ln in fp:

            try:
                k, v = ln.split('\t', 2)
                test.append((k.strip(), v.strip()))
            except:
                continue


        print 'adding...'
        for k,v in test:
            self.insert(k, v) 
        """
        print 'del test...'
        for i in xrange(20):
            k, v = test.pop()
            try:
                self.remove(k)
                print 'remove %s -> %s' % (k,v)
            except KeyError:
                pass

        print 'checking...'
        for k,v in test:
            all = self.get_all(k)

            if not v in all: 
                print 'HUH? cant find value for key %s -> %s' % (k,v)
        """

def test_load():
    t = Table()
    t.read_file('acronym-dictionary.txt')
    
    return t

"""def test_load():
    if len(sys.argv) < 2:
        print 'lhash test_file'
        return 0

    t = Table()
    t.read_file(sys.argv[1])

    print 'table length ->', len(t.table)
    print 'record count ->', len(t)
"""

def test_delete():
    t = Table()
    items = [
        'string1', 'string2', 'string3', 
        'string4', 'string5', 'string6',
        'string7', 'string8', 'string9'
    ]

    for i in items:
        t[i] = i

    for i in ['string3', 'string4', 'string6', 'string9', 'string8']:
        del t[i]
        items.remove(i)

    for i in items:
        print t[i]

    #add supplimental
    new = ['new1', 'new2', 'new3', 'new4', 'new5', 'new6', 'new7', 'string8']
    for i in new:
        t[i] = i

    for i in new:
        print t[i]

    for i in items:
        print t[i]

    for i in ['new2', 'new4', 'new5', 'new7']:
        del t[i]
        new.remove(i)

    for i in new:
        print t[i]

    for i in items:
        print t[i]

    print t.table

    all = new + items
    for i in all:
        del t[i]

    print t.table

if __name__ == '__main__':
    test_load() 
