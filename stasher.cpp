#include "stasher.h"
#include "pager.h"
#include "bucketbuf.h"
#include "bucket.h"
#include "bucketarray.h"
#include "hashheaderbuf.h"
#include "hashfunc.h"
#include <string.h>
#include <math.h>

#define HASHFUNC hash_func1

using namespace std;
using namespace ST; 

void StasherOpts::set_bucket_max_chunk(uint16_t max)
{
    double dpower = log((double)max) / log(2.0);
    uint16_t ipower = (uint16_t)dpower; 

    dpower -= ipower;

    if (dpower >= 0.5)
        ipower++;

    if (ipower > 0)
        m_maxexp = ipower; 
    else
        ipower = MAXEXP_DEFAULT;
}

void Stasher::init()
{
    m_pager = NULL;
    m_hhb = NULL;
    m_array = NULL;
    m_opened = false;
    m_hashfunc = NULL;
    m_filename = "";
}

Stasher::Stasher()
{
    init();
}

Stasher::~Stasher()
{
    close();
}

void Stasher::open(const string &filename, int flags, const StasherOpts &opts) 
{
    int pflags = (flags & OPEN_WRITE)? Pager::OPEN_WRITE : Pager::OPEN_READ;

    if (m_opened)
        throw IOException("cannot call open twice", m_filename);

    m_opened = true;

    m_pager = new Pager;
    m_pager->open(filename, pflags, opts.m_mode, opts.m_pagesize);

    m_hhb = new HashHeaderBuf(*m_pager);
    if (m_pager->length() > 1)
        m_pager->read_page(*m_hhb, 1);
    else
    {
        //FIXME: need to check to make sure maxrec, etc are valid
        m_hhb->allocate();
        m_hhb->clear();
        m_hhb->create();
        m_hhb->set_maxrec(opts.m_maxrec);
        m_hhb->set_maxexp(opts.m_maxexp);
        m_hhb->set_dupes(opts.m_dupe);

        if (opts.m_capacity > 0)
            m_hhb->set_capacity(opts.m_capacity);
        else
            m_hhb->set_capacity(opts.m_maxrec);
            
        m_pager->write_page(*m_hhb, 1);
    }

    if (!m_hhb->validate())
        throw IOException("hash header error", m_filename);
   
    //use the default hash function if user does not supply one
    if (opts.m_hashfunc)
        m_hashfunc = opts.m_hashfunc;
    else
        m_hashfunc = HASHFUNC;
    
    m_array = new BucketArray(*m_hhb);
    if (m_array->length() == 0)
    {
        Bucket bucket;
        m_array->append(bucket);
        m_pager->write_page(*bucket.get_head());
    }

    m_filename = filename;
}

void Stasher::close()
{
    if (m_hhb && m_pager && m_pager->writeable())
        m_pager->write_page(*m_hhb);

    //Mind order here!
    delete m_array;
    delete m_hhb;
    delete m_pager;
    init();
}

uint64_t Stasher::length()
{
    return m_hhb->get_itemcount();
}

inline uint32_t Stasher::address(uint32_t hash32)
{
    uint32_t partition = pow2(m_hhb->get_hashlevel());

    if (hash32 % partition >= m_hhb->get_splitpos())
        return hash32 % partition;

    return hash32 % (2 * partition);
}

void Stasher::split_bucket()
{
    uint32_t old_level, old_split;

    old_level = m_hhb->get_hashlevel();
    old_split = m_hhb->get_splitpos();

    if (old_split + 1 == pow2(old_level))
    {
        m_hhb->set_hashlevel(old_level + 1);
        m_hhb->set_splitpos(0);
    }
    else
        m_hhb->set_splitpos(old_split + 1);

    //rehash items 
    Bucket from, to;
    BucketIter ifrom, ito;

    m_array->get(from, old_split);
    m_array->append(to); 

    ifrom = from.iter();
    ito = to.iter();

    while (ifrom.next())
    {
        uint32_t newpos = address(ifrom.get_hash32());
        if (newpos != old_split)
        {
            to.copy_quick(ito, ifrom);
            from.remove(ifrom, Bucket::NOCLEAN | Bucket::KEEPOVERFLOW);
        }
    }

    //XXX: compact from ?
    m_pager->write_page(*m_hhb);
}

void Stasher::join_bucket()
{
    uint32_t old_level, old_split;

    old_level = m_hhb->get_hashlevel();
    old_split = m_hhb->get_splitpos();

    if (old_split == 0)
    {
        assert(old_level > 0);
        old_level--;
        m_hhb->set_hashlevel(old_level);
        m_hhb->set_splitpos(pow2(old_level) - 1);
    }
    else
        m_hhb->set_splitpos(old_split - 1);

    //move items of the last bucket to split pos
    Bucket from, to;
    BucketIter ifrom, ito;

    m_array->get(to, m_hhb->get_splitpos());
    m_array->last(from); 

    ifrom = from.iter();
    ito = to.iter();

    while (ifrom.next())
    {
        to.copy_quick(ito, ifrom);
        from.remove(ifrom, Bucket::NOCLEAN | Bucket::KEEPOVERFLOW);
    }

    m_array->shrink();
}

bool Stasher::put(const void *key, uint32_t klen, const void *value, uint32_t vlen)
{
    uint32_t hash32;
    Bucket bucket;
    bool gotdup = false;

    if (!klen || !vlen)
        return false;
    
    hash32 = m_hashfunc(key, klen);
    m_array->get(bucket, address(hash32));

    BucketIter iter = bucket.iter();

    //XXX: hopefully, there are not too many overflow buckets, or this could be slow...
    while (iter.next() and !gotdup)
    {
        if (iter.get_hash32() == hash32)
            gotdup = true;
    }

    if (!gotdup || !m_hhb->get_dupes())
        bucket.append(hash32, key, klen, value, vlen);
    else
        return false;

    if (!gotdup)
    {
        m_hhb->set_unique_hashes(m_hhb->get_unique_hashes() + 1);
        if ((m_hhb->get_unique_hashes() % m_hhb->get_capacity()) == 0)
            split_bucket();
    }

    m_hhb->set_itemcount(m_hhb->get_itemcount() + 1);
    
    return true;
}

bool Stasher::get(const void *key, uint32_t klen, buffer &value)
{
    uint32_t hash32;
    Bucket bucket;

    if (!klen)
        return false;

    hash32 = m_hashfunc(key, klen);
    m_array->get(bucket, address(hash32));

    BucketIter iter = bucket.iter();
    buffer keytest;
    while (iter.next())
    {
        if (hash32 == iter.get_hash32())
        {
            iter.get_key(keytest);
            if (keytest.size() == klen && !memcmp(&keytest[0], key, klen))
            {
                iter.get_value(value);
                return true;
            }
            else
                keytest.clear();
        }
    }

    return false;
}

//XXX: this copies too much code
bool Stasher::remove(const void *key, uint32_t klen)
{
    uint32_t hash32;
    Bucket bucket;
    bool allhash = true;
    bool found = false;

    if (!klen)
        return false;

    hash32 = m_hashfunc(key, klen);
    m_array->get(bucket, address(hash32));

    BucketIter iter = bucket.iter();
    buffer keytest;
    while (iter.next())
    {
        if (hash32 == iter.get_hash32())
        {
            iter.get_key(keytest);
            if (keytest.size() == klen && !memcmp(&keytest[0], key, klen))
            {
                bucket.remove(iter, Bucket::NOCLEAN);
                found = true;
            }
            else
            {
                keytest.clear();
                allhash = false;
            } 
        }
    }

    //FIXME: compact bucket
    
    if (found)
        m_hhb->set_itemcount(m_hhb->get_itemcount() - 1);

    if (allhash)
    {
        m_hhb->set_unique_hashes(m_hhb->get_unique_hashes() - 1);

        if ((m_hhb->get_unique_hashes() % m_hhb->get_capacity()) == 0)
            join_bucket();
    }

    return false;
}

