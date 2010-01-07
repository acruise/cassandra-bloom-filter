package com.facebook.infrastructure.utils;

import com.facebook.infrastructure.io.ICompactSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class CountingBloomFilter extends Filter
{
    public static final int MAX_COUNT = 15;
    static final int BUCKETS_PER_WORD = 16;

    private static ICompactSerializer<CountingBloomFilter> serializer_ = new CountingBloomFilterSerializer();

    public static ICompactSerializer<CountingBloomFilter> serializer()
    {
        return serializer_;
    }

    long[] filter_;

    public CountingBloomFilter(int numElements, int bucketsPerElement)
    {
        this(BloomCalculations.computeBestK(bucketsPerElement), new long[(numElements * bucketsPerElement + 20) / BUCKETS_PER_WORD]);
    }

    CountingBloomFilter(int hashes, long[] filter)
    {
        hashCount = hashes;
        filter_ = filter;
    }

    public CountingBloomFilter cloneMe()
    {
        long[] filter = new long[filter_.length];
        System.arraycopy(filter_, 0, filter, 0, filter_.length);
        return new CountingBloomFilter(hashCount, filter);
    }

    int maxBucket() {
        int max = 0;
        for (int i = 0; i < buckets(); i++) {
            Bucket bucket = new Bucket(i);
            if (bucket.value > max) {
                max = (int)bucket.value;
            }
        }
        return max;
    }

    public void clear() {
        Arrays.fill(filter_, (byte)0);
    }

    public void merge(CountingBloomFilter cbf)
    {
        assert cbf != null;
        assert filter_.length == cbf.filter_.length;
        assert hashCount == cbf.hashCount;
        
        for ( int i = 0; i < buckets(); ++i )
        {
            Bucket b = new Bucket(i);
            Bucket b2 = cbf.getBucket(i);
            long merged = b.value + b2.value;
            b.set(merged > MAX_COUNT ? MAX_COUNT : merged);
        }
    }

    Bucket getBucket(int i) {
        return new Bucket(i);
    }

    public boolean isPresent(String key)
    {
        for (int bucketIndex : getHashBuckets(key)) {
            Bucket bucket = new Bucket(bucketIndex);
            if (bucket.value == 0) {
                return false;
            }
        }
        return true;
    }

    /*
     param@ key -- value whose hash is used to fill
     the filter_.
     This is a general purpose API.
     */
    public void add(String key)
    {
        assert key != null;
        for (int bucketIndex : getHashBuckets(key)) {
            Bucket bucket = new Bucket(bucketIndex);
            if(bucket.value < MAX_COUNT) {
                bucket.set(bucket.value + 1);
            }
        }
    }

    public void delete(String key)
    {
        if (!isPresent(key)) {
            // TODO test this
            throw new IllegalArgumentException("key is not present");
        }

        for (int bucketIndex : getHashBuckets(key)) {
            Bucket bucket = new Bucket(bucketIndex);
            if(bucket.value >= 1 && bucket.value < MAX_COUNT) {
                bucket.set(bucket.value - 1);
            }
        }
    }

    int buckets() {
        return filter_.length * BUCKETS_PER_WORD;
    }

    private class Bucket {
        public final int wordIndex;
        public final int shift;
        public final long mask;
        public final long value;

        public Bucket(int bucketIndex) {
            wordIndex = bucketIndex >> 4;
            shift = (bucketIndex & 0x0f) << 2;

            mask = 15L << shift;
            value = ((filter_[wordIndex] & mask) >>> shift);
        }

        void set(long value) {
            filter_[wordIndex] = (filter_[wordIndex] & ~mask) | (value << shift);
        }
    }

    ICompactSerializer tserializer() {
        return serializer_;
    }

    int emptyBuckets() {
        int n = 0;
        for (int i = 0; i < buckets(); i++) {
            if (new Bucket(i).value == 0) {
                n++;
            }
        }
        return n;
    }}

class CountingBloomFilterSerializer implements ICompactSerializer<CountingBloomFilter>
{
    /*
     * The following methods are used for compact representation
     * of BloomFilter. This is essential, since we want to determine
     * the size of the serialized Bloom Filter blob before it is
     * populated armed with the knowledge of how many elements are
     * going to reside in it.
     */

    public void serialize(CountingBloomFilter cbf, DataOutputStream dos)
            throws IOException
    {
        /* write the number of hash functions used */
        dos.writeInt(cbf.getHashCount());
        /* write length of the filter */
        dos.writeInt(cbf.filter_.length);
        for (int i = 0; i < cbf.filter_.length; i++) {
            dos.writeLong(cbf.filter_[i]);
        }
    }

    public CountingBloomFilter deserialize(DataInputStream dis) throws IOException
    {
        /* read the number of hash functions */
        int hashes = dis.readInt();
        /* read the length of the filter */
        int length = dis.readInt();
        long[] filter = new long[length];
        for (int i = 0; i < length; i++) {
            filter[i] = dis.readLong();
        }
        return new CountingBloomFilter(hashes, filter);
    }
}
