package com.facebook.infrastructure.utils;

import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.io.DataOutputBuffer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

public class FilterTest {
    public void testManyHashes(Iterator<String> keys) {
        int MAX_HASH_COUNT = 128;
        Set<Integer> hashes = new HashSet<Integer>();
        int collisions = 0;
        while (keys.hasNext()) {
            hashes.clear();
            for (int hashIndex : Filter.getHashBuckets(keys.next(), MAX_HASH_COUNT, 1024*1024)) {
                hashes.add(hashIndex);
            }
            collisions += (MAX_HASH_COUNT - hashes.size());
        }
        synchronized (System.out) {
            System.out.println("Collisions: " + collisions);
            System.out.flush();
        }
        assert collisions <= 100;
    }

    @Test
    public void testManyRandom() {
        testManyHashes(randomKeys());
    }

    // used by filter subclass tests

    static final double MAX_FAILURE_RATE = 0.1;
    public static final BloomCalculations.BloomSpecification spec = BloomCalculations.computeBucketsAndK(MAX_FAILURE_RATE);
    static final int ELEMENTS = 10000;

    static final ResetableIterator<String> intKeys() {
        return new KeyGenerator.IntGenerator(ELEMENTS);
    }
    static final ResetableIterator<String> randomKeys() {
        return new KeyGenerator.RandomStringGenerator(314159, ELEMENTS);
    }
    static final ResetableIterator<String> randomKeys2() {
        return new KeyGenerator.RandomStringGenerator(271828, ELEMENTS);
    }

    static int total_fp;

    public static void testFalsePositives(Filter f, ResetableIterator<String> keys, ResetableIterator<String> otherkeys) {
        assert keys.size() == otherkeys.size();

        while(keys.hasNext()) {
            f.add(keys.next());
        }
        System.out.println("keys added");
        /*
        keys.reset();
        while(keys.hasNext()) {
            assert f.isPresent(keys.next());
        }
        */

        int fp = 0;
        while(otherkeys.hasNext()) {
            if (f.isPresent(otherkeys.next())) {
                fp++;
            }
        }
        total_fp += fp;
        synchronized (System.out) {
            System.out.println("Total/this false positives: " + total_fp + "/" + fp);
        }
        assert fp < 1.01 * keys.size() * MAX_FAILURE_RATE;
    }

    public static Filter testSerialize(Filter f) throws IOException {
        f.add("a");
        DataOutputBuffer out = new DataOutputBuffer();
        f.tserializer().serialize(f, out);

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        Filter f2 = (Filter)f.tserializer().deserialize(in);

        assert f2.isPresent("a");
        assert !f2.isPresent("b");
        return f2;
    }

}
