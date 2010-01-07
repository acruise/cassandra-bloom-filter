package com.facebook.infrastructure.utils;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class CountingBloomFilterTest {
    public CountingBloomFilter cbf;

    public CountingBloomFilterTest() {
        cbf = new CountingBloomFilter(FilterTest.ELEMENTS, FilterTest.spec.bucketsPerElement);
    }

    @BeforeMethod
    public void clear() {
        cbf.clear();
    }

    @Test
    public void testOne() {
        cbf.add("a");
        assert cbf.isPresent("a");
        assert cbf.maxBucket() == 1;
        assert !cbf.isPresent("b");

        cbf.delete("a");
        assert !cbf.isPresent("a");
        assert cbf.maxBucket() == 0;
    }

    @Test
    public void testCounting() {

        cbf.add("a");
        cbf.add("a");
        assert cbf.maxBucket() == 2;
        cbf.delete("a");
        assert cbf.isPresent("a");
        assert cbf.maxBucket() == 1;

        for (int i = 0; i < FilterTest.ELEMENTS; i++) {
            cbf.add(Integer.toString(i));
        }
        for (int i = 0; i < FilterTest.ELEMENTS; i++) {
            cbf.delete(Integer.toString(i));
        }
        assert cbf.isPresent("a");

        cbf.delete("a");
        assert !cbf.isPresent("a");
        assert cbf.maxBucket() == 0;
    }

    @Test
    public void testMerge() {
        cbf.add("a");
        cbf.add("a");

        CountingBloomFilter cbf2 = new CountingBloomFilter(FilterTest.ELEMENTS, FilterTest.spec.bucketsPerElement);
        cbf2.add("a");
        cbf2.add("a");

        cbf.merge(cbf2);
        assert cbf.maxBucket() == 4;
    }

    @Test
    public void testMergeMaxCount() {
        for (int i = 0; i < CountingBloomFilter.MAX_COUNT; i++) {
            cbf.add("b");
        }

        CountingBloomFilter cbf2 = new CountingBloomFilter(FilterTest.ELEMENTS, FilterTest.spec.bucketsPerElement);
        cbf2.add("b");

        cbf.merge(cbf2);
        assert cbf.maxBucket() == CountingBloomFilter.MAX_COUNT;
    }

    @Test
    public void testMaxCount() {
        for (int i = 0; i < CountingBloomFilter.MAX_COUNT; i++) {
            cbf.add("a");
        }
        assert cbf.maxBucket() == CountingBloomFilter.MAX_COUNT;
        cbf.add("a");
        assert cbf.maxBucket() == CountingBloomFilter.MAX_COUNT;
        cbf.delete("a");
        assert cbf.maxBucket() == CountingBloomFilter.MAX_COUNT;
    }

    @Test
    public void testFalsePositivesInt() {
        FilterTest.testFalsePositives(cbf, FilterTest.intKeys(), FilterTest.randomKeys2());
    }
    @Test
    public void testFalsePositivesRandom() {
        FilterTest.testFalsePositives(cbf, FilterTest.randomKeys(), FilterTest.randomKeys2());
    }
    @Test
    public void testWords() {
        CountingBloomFilter cbf2 = new CountingBloomFilter(KeyGenerator.WordGenerator.WORDS / 2, FilterTest.spec.bucketsPerElement);
        int skipEven = KeyGenerator.WordGenerator.WORDS % 2 == 0 ? 0 : 2;
        FilterTest.testFalsePositives(cbf2,
                                      new KeyGenerator.WordGenerator(skipEven, 2),
                                      new KeyGenerator.WordGenerator(1, 2));
    }

    @Test
    public void testSerialize() throws IOException {
        CountingBloomFilter cbf2 = (CountingBloomFilter) FilterTest.testSerialize(cbf);
        assert cbf2.maxBucket() == 1;
    }
}
