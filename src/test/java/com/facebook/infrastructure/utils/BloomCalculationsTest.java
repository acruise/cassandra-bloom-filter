package com.facebook.infrastructure.utils;

import org.testng.annotations.Test;

public class BloomCalculationsTest {
    @Test
    public void testComputeBestK() {
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBucketsAndK(0.0001);
        assert BloomCalculations.computeBestK(spec.bucketsPerElement) == spec.K;
    }

    @Test
    public void testComputeBucketsAndK() {
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBucketsAndK(0.0001);
        assert spec.bucketsPerElement == 15;
        assert spec.K == 8;
    }
}
