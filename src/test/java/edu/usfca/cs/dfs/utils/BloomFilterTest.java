package edu.usfca.cs.dfs.utils;

import org.junit.Assert;
import org.junit.Test;

public class BloomFilterTest {

    @Test
    public void putTest() {
        String t1 = "Hello World";
        String t2 = "Hello WORLD";
        String t3 = "Holly Woood";
        String t4 = "MMMMMMMMMMM";
        BloomFilter bf = new BloomFilter(1000000, 3);
        bf.put(t1.getBytes());
        bf.put(t2.getBytes());
        bf.put(t3.getBytes());
        Assert.assertTrue(bf.get(t1.getBytes()));
        Assert.assertTrue(bf.get(t2.getBytes()));
        Assert.assertTrue(bf.get(t3.getBytes()));
        Assert.assertFalse(bf.get(t4.getBytes()));
    }
}
