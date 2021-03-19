package edu.usfca.cs.dfs.utils;

import com.sangupta.murmur.Murmur3;

import java.util.ArrayList;
import java.util.BitSet;

public class BloomFilter {
    private int filters;
    private int hashs;
    private BitSet bitsets;
    private ArrayList<Integer> seed = new ArrayList<Integer>();
    private long count;

    public BloomFilter(int filters, int hashs) {
        this.filters = filters;
        this.hashs = hashs;
        bitsets = new BitSet(filters);
        count = 0;
        for (int i = 0; i < hashs; i++) {
            seed.add(i);
        }
    }

    public void put(byte[] data) {
        count += 1;
        for (int i : seed) {
            bitsets.set((int) (Murmur3.hash_x86_32(data, data.length, i) % filters), true);
        }
    }

    public boolean get(byte[] data) {
        boolean contain = true;
        for (int i : seed) {
            contain = contain && bitsets.get((int) (Murmur3.hash_x86_32(data, data.length, i) % filters));
        }
        return contain;
    }
}