package edu.usfca.cs.dfs.utils;

public class Constant {

    // TODO when deploy on orion, FILEPATH_PREFIX must be -> /bigdata/${whoami}/
    public static String FILEPATH_PREFIX = "TestFiles/";
    // public static String FILEPATH_PREFIX = "/bigdata/mchen81/";

    public static final int CHUNK_SIZE = 128 * 1024 * 1024;
    public static final int BLOOM_FILTER_FILTERS = 100000;
    public static final int BLOOM_FILTER_HASHES = 3;

    public static final int NETTY_MAX_LENGTH_FRAME = 256 * 1024 * 1024;

    public static String getTmpFilePath() {
        return FILEPATH_PREFIX + "tmp/";
    }

    public static String getLogFilePath() {
        return FILEPATH_PREFIX + "log4j/";
    }
}
