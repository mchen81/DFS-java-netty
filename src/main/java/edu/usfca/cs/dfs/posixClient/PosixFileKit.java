package edu.usfca.cs.dfs.posixClient;


import java.io.File;

public class PosixFileKit {
    public String fileName;
    public String checkSum;
    public long length;
    public File file;
    public String fileActualPath;


    public PosixFileKit(String fileName, String checkSum, long length) {
        this.fileName = fileName;
        this.checkSum = checkSum;
        this.length = length;
    }

    public PosixFileKit(String fileName, long length) {
        this.fileName = fileName;
        this.length = length;
    }

    public PosixFileKit(String fileName, String checkSum) {
        this.fileName = fileName;
        this.checkSum = checkSum;
    }
}
