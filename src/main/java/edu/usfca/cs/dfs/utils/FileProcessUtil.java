package edu.usfca.cs.dfs.utils;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.ArrayList;

public class FileProcessUtil {
    //set the maximum size of a chunk to 64MB
    public static int sizeOfChunk = Constant.CHUNK_SIZE;

    /**
     * return sorted chunk list
     * modified code from https://stackoverflow.com/questions/10864317/how-to-break-a-file-into-pieces-using-java
     *
     * @param file: rhe file we want to split
     * @throws IOException
     */
    public static ArrayList<String> split(File file) {
        double chunkSize = sizeOfChunk;
        double chunkNumber = (double) file.length() / chunkSize;
        chunkNumber = Math.ceil(chunkNumber);
        ArrayList<String> chunkList = new ArrayList<>();
        int lastChunk = (int) chunkNumber - 1;
        try {
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
            for (int i = 0; i < (int) chunkNumber; i++) {
                if (i == lastChunk) { // the last chunk
                    byte[] buffer = new byte[(int) (file.length() - (i * sizeOfChunk))];
                    bufferedInputStream.read(buffer);
                    String tmpChunkName = Constant.getTmpFilePath() + file.getName() + Integer.toString(i + 1);
                    FileUtils.writeByteArrayToFile(new File(tmpChunkName)
                            , buffer);
                    chunkList.add(tmpChunkName);
                    // chunkList.add(ByteString.copyFrom(buffer, 0, (int)(file.length() - (i * sizeOfChunk))));
                } else {
                    byte[] buffer = new byte[sizeOfChunk];
                    bufferedInputStream.read(buffer);
                    String tmpChunkName = Constant.getTmpFilePath() + file.getName() + Integer.toString(i + 1);
                    FileUtils.writeByteArrayToFile(new File(tmpChunkName)
                            , buffer);
                    chunkList.add(tmpChunkName);
                    //chunkList.add(ByteString.copyFrom(buffer, 0, sizeOfChunk));
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        return chunkList;
    }

    /**
     * modified code from https://stackoverflow.com/questions/10864317/how-to-break-a-file-into-pieces-using-java
     *
     * @param chunks          the sorted chunk list
     * @param destinationFile the file that receive the merged file
     * @throws IOException
     */
    public static void reconstructChunksToFile(byte[][] chunks, File destinationFile) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(destinationFile, true);
        for (byte[] chunk : chunks) {
            fileOutputStream.write(chunk);
            fileOutputStream.flush();
        }
        fileOutputStream.close();
    }

    public static void reconstructChunksToFile(int chunksNumber, File destinationFile) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(destinationFile, true);
        for (int i = 1; i <= chunksNumber; i++) {
            File chunk = new File(Constant.getTmpFilePath() + i);
            byte[] buffer = new byte[(int) chunk.length()];
            FileInputStream fileInputStream = new FileInputStream(chunk);
            fileInputStream.read(buffer);
            fileInputStream.close();
            fileOutputStream.write(buffer);
            fileOutputStream.flush();
        }
        fileOutputStream.close();
    }

    public static int getChunkNumber(long length) {
        return (int) Math.ceil(((double) length / (double) sizeOfChunk));
    }

    public static String getHashedFilename(String filepath) {
        return ChecksumUtil.getMD5Checksum(filepath.getBytes());
    }
}
