package edu.usfca.cs.dfs.utils;

import com.google.protobuf.ByteString;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Before running this test input command:
 * mkfile -n 30m TestFiles/file.txt
 * It will create a 30MB file
 */
public class FileProcessUtilTest {

    @Test
    public void fileSizeTest() {
        File testFile = new File("TestFiles/file.txt"); //30 MB
        Assert.assertEquals(30 * 1024 * 1024, testFile.length());
    }

//    @Test
//    public void chunkNumberCalculateTest() throws IOException {
//        FileProcessUtil.sizeOfChunk = 100 * 1024;
//        File testFile = new File("TestFiles/file.txt"); //30 MB
//        byte[] data = FileUtils.readFileToByteArray(testFile);
//        int chunkNumber = FileProcessUtil.getChunkNumber(data);
//        int expectedChunkNumber = (int) Math.ceil((data.length / (double) FileProcessUtil.sizeOfChunk));
//        Assert.assertEquals(expectedChunkNumber, chunkNumber);
//    }

//    @Test
//    public void splitFileTest() throws IOException {
//        FileProcessUtil.sizeOfChunk = 100 * 1024;
//        File testFile = new File("TestFiles/file.txt"); //30 MB
//        byte[] data = FileUtils.readFileToByteArray(testFile);
//        ArrayList<ByteString> byteStrings = FileProcessUtil.split(testFile);
//        Assert.assertEquals(FileProcessUtil.getChunkNumber(data), byteStrings.size());
//    }

//    @Test
//    public void reconstructFileTest() throws IOException {
//        FileProcessUtil.sizeOfChunk = 100 * 1024;
//        File testFile = new File("TestFiles/file.txt"); //30 MB
//        byte[] data = FileUtils.readFileToByteArray(testFile);
//        String checksum = ChecksumUtil.getMD5Checksum(data);
//        ArrayList<ByteString> byteStrings = FileProcessUtil.split(testFile);
//        byte[][] chunkData = new byte[byteStrings.size()][];
//        for (int i = 0; i < byteStrings.size(); i++) {
//            ByteString bytes = byteStrings.get(i);
//            chunkData[i] = bytes.toByteArray();
//        }
//        File reconstructedFile = new File("TestFiles/reconstructedFile.txt");
//        reconstructedFile.delete();
//        reconstructedFile = new File("TestFiles/reconstructedFile.txt");
//
//        FileProcessUtil.reconstructChunksToFile(chunkData, reconstructedFile);
//        String reconstructedFileChecksum = ChecksumUtil.getMD5Checksum(FileUtils.readFileToByteArray(reconstructedFile));
//        Assert.assertEquals(checksum, reconstructedFileChecksum);
//        reconstructedFile.delete();
//    }

    @Test
    public void hashedFileNameTest(){
        String A = "HelloWorld";
        String B = "Helloworld";
        String C = "helloworld";
        String D = "HelloWorld";

        String hashedA = FileProcessUtil.getHashedFilename(A);
        String hashedB = FileProcessUtil.getHashedFilename(B);
        String hashedC = FileProcessUtil.getHashedFilename(C);
        String hashedD = FileProcessUtil.getHashedFilename(D);

        Assert.assertFalse(hashedA.equals(hashedB));
        Assert.assertFalse(hashedA.equals(hashedC));
        Assert.assertTrue(hashedA.equals(hashedD));
        System.out.println(hashedA);
    }

    @Test
    public void willOverwrite() throws IOException{
        File testFile = new File("TestFiles/testfile.txt");
        String hello = "Heldffdffdflo";
        FileUtils.writeByteArrayToFile(testFile, hello.getBytes());
    }


    @Test
    public void fileEqual() throws IOException{
        Assert.assertEquals(
                ChecksumUtil.getMD5Checksum(FileUtils.readFileToByteArray(new File("TestFiles/file.txt"))),
                ChecksumUtil.getMD5Checksum(FileUtils.readFileToByteArray(new File("TestFiles/newFile.txt")))
        );
    }
}
