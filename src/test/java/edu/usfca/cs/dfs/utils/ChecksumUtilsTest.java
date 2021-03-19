package edu.usfca.cs.dfs.utils;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ChecksumUtilsTest {

    @Test
    public void testMD5ChecksumInString(){
        byte[] data = "I am a good guy".getBytes();
        String checksum = ChecksumUtil.getMD5Checksum(data);
        Assert.assertEquals("57ba368396c54229b4c90f282986c266", checksum);
    }

    @Test
    public void testChecksumMD5() {
        try{
            byte[] data = FileUtils.readFileToByteArray(new File("src/test/resources/testMD5.txt"));
            String checksum = ChecksumUtil.getMD5Checksum(data);
            // Expected value is calculated by http://onlinemd5.com
            Assert.assertEquals("042260DA99409F2E3628AD47BDF67128", checksum.toUpperCase());
        }catch (IOException e){
            Assert.fail("cannot find the file");
        }
    }

}
