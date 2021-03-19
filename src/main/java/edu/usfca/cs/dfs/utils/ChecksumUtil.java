package edu.usfca.cs.dfs.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ChecksumUtil {

    /**
     * refers from https://stackoverflow.com/questions/304268/getting-a-files-md5-checksum-in-java
     * @param dataBytes
     * @return
     */
    public static String getMD5Checksum(byte[] dataBytes){
        try{
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(dataBytes, 0, dataBytes.length);
            byte[] digestBytes = messageDigest.digest();
            StringBuffer sb = new StringBuffer("");
            for (int i = 0; i < digestBytes.length; i++) {
                sb.append(Integer.toString((digestBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return sb.toString();
        }catch (NoSuchAlgorithmException e){
            System.out.println("Cannot find MD5 Algorithm");
            return null;
        }
    }

}
