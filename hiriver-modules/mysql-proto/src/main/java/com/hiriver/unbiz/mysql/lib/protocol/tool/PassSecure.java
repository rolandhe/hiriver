package com.hiriver.unbiz.mysql.lib.protocol.tool;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class PassSecure {

    private PassSecure() {
    }

    public static byte[] nativeMysqlSecure(String password, byte[] authRandom) {
        byte[] passBytes;
        try {
            passBytes = password.getBytes("utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }

        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e);
        }

        return scramble411(passBytes, authRandom, md);
    }

    private static final byte[] scramble411(byte[] pass, byte[] seed, MessageDigest md) {
        byte[] passwordHashStage1 = md.digest(pass);
        md.reset();

        byte[] passwordHashStage2 = md.digest(passwordHashStage1);
        md.reset();

        md.update(seed);
        md.update(passwordHashStage2);

        byte[] toBeXord = md.digest();

        int numToXor = toBeXord.length;

        for (int i = 0; i < numToXor; i++) {
            toBeXord[i] = (byte) (toBeXord[i] ^ passwordHashStage1[i]);
        }

        return toBeXord;
        
    }
}
