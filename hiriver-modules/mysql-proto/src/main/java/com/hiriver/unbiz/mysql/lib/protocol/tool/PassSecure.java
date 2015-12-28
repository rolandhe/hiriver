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
        byte[] pass1 = md.digest(pass);
        md.reset();
        byte[] pass2 = md.digest(pass1);
        md.reset();
        md.update(seed);
        byte[] pass3 = md.digest(pass2);
        for (int i = 0; i < pass3.length; i++) {
            pass3[i] = (byte) (pass3[i] ^ pass1[i]);
        }
        return pass3;
    }
}
