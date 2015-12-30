package com.hiriver.unbiz.mysql.lib.protocol.tool;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 
 * mysql密码加密实现工具
 * 
 * @author hexiufeng
 *
 */
public class PassSecure {

    private PassSecure() {
    }

    /**
     * 针对mysql "Secure Password Authentication"方式的实现，mysql支持5中密码验证方式., 其中"Secure Password Authentication"
     * 是比较安全的一种，又分为插件支持和非插件支持，我们实现 非插件支持的Authentication::Native41方式.
     * 
     * @see <a href="http://dev.mysql.com/doc/internals/en/authentication-method.html">http://dev.mysql.com/doc/
     *      internals/en/authentication-method.html</a>
     * @see <a href=
     *      "http://dev.mysql.com/doc/internals/en/secure-password-authentication.html#packet-Authentication::Native41">
     *      http://dev.mysql.com/doc/internals/en/secure-password-authentication.html#packet-Authentication::Native41
     *      </a>
     * 
     * @param password 密码
     * @param authRandom 来自mysql server的加密种子
     * @return 最终加密后的密码
     */
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

    /**
     * 按照mysql的规范对密码进行加密编码。规则如下:<br>
     * <p>
     * SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
     * </p>
     * 
     * <b>注意</b>:<br>
     * <p>
     * 本代码实现摘自mysql jdbc驱动的实现
     * </p>
     * 
     * @param pass 密码
     * @param seed 来在mysql server的动态加密种子，每次连接都不同，由mysql server产出
     * @param md sha-1算法实现实例
     * @return 加密后的密码
     */
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
