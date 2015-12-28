package com.hiriver.unbiz.mysql.lib;

public enum MyCharset {

    LATIN1_SWEDISH(0x08, "latin1_swedish_ci", "latin1"), UTF8(0x21, "utf8_general_ci", "utf-8"), BINARY(0x3f, "binary",
            "binary"), UNKNOWN(0, "", "");

    private MyCharset(int charset, String collation, String charsetName) {
        this.charset = charset;
        this.collation = collation;
        this.charsetName = charsetName;
    }

    public static MyCharset ofCharset(int charset) {
        for (MyCharset cst : MyCharset.values()) {
            if (cst.getCharset() == charset) {
                return cst;
            }
        }
        return UNKNOWN;
    }

    private final int charset;
    private final String collation;
    private final String charsetName;

    public int getCharset() {
        return charset;
    }

    public String getCollation() {
        return collation;
    }

    public String getCharsetName() {
        return charsetName;
    }

}
