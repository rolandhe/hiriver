package com.hiriver.unbiz.mysql.lib.protocol.binlog;

/**
 * 单个gtid，uuid:[12-]45
 * 
 * @author hexiufeng
 *
 */
public class GtId {
    private final String uuid;
    private GtIdInterval internel;

    public void setInternel(GtIdInterval internel) {
        this.internel = internel;
    }

    public GtId(String uuid, long start, long stop) {
        this.uuid = uuid;
        this.internel = new GtIdInterval(start,stop);
    }

    /**
     * 构造器
     * 
     * @param gtidInfoString uuid:[3-]12
     */
    public GtId(String gtidInfoString) {
        String[] array = gtidInfoString.split(":");
        uuid = array[0];
        this.internel = new GtIdInterval(array[1]);
    }

    public String getUuid() {
        return uuid;
    }

    
    public GtIdInterval getInternel() {
        return internel;
    }

    @Override
    public String toString(){
        return this.uuid + ":" + internel.toShortString();
    }
    
    
    public GtId cloneGtId(){
        return new GtId(uuid,internel.getStart(),internel.getStop());
    }
    
    public GtId cloneNextGtId(){
        return new GtId(uuid,internel.getStart(),internel.getStop() + 1L);
    }
}
