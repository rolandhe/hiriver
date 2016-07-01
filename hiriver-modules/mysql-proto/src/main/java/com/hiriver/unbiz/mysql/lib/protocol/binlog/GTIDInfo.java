package com.hiriver.unbiz.mysql.lib.protocol.binlog;

/**
 * 描述一个gtid，format: uuid:transactionid
 * 
 * @author hexiufeng
 *
 */
public class GTIDInfo {
    private final String uuid;
    
    
    private final long start;
    private final long stop;

    public GTIDInfo(String uuid, long start, long stop){
        this.uuid = uuid;
        this.start = start;
        this.stop = stop;
    }
    
    /**
     * 构造器
     * 
     * @param gtidInfoString uuid:[3-]12
     */
    public GTIDInfo(String gtidInfoString) {
        String[] array = gtidInfoString.split(":");
        uuid = array[0];
        if(array[1].indexOf('-') >= 0){
            String[] numPosArray = array[1].split("-");
            start = Long.parseLong(numPosArray[0]);
            stop = Long.parseLong(numPosArray[1]);
        }else{
            start = 1L;
            long end = Long.parseLong(array[1]);
            stop = end;
        }
        
    }

    public long getStart() {
        return start;
    }

    public String getUuid() {
        return uuid;
    }

//    public byte[] getUuidBytes() {
//        return GTSidTool.convertSidString2DumpFormatBytes(uuid);
//    }

    public long getStop() {
        return stop;
    }

    
    @Override
    public String toString(){
        return this.uuid + ":" + start + "-" + stop;
    }
    
    public String toShortString(){
        return this.uuid + ":" + stop;
    }
}
