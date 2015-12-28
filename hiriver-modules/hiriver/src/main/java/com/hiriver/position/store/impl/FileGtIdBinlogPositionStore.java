package com.hiriver.position.store.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.position.store.BinlogPositionStore;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GTidBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

public class FileGtIdBinlogPositionStore implements BinlogPositionStore {
    private static final Logger LOG = LoggerFactory.getLogger(FileGtIdBinlogPositionStore.class);
    private static final int MAX_BUFFER = 2048;
    private String filePath;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void store(BinlogPosition binlogPosition, String channelId) {
        OutputStream output = null;
        try {
            output = new FileOutputStream(getFileName(channelId));
            IOUtils.write(binlogPosition.toBytesArray(), output);
        } catch (FileNotFoundException e) {
            LOG.error("store binlog pos error.", e);
        } catch (IOException e) {
            LOG.error("store binlog pos error.", e);
        }finally{
            if(output != null){
                IOUtils.closeQuietly(output);
            }
        }

    }

    private String getFileName(String channelId){
        return filePath + "/" + channelId;}

    @Override
    public BinlogPosition load(String channelId) {
        byte[] buffer = new byte[MAX_BUFFER];
        
        InputStream input = null;
        File file = new File(getFileName(channelId));
        if(!file.exists()){
            return null;
        }
        try {
            
            input = new FileInputStream(file);
            int len = IOUtils.read(input, buffer, 0, MAX_BUFFER);
            String line = new String(buffer,0,len,"UTF-8");
            return new GTidBinlogPosition(line);
        } catch (FileNotFoundException e) {
            LOG.error("read binlog pos error.", e);
        } catch (IOException e) {
            LOG.error("read binlog pos error.", e);
        }
        return null;
    }


}
