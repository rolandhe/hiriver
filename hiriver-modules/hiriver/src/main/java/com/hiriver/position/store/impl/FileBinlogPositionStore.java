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

/**
 * 基于文件的同步点存储实现
 * 
 * @author hexiufeng
 *
 */
public class FileBinlogPositionStore extends AbstractBinlogPositionStore implements BinlogPositionStore {
    private static final Logger LOG = LoggerFactory.getLogger(FileBinlogPositionStore.class);
    private static final int MAX_BUFFER = 2048;
    private String filePath;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    private String getFileName(String channelId) {
        return filePath + "/" + channelId;
    }

    @Override
    protected void storeImpl(byte[] posBuf, String channelId) {
        OutputStream output = null;
        try {
            output = new FileOutputStream(getFileName(channelId));
            IOUtils.write(posBuf, output);
        } catch (FileNotFoundException e) {
            LOG.error("store binlog pos error " + channelId, e);
        } catch (IOException e) {
            LOG.error("store binlog pos error " + channelId, e);
        } finally {
            if (output != null) {
                IOUtils.closeQuietly(output);
            }
        }

    }

    @Override
    protected byte[] loadImpl(String channelId) {
        byte[] buffer = new byte[MAX_BUFFER];

        InputStream input = null;
        File file = new File(getFileName(channelId));
        if (!file.exists()) {
            return null;
        }
        try {

            input = new FileInputStream(file);
            int len = IOUtils.read(input, buffer, 0, MAX_BUFFER);
            byte[] posBuf = new byte[len];
            System.arraycopy(buffer, 0, posBuf, 0, len);
            return posBuf;
        } catch (FileNotFoundException e) {
            LOG.error("read binlog pos error " + channelId, e);
        } catch (IOException e) {
            LOG.error("read binlog pos error " + channelId, e);
        }
        return null;
    }

}
