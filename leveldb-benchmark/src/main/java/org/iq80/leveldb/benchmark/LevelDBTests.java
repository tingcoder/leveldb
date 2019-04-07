package org.iq80.leveldb.benchmark;

import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.*;
import org.iq80.leveldb.impl.Iq80DBFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/***
 * @author yfeng
 * @date 2019-01-22 11:58
 */
@Slf4j
public class LevelDBTests {
    private static final String dbDir = "E://BigData//LevelDB//db1";
    byte[] keyBytes = "user-1-name".getBytes();

    private DB dbInstance;

    public static void main(String[] args) throws IOException {
        LevelDBTests lt = new LevelDBTests();
        // lt.writeData("user-2-name", "雨果");
       //  lt.writeData("user-3-name", "雨果2-d名字");

        lt.deleteData("user-4-name");

        /*
        Map<String, String> batchData = new HashMap<>();
        batchData.put("user-1-name", "Frank");
        batchData.put("user-3-name", "诸葛亮");
        batchData.put("user-1-nickName", "弗兰克");
        batchData.put("user-2-nickName", "YuGuo");
        batchData.put("user-3-nickName", "孔明");
        lt.batchInsert(batchData);

        long start = System.currentTimeMillis();
        for (int i = 1; i <= 10000; i++) {
            lt.writeData("un-" + i + "-name", "昵称-" + i);
        }
        long spend = System.currentTimeMillis() - start;
        System.out.println("10000条消息，花费" + spend + "毫秒");*/


        String val = lt.getData("user-3-name");
        System.out.println(val);

        //关闭数据库
        lt.closeDB();
    }

    private void iterateDB() throws IOException {
        DB db = getDB();
        DBIterator dbIterator = db.iterator();
        while (dbIterator.hasNext()) {
            Map.Entry<byte[], byte[]> entry = dbIterator.next();
            String key = new String(entry.getKey());
            String value = new String(entry.getValue());
            System.out.printf("结果: %s => %s \n", key, value);
        }
    }

    //批量写入数据
    private void batchInsert(Map<String, String> kvMap) throws IOException {
        DB db = getDB();
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.sync(true);

        WriteBatch writeBatch = db.createWriteBatch();
        kvMap.entrySet().forEach((entry) -> {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value != null) {
                writeBatch.put(key.getBytes(), value.getBytes());
            } else {
                writeBatch.delete(key.getBytes());
            }
        });
        db.write(writeBatch, writeOptions);
    }

    private DB getDB() throws IOException {
        if (dbInstance == null) {
            synchronized (this) {
                Options opts = new Options();
                opts.createIfMissing(true);
                opts.compressionType(CompressionType.NONE);
                DBFactory dbFactory = new Iq80DBFactory();
                log.info("创建DB实例---------------------------------------开始");
                dbInstance = dbFactory.open(new File(dbDir), opts);
                log.info("创建DB实例---------------------------------------成功");
            }
        }
        return dbInstance;
    }

    private void closeDB() {
        if (dbInstance != null) {
            try {
                dbInstance.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private void writeData(String k, String v) throws IOException {
        DB db = getDB();
        log.info("写入----------------------开始");
        db.put(k.getBytes(), v.getBytes());
        log.info("写入----------------------成功");
    }

    private String getData(String k) throws IOException {
        DB db = getDB();
        byte[] datas = db.get(k.getBytes());
        if (datas == null || datas.length == 0) {
            return null;
        }
        return new String(datas);
    }

    private void deleteData(String k) throws IOException {
        DB db = getDB();
        db.delete(k.getBytes());
    }

    public void testLevelDB() {
        try {

            String key = "user-1-name";
            //写入数据
            writeData(key, "Frank");

            //查询数据
            long start = System.currentTimeMillis();
            String queryResult = getData(key);
            System.out.println("查询返回: " + queryResult + " 耗时" + (System.currentTimeMillis() - start) + "毫秒");

            //删除数据
            deleteData(key);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeDB();
        }
    }
}
