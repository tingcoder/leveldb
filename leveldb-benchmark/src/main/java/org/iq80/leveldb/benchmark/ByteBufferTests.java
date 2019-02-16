package org.iq80.leveldb.benchmark;

import com.google.common.base.Stopwatch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/***
 * @author yfeng
 * @date 2019-02-15 11:15
 */
public class ByteBufferTests {
    String dir = "C:\\Users\\Administrator\\Downloads\\mbf\\";

    public static void main(String[] args) throws Exception {
        ByteBufferTests bbt = new ByteBufferTests();
        bbt.testMappedByteBuffer();
    }

    public void testMappedByteBuffer() throws Exception {
        RandomAccessFile inFile = new RandomAccessFile(dir + "data.txt", "rw");
        RandomAccessFile outFile = new RandomAccessFile(dir + "data-cp.txt", "rw");
        FileChannel inChannel = inFile.getChannel();
        FileChannel outChannel = outFile.getChannel();

        long fileSize = inChannel.size();
        System.out.println("文件大小:" + fileSize / 1000 + "kb");

        Stopwatch stopwatch = Stopwatch.createStarted();
        MappedByteBuffer readMbb = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
        System.out.println("map文件消耗:" + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "毫秒");

        stopwatch.reset().start();
        MappedByteBuffer writeMbb = outChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        stopwatch.reset().start();
        writeMbb.put(readMbb);
        System.out.println("写入内容消耗:" + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "毫秒");

        inChannel.close();
        outChannel.close();

        TimeUnit.MINUTES.sleep(3);
    }

    public void testByteBuffer() throws Exception {
        int mb = 1024 * 1024;
        ByteBuffer buffer = ByteBuffer.allocate(14 * mb);
        FileInputStream fis = new FileInputStream(new File(dir + "dump-02-14.bin"));
        FileOutputStream fos = new FileOutputStream(new File(dir + "dump-02-14.bin.out"));
        FileChannel inChannel = fis.getChannel();
        FileChannel outChannel = fos.getChannel();

        Stopwatch stopwatch = Stopwatch.createStarted();
        inChannel.read(buffer);
        int msize = buffer.array().length / mb;
        System.out.println("读取" + msize + "MB 消耗:" + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "毫秒");

        stopwatch.reset().start();
        buffer.flip();
        outChannel.write(buffer);
        System.out.println("写入" + msize + "MB 消耗:" + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "毫秒");


        inChannel.close();
        outChannel.close();
        fis.close();
        fos.close();
    }
}
