package org.corfudb.perfclient;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

/**
 * Created by m on 4/26/18.
 */
public class FsyncTest {

    public static void main(String[] args) throws Exception  {

        String fileName = "/home/m/txLog.data";
        //fileName = "/media/m/corfuStore/txLog.data";
        new File(fileName).delete();
        int payloadSize = 1000;
        int uuidSize = Long.BYTES * 4;
        byte[] payload = new byte[uuidSize + payloadSize];
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        FileChannel channel = FileChannel.open(FileSystems.getDefault().getPath(fileName),
                EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.SPARSE));

        int iter = 20_000;

        long s1 = System.currentTimeMillis();

        for (int x = 0; x < iter; x++) {
            // Simulate a batch of 3 writes, since there are 3 writers (best case)
            channel.write(buffer);
            channel.write(buffer);
            channel.write(buffer);
            channel.force(false);

        }

        long s2 = System.currentTimeMillis();

        System.out.println("time(ms) " + (s2 - s1));
        System.out.println("throughput(mb/s) " + ((iter * 3 * payload.length * 1.0)/1000000.0) / ((s2 - s1)/1000.0));
    }
}
