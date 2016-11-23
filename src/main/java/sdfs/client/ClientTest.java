package sdfs.client;

import sdfs.datanode.DataNodeServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alex on 19/11/2016.
 */
public class ClientTest {
    static SDFSClient sdfsClient = new SDFSClient();
    public static void main(String[] args) {
        testList();
//        testMkdir("root/test/t.txt");
//        testMutiClient();
//        try {
//            testAppendData();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public static void testList(){
        List<Integer> l = new ArrayList<>();
        l.add(12);
        l.add(34);
        l.add(9);
        l.add(22);
        l.add(81);
        l.add(17);

        l.remove(2);
        for (int i = 0;i<l.size();i++){
            System.out.print(l.get(i)+" ");
        }
        System.out.println();
        l.add(2,61);
        for (int i = 0;i<l.size();i++){
            System.out.print(l.get(i)+" ");
        }
        System.out.println();
    }

    public  static void testMkdir(String fileUri) {
        try {
            sdfsClient.mkdir(fileUri);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testMutiClient(){

        String filename = "home/ttttttttt.txt";
        try {
//            sdfsClient.mkdir("home");
            SDFSFileChannel sdfsFileChannel1 = sdfsClient.create(filename);
            sdfsFileChannel1.write(ByteBuffer.allocate(1000000));
            SDFSFileChannel sdfsFileChannel2 = sdfsClient.openReadonly(filename);
            System.out.println(sdfsFileChannel2.getFileNode().getBlockAmount());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void testruncate(){
        String filename = "home/tttttttttt.txt";
        try {
//            sdfsClient.mkdir("home");
            SDFSFileChannel sdfsFileChannel1 = sdfsClient.create(filename);
            sdfsFileChannel1.write(ByteBuffer.allocate(1000000));
            sdfsFileChannel1.close();

            SDFSFileChannel sdfsFileChannel2 = sdfsClient.openReadWrite(filename);
            sdfsFileChannel2.truncate(0);
            System.out.println(sdfsFileChannel2.getFileNode().getBlockAmount());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testAppendData() throws IOException {
        int fileSize = 2* 64*1024+2;
        int secondPosition = 3*64*1024-1;
        String filename = "home/append13.txt";
        ByteBuffer dataBuffer = writeData(filename);
        SDFSFileChannel channel1 = sdfsClient.openReadWrite(filename);
        ByteBuffer buffer = ByteBuffer.allocate(fileSize);
        buffer.position(0);
        channel1.read(buffer);

        channel1.position(secondPosition);
        buffer.position(0);

        System.out.println("channel1.read(buffer)==0: "+(channel1.read(buffer)==0));
        System.out.println("channel1.write(dataBuffer)==0"+(channel1.write(dataBuffer)==0));

        dataBuffer.position(0);
        System.out.println("channel1.write(dataBuffer)==0"+(channel1.write(dataBuffer)==fileSize));



        ByteBuffer testBuffer2 = ByteBuffer.allocate(20);
        channel1.position(secondPosition);
        channel1.read(testBuffer2);
        System.out.println("testBuffer.position():"+testBuffer2.position());
        testBuffer2.position(0);
        for (int i = 0;i<20;i++){
            System.out.println(testBuffer2.get());
        }

        channel1.position(0);
        System.out.println("channel1.read(buffer)==fileSize"+(channel1.read(buffer)==fileSize));


        channel1.truncate(secondPosition+fileSize+1);
        buffer.position(0);
        System.out.println("channel position: "+channel1.position());
        channel1.read(buffer);
        buffer.position(0);

        for (int i=0;i<64*1024+10;i++){
            if (i>65530){
                System.out.println(buffer.get());
            }else{
                buffer.get();
            }
        }

        ByteBuffer testBuffer = ByteBuffer.allocate(20);
        channel1.position(secondPosition);
        channel1.read(testBuffer);
        System.out.println("testBuffer.position():"+testBuffer.position());
        testBuffer.position(0);
        for (int i = 0;i<20;i++){
            System.out.println(testBuffer.get());
        }

//        System.out.println(buffer.get()==(byte)0);
//        System.out.println(buffer.get()==(byte)1);
//        System.out.println(buffer.get()==(byte)2);
//        System.out.println(buffer.get()==(byte)3);
//        System.out.println(buffer.get()==(byte)4);


    }

    public static ByteBuffer writeData(String filename){
        int fileSize = 2* 64*1024+2;
        try {
            SDFSFileChannel channel = sdfsClient.create(filename);
            ByteBuffer byteBuffer = ByteBuffer.allocate(fileSize);
            for (int i = 0;i<byteBuffer.capacity();i++){
                byteBuffer.put((byte) i);
            }
            byteBuffer.position(0);
            channel.write(byteBuffer);
            channel.close();
            return byteBuffer;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  null;
    }
}
