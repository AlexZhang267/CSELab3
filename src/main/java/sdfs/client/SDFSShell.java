package sdfs.client;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by alex on 25/10/2016.
 */


/**
 * 用于自己测试的shell
 */
public class SDFSShell {
//    static SimpleDistributedFileSystem simpleDistributedFileSystem = new SimpleDistributedFileSystem(new InetSocketAddress(2333), 16);

    static SDFSClient simpleDistributedFileSystem = new SDFSClient();
    public static void main(String[] args) {
        System.out.println("SDFS Shell: \nSupport Operation: MKDIR, PUT, GET, PRINT \nEnter help for more information");
        String str = "";
        do {
            System.out.print(">>> ");
            Scanner input = new Scanner(System.in);
            str = input.nextLine();
            /**
             * parse input commend
             */
            str = str.trim();
            String[] commends = str.split(" ");
            assert commends.length > 1;

            switch (commends[0].toLowerCase()) {
                case "mkdir":
                    if (commends.length == 2) {
                        mkdir(commends[1]);
                    }else {
                        System.out.println("ArgumentError: mkdir need 1 params");
                    }
                    break;
                case "put":
                    if (commends.length == 3) {
                        put(commends[1], commends[2]);
                    }else {
                        System.out.println("ArgumentError: put need 2 params");
                    }
                    break;
                case "create":
                    if (commends.length==2){
                        create(commends[1]);
                    }
                    break;
                case "get":
                    if (commends.length == 3) {
                        get(commends[1], commends[2]);
                    }else {
                        System.out.println("ArgumentError: get need 2 params");
                    }
                    break;
                case "printnodes":
                    printNodes();
                    break;
                case "testorw":
                    if (commends.length==2){
                        testOpenReadWrite(commends[1]);
                    }
                    break;
                case "openreadonly":
                    if (commends.length==2){
                        openReadonly(commends[1]);
                    }
                    break;
                case "openreadwrite":
                    if (commends.length==2){
                        openReadWrite(commends[1]);
                    }
                    break;
                case "help":
                    printHelpInfo();
                    break;
                case "testcreate":
                    testCreate(commends[1]);
                    break;
                default:
                    System.out.println("default option");
                    break;
            }
        } while (!str.toLowerCase().equals("exit"));
    }

    public static void testCreate(String fileUri){
        System.out.println("testCreate:"+fileUri);
        try {
            SDFSFileChannel sdfsFileChannel = simpleDistributedFileSystem.create(fileUri);

            System.out.println(0==sdfsFileChannel.size());
            System.out.println(0==sdfsFileChannel.getBlockAmount());
            System.out.println(sdfsFileChannel.isOpen());
            System.out.println(0==sdfsFileChannel.position());
            ByteBuffer byteBuffer= ByteBuffer.wrap(new byte[10]);
            System.out.println(0==sdfsFileChannel.read(byteBuffer));

            boolean goFlag = true;
            String inputStr;
            String[] commands;
            while (goFlag){
                System.out.println(">>> >>>");
                Scanner input  = new Scanner(System.in);
                inputStr = input.nextLine();
                commands = inputStr.split(" ");
                if (commands.length==0){
                    continue;
                }
                switch (commands[0]){
                    case "exit":
                        goFlag=false;
                        break;
                    case "position":
                        sdfsFileChannel.position(Integer.parseInt(commands[1]));
                        System.out.println(0==sdfsFileChannel.size());
                        System.out.println(0==sdfsFileChannel.getFileNode().getBlockAmount());
                        System.out.println(sdfsFileChannel.isOpen());
                        System.out.println(0==sdfsFileChannel.position());
                        ByteBuffer byteBuffer1= ByteBuffer.wrap(new byte[10]);
                        System.out.println(0==sdfsFileChannel.read(byteBuffer1));
                        break;
                    case "close":
                        sdfsFileChannel.close();
                        System.out.println("sdfs channel is closed:"+sdfsFileChannel.isOpen());
                        break;
                    case "size":
                        System.out.println(sdfsFileChannel.size());
                        break;
                    case "read":
                        ByteBuffer byteBuffer2 = ByteBuffer.wrap(new byte[2]);
                        sdfsFileChannel.read(byteBuffer2);
                        break;
                    case "write":
                        ByteBuffer byteBuffer3 = ByteBuffer.wrap(new byte[2]);
                        sdfsFileChannel.read(byteBuffer3);
                        break;

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testOpenReadWrite(String fileUri){
        System.out.println("testOpenReadWrite"+fileUri);
        try {
//            SDFSFileChannel channel = NameNodeStub.getNameNodeStub().openReadwrite(fileUri);
            SDFSFileChannel channel = simpleDistributedFileSystem.openReadWrite(fileUri);
            boolean goFlag=true;
            String inputStr;
            String[] commands;
            while (goFlag){
                System.out.print(">>> >>>");
                Scanner input  = new Scanner(System.in);
                inputStr = input.nextLine();
                commands = inputStr.split(" ");
                if (commands.length==0){
                    continue;
                }
                switch (commands[0]){
                    case "size":
                        System.out.println(channel.size());
                        break;
                    case "getPosition":
                        System.out.println(channel.position());
                        break;
                    case "setPosition":
                        channel.position(Integer.parseInt(commands[1]));
                        System.out.println("position now: "+channel.position());
                        break;
                    case "close":
                        channel.close();
                        break;
                    case "isOpen":
                        System.out.println(channel.isOpen());
                        break;
                    case "printBuffer":
//                        Map<Integer,Boolean> bufferDirty = channel.getBufferDirty();
//                        for (int key:bufferDirty.keySet()){
//                            System.out.println("Key: "+key+", dirty? "+bufferDirty.get(key));
//                        }
//                        List<Integer> LRUList = channel.getLRUList();
//                        System.out.println("print LRU list");
//                        for(int i : LRUList){
//                            System.out.println("    "+i);
//                        }
//
//                        break;
                    case "read":
                        int len = Integer.parseInt(commands[1]);
                        ByteBuffer byteBuffer =ByteBuffer.wrap(new byte[len]);
                        int size = channel.read(byteBuffer);
                        System.out.println("Read Size: "+size);
                        System.out.println(new String(byteBuffer.array(),"ASCII"));
                        break;
                    case "write":
                        String src="";
                        for (int i = 1;i<commands.length;i++){
                            src+=commands[i];
                        }
                        ByteBuffer byteBuffer1 = ByteBuffer.wrap(src.getBytes());
                        channel.write(byteBuffer1);
                        break;
                    case "flush":
                        channel.flush();
                        break;
                    case "exit":
                        goFlag=false;
                        break;
                    case "truncate":
                        channel.truncate(Integer.parseInt(commands[1]));
                        break;
                    default:
                        System.out.println("activate default case");
                        break;
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void mkdir(String fileUri){
        try {
            simpleDistributedFileSystem.mkdir(fileUri);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void printNodes(){
        simpleDistributedFileSystem.printNodes();
    }

    public static void put(String locaFile, String remoteDir) {
        File file = new File(locaFile);
        try {
            /*
            将输入的文件转化为byte[]
             */
            FileInputStream inputStream = new FileInputStream(file);
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            int i;
            while ((i = inputStream.read()) != -1) {
                byteArrayOutputStream.write(i);
            }
            inputStream.close();
            byte[] bytes = byteArrayOutputStream.toByteArray();

            //在文件系统中创建文件,并将上一步的byte[]写入
            SDFSFileChannel sdfsFileChannel = simpleDistributedFileSystem.create(remoteDir);
            if (sdfsFileChannel==null){
                return;
            }
            sdfsFileChannel.write(ByteBuffer.wrap(bytes));
            sdfsFileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void create(String fileUri){
        try {
            simpleDistributedFileSystem.create(fileUri);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void get(String remoteFile, String localDir) {
        // if the file is exist, note the operator
        File file = new File(localDir);
        try {
            if (!file.exists()) {
                file.createNewFile();
            } else {
                do {
                    System.out.println("The file is exist. Do you want to overwrite it?[Y/N]");
                    System.out.print(">>> ");
                    Scanner input = new Scanner(System.in);
                    String str = input.nextLine();
                    if (str.toLowerCase().equals("y")) {
                        break;
                    } else if (str.toLowerCase().equals("n")) {
                        return;
                    }
                } while (true);

            }
            SDFSFileChannel sdfsFileChannel = simpleDistributedFileSystem.openReadonly(remoteFile);

            /*
                打开文件失败,错误信息会在InputStream中提示,此处直接返回
             */
            if (sdfsFileChannel==null){
                return;
            }

            FileOutputStream fileOutputStream = new FileOutputStream(file);
            byte[] b = new byte[sdfsFileChannel.getFileSize()];
            System.out.println("fileSize: "+sdfsFileChannel.getFileSize());
            ByteBuffer bb = ByteBuffer.wrap(b);
            sdfsFileChannel.read(bb);
            fileOutputStream.write(bb.array());
            fileOutputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void openReadonly(String fileUri){
        try {
            simpleDistributedFileSystem.openReadonly(fileUri);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void openReadWrite(String fileUri){
        try{
            simpleDistributedFileSystem.openReadWrite(fileUri);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void printHelpInfo(){
        System.out.println("mkdir: 在NameNode创建一个目录");
        System.out.println("    如：mkdir home/test");
        System.out.println("put: 将本地文件放到服务器上");
        System.out.println("    如：put /Users/alex/Desktop/untitled2.html home/test/test2.html");
        System.out.println("get:  将服务器上的文件拿到本地");
        System.out.println("    如：get home/test/test2.html /Users/alex/Desktop/untitled3.html");
        System.out.println("printnodes:  打印namenode中的目录");
        System.out.println("    如：printnodes");
        System.out.println("openreadonly:  打开服务器上一个文件，只是检测会不会报错");
        System.out.println("    如：openreadonly home/test/test2.html");
        System.out.println("openreadwrite:  打开服务器上一个文件，只是检测会不会报错");
        System.out.println("    如：openreadonly home/test/test2.html");
        System.out.println("testorw 和 testcreate:  比较繁琐我觉得你们不会想试这个功能的，详细参见代码");

    }
}
