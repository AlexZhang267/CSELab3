package sdfs

import org.apache.commons.lang3.RandomStringUtils
import sdfs.datanode.DataNodeServer

class Util {
    static def random = new Random()

    static int generatePort() {
        random.nextInt(1000) + 34000
    }

    static String generateFilename() {
        //todo: 原来是255
        RandomStringUtils.random(1).replace('/', ':')
    }

    static int getBlockAmount(int fileSize) {
        fileSize == 0 ? 0 : (((fileSize - 1) / DataNodeServer.BLOCK_SIZE) + 1) as int
    }
}
