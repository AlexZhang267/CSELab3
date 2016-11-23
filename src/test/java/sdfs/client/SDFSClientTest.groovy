package sdfs.client

import sdfs.datanode.DataNodeServer
import sdfs.namenode.NameNodeServer
import sdfs.protocol.IDataNodeProtocol
import sdfs.protocol.INameNodeDataNodeProtocol
import sdfs.protocol.INameNodeProtocol
import spock.lang.Shared
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.OverlappingFileLockException
import java.rmi.registry.LocateRegistry
import java.rmi.registry.Registry
import java.rmi.server.UnicastRemoteObject

import static sdfs.Util.*

class SDFSClientTest extends Specification {
    @Shared
    Registry registry
    @Shared
    INameNodeProtocol nameNodeServer
    @Shared
    IDataNodeProtocol dataNodeServer
    @Shared
    SDFSClient client
    @Shared
    def FILE_SIZE = 2 * DataNodeServer.BLOCK_SIZE + 2
    @Shared
    def dataBuffer = ByteBuffer.allocate(FILE_SIZE)
    @Shared
    def buffer = ByteBuffer.allocate(FILE_SIZE)
    def parentDir = generateFilename()
    def filename = parentDir + "/" + generateFilename()

    def setupSpec() {
        System.setProperty("sdfs.namenode.dir", File.createTempDir().absolutePath);
        System.setProperty("sdfs.datanode.dir", File.createTempDir().absolutePath);
        registry = LocateRegistry.createRegistry(generatePort())
        nameNodeServer = new NameNodeServer(NameNodeServer.FLUSH_DISK_INTERNAL_SECONDS, registry)
//        def nameNodeRemote = UnicastRemoteObject.exportObject(nameNodeServer, 0)
        dataNodeServer = new DataNodeServer(nameNodeServer as INameNodeDataNodeProtocol)
        registry.bind("DataNodeServer",dataNodeServer)
        client = new SDFSClient(SDFSClient.FILE_DATA_BLOCK_CACHE_SIZE, registry)
        for (int i = 0; i < FILE_SIZE; i++)
            dataBuffer.put(i.byteValue())
    }

    def cleanupSpec() {
//        registry.unbind(DataNodeServer.class.name)
        UnicastRemoteObject.unexportObject(nameNodeServer, false)
        UnicastRemoteObject.unexportObject(dataNodeServer, false)
    }

    def setup() {
        client.mkdir(parentDir)
    }

    private def writeData() {
        def fc = client.create(filename)
        dataBuffer.position(0)
        fc.write(dataBuffer)
        fc.close()
    }

    def "Test multi client basic"() {
        def fileSize = FILE_SIZE

        when:
        def fc = client.create("$filename")
        dataBuffer.position(0)

        then:
        fc.write(dataBuffer) == fileSize
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == fileSize
        fc.write(dataBuffer) == 0

        when:
        client.openReadWrite(filename)

        then:
        thrown(OverlappingFileLockException)

        when:
        def readonlyFileChannel = client.openReadonly(filename)

        then:
        readonlyFileChannel.size() == 0
        readonlyFileChannel.fileNode.blockAmount == getBlockAmount(0)
        readonlyFileChannel.position() == 0

        when:
        fc.close()

        then:
        readonlyFileChannel.size() == 0
        readonlyFileChannel.fileNode.blockAmount == getBlockAmount(0)
        readonlyFileChannel.position() == 0

        when:
        fc = client.openReadonly(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer

        when:
        fc.close()

        then:
        noExceptionThrown()

        when:
        fc = client.openReadWrite(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer

        when:
        readonlyFileChannel.close()

        then:
        noExceptionThrown()
    }

    def "Test truncate"() {
        def fileSize = FILE_SIZE
        writeData()

        when:
        def fc = client.openReadWrite(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer

        when:
        fc.truncate(fileSize + 1)

        then:
        fc.position() == fileSize
        fc.size() == fileSize

        when:
        fc.truncate(0)
        buffer.position(0)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.position() == 0
        fc.read(buffer) == 0

        when:
        def readonlyFileChannel = client.openReadonly(filename)

        then:
        readonlyFileChannel.size() == fileSize
        readonlyFileChannel.fileNode.blockAmount == getBlockAmount(fileSize)
        readonlyFileChannel.position() == 0

        when:
        fc.close()

        then:
        readonlyFileChannel.size() == fileSize
        readonlyFileChannel.fileNode.blockAmount == getBlockAmount(fileSize)
        readonlyFileChannel.position() == 0

        when:
        fc = client.openReadWrite(filename)
        dataBuffer.position(0)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.position() == 0
        fc.read(buffer) == 0
        fc.write(dataBuffer) == fileSize

        when:
        fc.close()
        readonlyFileChannel.close()

        then:
        noExceptionThrown()
    }

    def "Test append data"() {
        def fileSize = FILE_SIZE
        def secondPosition = 3 * DataNodeServer.BLOCK_SIZE - 1
        writeData()
        //0 ~ filesize-1:[0:file_size-1]
        when:
        def fc = client.openReadWrite(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer

        when:
        fc.position(secondPosition)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == secondPosition
        fc.read(buffer) == 0
        fc.write(dataBuffer) == 0

        when:
        dataBuffer.position(0)

        then:
        fc.write(dataBuffer) == fileSize
        //secondPosition: 0 ~ filesize-1
        fc.size() == secondPosition + fileSize
        fc.fileNode.blockAmount == getBlockAmount(secondPosition + fileSize)
        fc.position() == secondPosition + fileSize
        fc.read(buffer) == 0

        when:
        fc.position(0)

        then:
        fc.read(buffer) == fileSize
        fc.position() == fileSize
        fc.size() == secondPosition + fileSize
        buffer == dataBuffer
        fc.read(buffer) == 0
        fc.size() == secondPosition + fileSize
        fc.fileNode.blockAmount == getBlockAmount(secondPosition + fileSize)
        fc.position() == fileSize

        when:
        fc.truncate(secondPosition + fileSize + 1)

        then:
        fc.size() == secondPosition + fileSize
        fc.fileNode.blockAmount == getBlockAmount(secondPosition + fileSize)
        fc.position() == fileSize

        when:
        buffer.position(0)

        then:
        fc.read(buffer) == fileSize
        fc.position() == 2*fileSize// added by me

        when:
        buffer.position(0)

        then:
        for (int i = 0; i < DataNodeServer.BLOCK_SIZE - 3; i++)
            buffer.get() == 0.byteValue()
        buffer.get() == 0.byteValue()//3 * DataNodeServer.BLOCK_SIZE - 2
        buffer.get() == 1.byteValue()//3 * DataNodeServer.BLOCK_SIZE - 1
        buffer.get() == 2.byteValue()
        buffer.get() == 3.byteValue()
        buffer.get() == 4.byteValue()

        when:
        fc.truncate(fileSize)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == fileSize

        when:
        fc.close()
        fc = client.openReadWrite(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer

        when:
        fc.close()

        then:
        noExceptionThrown()
    }
}