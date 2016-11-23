package sdfs.client

import sdfs.datanode.DataNodeServer
import sdfs.exception.SDFSFileAlreadyExistException
import sdfs.namenode.NameNodeServer
import sdfs.protocol.IDataNodeProtocol
import sdfs.protocol.INameNodeDataNodeProtocol
import sdfs.protocol.INameNodeProtocol
import spock.lang.Shared
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonWritableChannelException
import java.rmi.registry.LocateRegistry
import java.rmi.registry.Registry
import java.rmi.server.UnicastRemoteObject

import static sdfs.Util.generateFilename
import static sdfs.Util.generatePort

class SDFSClientBasicTest extends Specification {
    @Shared
    Registry registry
    @Shared
    INameNodeProtocol nameNodeServer
    @Shared
    IDataNodeProtocol dataNodeServer
    @Shared
    SDFSClient client

    def setupSpec() {
        System.setProperty("sdfs.namenode.dir", File.createTempDir().absolutePath);
        System.setProperty("sdfs.datanode.dir", File.createTempDir().absolutePath);
        registry = LocateRegistry.createRegistry(generatePort())
        nameNodeServer = new NameNodeServer(NameNodeServer.FLUSH_DISK_INTERNAL_SECONDS, registry)
//        def nameNodeRemote = UnicastRemoteObject.exportObject(nameNodeServer, 0)
        dataNodeServer = new DataNodeServer(nameNodeServer as INameNodeDataNodeProtocol)
        registry.bind(DataNodeServer.class.name,dataNodeServer)
        client = new SDFSClient(SDFSClient.FILE_DATA_BLOCK_CACHE_SIZE, registry)
//        client = new SDFSClient()
    }

    def cleanupSpec() {
//        registry.unbind(DataNodeServer.class.name)
//        registry.unbind(DataNodeServer.class.name)
//        registry.unbind("NameNodeServer")

        UnicastRemoteObject.unexportObject(nameNodeServer, false)
        UnicastRemoteObject.unexportObject(dataNodeServer, false)
    }

    def "Test file tree"() {
        def parentDir = generateFilename()
        client.mkdir(parentDir)
        //todo：原来是255
        for (int i = 0; i < 3; i++)
            client.mkdir(parentDir += "/" + generateFilename())
        def dirName = generateFilename()
        def filename = generateFilename()
        client.mkdir("$parentDir/$dirName")
        client.create("$parentDir/$filename").close()

        when:
        client.mkdir("$parentDir/$dirName")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        client.mkdir("$parentDir/$filename")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        client.create("$parentDir/$dirName")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        client.create("$parentDir/$filename")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        client.openReadonly("$parentDir/${generateFilename()}")

        then:
        thrown(FileNotFoundException)

        when:
        client.openReadWrite("$parentDir/${generateFilename()}")

        then:
        thrown(FileNotFoundException)

        when:
        client.openReadonly("${generateFilename()}/$filename")

        then:
        thrown(FileNotFoundException)

        when:
        client.openReadWrite("${generateFilename()}/$filename")

        then:
        thrown(FileNotFoundException)

        when:
        client.create("${generateFilename()}/$filename")

        then:
        thrown(FileNotFoundException)
    }

    def "Test create empty"() {
        def parentDir = generateFilename()
        client.mkdir(parentDir)
        def filename = parentDir + "/" + generateFilename()

        when:
        def fc = client.create(filename)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.isOpen()
        fc.position() == 0
        fc.read(ByteBuffer.allocate(1)) == 0

        when:
        fc.position(-1)

        then:
        thrown(IllegalArgumentException)

        when:
        fc.position(1)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.isOpen()
        fc.position() == 1
        fc.read(ByteBuffer.allocate(1)) == 0

        when:
        fc.close()

        then:
        !fc.isOpen()

        when:
        fc.position()

        then:
        thrown(ClosedChannelException)

        when:
        fc.position(0)

        then:
        thrown(ClosedChannelException)

        when:
        fc.read(ByteBuffer.allocate(1))

        then:
        thrown(ClosedChannelException)

        when:
        fc.write(ByteBuffer.allocate(1))

        then:
        thrown(ClosedChannelException)

        when:
        fc.size()

        then:
        thrown(ClosedChannelException)

        when:
        fc.flush()

        then:
        thrown(ClosedChannelException)

        when:
        fc.truncate(0)

        then:
        thrown(ClosedChannelException)

        when:
        fc.close()

        then:
        noExceptionThrown()

        when:
        fc = client.openReadonly(filename)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.isOpen()
        fc.position() == 0

        when:
        fc.write(ByteBuffer.allocate(1))

        then:
        thrown(NonWritableChannelException)

        when:
        fc.truncate(0)

        then:
        thrown(NonWritableChannelException)

        when:
        fc.close()

        then:
        !fc.isOpen()

        when:
        fc.close()

        then:
        noExceptionThrown()
    }
}