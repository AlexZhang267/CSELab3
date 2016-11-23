package sdfs.namenode

import sdfs.datanode.DataNodeServer
import sdfs.exception.IllegalAccessTokenException
import sdfs.protocol.INameNodeProtocol
import spock.lang.Shared
import spock.lang.Specification

import java.rmi.registry.LocateRegistry

import static sdfs.Util.generateFilename
import static sdfs.Util.generatePort

class NameNodeServerTest extends Specification {
    @Shared
    INameNodeProtocol nameNodeServer

    def setupSpec() {
        System.setProperty("sdfs.namenode.dir", File.createTempDir().absolutePath);
        nameNodeServer = new NameNodeServer(NameNodeServer.FLUSH_DISK_INTERNAL_SECONDS, LocateRegistry.createRegistry(generatePort()))
    }

    def "CloseReadonlyFile"() {
        def filename = generateFilename()
        def accessToken = nameNodeServer.create(filename).accessToken
        def readonlyAccessToken = nameNodeServer.openReadonly(filename).accessToken

        when:
        nameNodeServer.closeReadonlyFile(UUID.randomUUID())

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.closeReadonlyFile(accessToken)

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.closeReadwriteFile(accessToken, 0)

        then:
        noExceptionThrown()

        when:
        nameNodeServer.closeReadonlyFile(accessToken)

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.closeReadonlyFile(readonlyAccessToken)

        then:
        noExceptionThrown()

        when:
        nameNodeServer.closeReadonlyFile(readonlyAccessToken)

        then:
        thrown(IllegalAccessTokenException)
    }

    def "CloseReadwriteFile"() {
        def filename = generateFilename()
        def accessToken = nameNodeServer.create(filename).accessToken
        def readonlyAccessToken = nameNodeServer.openReadonly(filename).accessToken

        when:
        nameNodeServer.closeReadwriteFile(UUID.randomUUID(), 0)

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.closeReadwriteFile(nameNodeServer.create(generateFilename()).accessToken, 1)

        then:
        thrown(IllegalArgumentException)

        when:
        nameNodeServer.closeReadwriteFile(nameNodeServer.create(generateFilename()).accessToken, -1)

        then:
        thrown(IllegalArgumentException)

        when:
        nameNodeServer.closeReadwriteFile(accessToken, 0)

        then:
        noExceptionThrown()

        when:
        nameNodeServer.closeReadwriteFile(accessToken, 0)

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.closeReadwriteFile(readonlyAccessToken, 0)

        then:
        thrown(IllegalAccessTokenException)

        when:
        accessToken = nameNodeServer.openReadwrite(filename).accessToken
        nameNodeServer.addBlocks(accessToken, 1)
        nameNodeServer.closeReadwriteFile(accessToken, 0)

        then:
        thrown(IllegalArgumentException)

        when:
        nameNodeServer.closeReadwriteFile(accessToken, 1)

        then:
        thrown(IllegalAccessTokenException)
//        noExceptionThrown()

        when:
        accessToken = nameNodeServer.openReadwrite(filename).accessToken
        nameNodeServer.closeReadwriteFile(accessToken, 0)

        then:
        noExceptionThrown()

        when:
        accessToken = nameNodeServer.openReadwrite(filename).accessToken
        nameNodeServer.addBlocks(accessToken, 1)
        nameNodeServer.removeLastBlocks(accessToken, 1)
        nameNodeServer.closeReadwriteFile(accessToken, 0)
        nameNodeServer.closeReadonlyFile(readonlyAccessToken)

        then:
        noExceptionThrown()
    }


    def "AddBlocks"() {
        def filename = generateFilename()
        def accessToken = nameNodeServer.create(filename).accessToken
        def readonlyAccessToken = nameNodeServer.openReadonly(filename).accessToken

        when:
        nameNodeServer.addBlocks(UUID.randomUUID(), 1)

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.addBlocks(readonlyAccessToken, 1)

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.addBlocks(accessToken, -1)

        then:
        thrown(IllegalArgumentException)

        when:
        nameNodeServer.addBlocks(accessToken, 1)
        nameNodeServer.closeReadwriteFile(accessToken, 1)

        then:
        noExceptionThrown()

        when:
        accessToken = nameNodeServer.openReadwrite(filename).accessToken
        nameNodeServer.addBlocks(accessToken, 1)
        nameNodeServer.closeReadwriteFile(accessToken, 1)

        then:
        thrown(IllegalArgumentException)

        when:
        accessToken = nameNodeServer.openReadwrite(filename).accessToken
        nameNodeServer.addBlocks(accessToken, 1)
        nameNodeServer.closeReadwriteFile(accessToken, DataNodeServer.BLOCK_SIZE * 2)
        nameNodeServer.closeReadonlyFile(readonlyAccessToken)

        then:
        noExceptionThrown()
    }

    def "RemoveLastBlocks"() {
        def filename = generateFilename()
        def accessToken = nameNodeServer.create(filename).accessToken
        def readonlyAccessToken = nameNodeServer.openReadonly(filename).accessToken

        when:
        nameNodeServer.removeLastBlocks(UUID.randomUUID(), 1)

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.removeLastBlocks(readonlyAccessToken, 1)

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.removeLastBlocks(accessToken, 1)

        then:
        thrown(IndexOutOfBoundsException)

        when:
        nameNodeServer.addBlocks(accessToken, 1)
        nameNodeServer.removeLastBlocks(accessToken, -1)

        then:
        thrown(IllegalArgumentException)

        when:
        nameNodeServer.removeLastBlocks(accessToken, 1)
        nameNodeServer.closeReadwriteFile(accessToken, 0)
        nameNodeServer.closeReadonlyFile(readonlyAccessToken)

        then:
        noExceptionThrown()
    }

    def "newCopyOnWriteBlock"() {
        def filename = generateFilename()
        def accessToken = nameNodeServer.create(filename).accessToken
        nameNodeServer.addBlocks(accessToken, 1)
        def readonlyAccessToken = nameNodeServer.openReadonly(filename).accessToken

        when:
        nameNodeServer.newCopyOnWriteBlock(UUID.randomUUID(), 0)

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.newCopyOnWriteBlock(readonlyAccessToken, 0)

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.newCopyOnWriteBlock(accessToken, 1)

        then:
        thrown(IndexOutOfBoundsException)

        when:
        nameNodeServer.newCopyOnWriteBlock(accessToken, -1)

        then:
        thrown(IndexOutOfBoundsException)

        when:
        nameNodeServer.newCopyOnWriteBlock(accessToken, 0)
        nameNodeServer.closeReadwriteFile(accessToken, 1)
        nameNodeServer.closeReadonlyFile(readonlyAccessToken)

        then:
        noExceptionThrown()
    }
}
