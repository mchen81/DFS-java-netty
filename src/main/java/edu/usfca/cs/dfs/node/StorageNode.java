package edu.usfca.cs.dfs.node;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.message.router.ClientMessageRouter;
import edu.usfca.cs.dfs.message.sender.NodeMessageSender;
import edu.usfca.cs.dfs.proto.Message;
import edu.usfca.cs.dfs.utils.ChecksumUtil;
import edu.usfca.cs.dfs.utils.Constant;
import edu.usfca.cs.dfs.utils.FileProcessUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class StorageNode {

    static Logger log = Logger.getLogger(StorageNode.class);

    private NodeMessageSender nodeMessageSender;
    private ClientMessageRouter clientMessageRouter;

    private NodeProcessRecorder recorder = new NodeProcessRecorder();

    private Map<ChunkInfo, Channel> repairingTable;
    private int openedPort;

    public StorageNode(String hostName, int hostPort, String nodeName, int nodePort) throws IOException {
        nodeMessageSender = new NodeMessageSender(hostName, hostPort, this);
        nodeMessageSender.sendRegistration(nodeName, nodePort);
        Thread heartbeatSender = new Thread(new HeartBeatSender(this));
        heartbeatSender.start();
        clientMessageRouter = new ClientMessageRouter(this, recorder);
        openedPort = nodePort;
        clientMessageRouter.start(nodePort);
        repairingTable = new HashMap<>();
    }

    public void sendHeartbeat() {
        File file = new File(Constant.FILEPATH_PREFIX);
        nodeMessageSender.sendHeartbeat(file.getFreeSpace(), recorder.getProcessedNumber());
    }

    public void deleteFile(String hashedFilepath) {
        recorder.addProcessedNumber(1);
        File file = new File(Constant.FILEPATH_PREFIX + hashedFilepath);
        if (file.exists() && file.isDirectory()) {
            try {
                FileUtils.deleteDirectory(file);
                log.info("[DELETE] " + hashedFilepath + " is deleted");
            } catch (Exception e) {
                log.info("[DELETE] IOE happens when deleting a directory");
            }
        }
    }

    public boolean storeChunk(Message.ChunkMetadata metadata, byte[] chunkData) {
        recorder.addProcessedNumber(1);
        String filePath = metadata.getFilePath();
        String expectedChecksum = metadata.getChecksum();
        String actualChecksum = ChecksumUtil.getMD5Checksum(chunkData);
        if (!expectedChecksum.equals(actualChecksum)) {
            log.info("expectedChecksum != actualChecksum");
            return false;
        }
        try {
            String hashedFilePath = FileProcessUtil.getHashedFilename(filePath);
            String newFilename = hashedFilePath + "/chunk" + metadata.getChunkId();
            // Note: no need to delete the file because it will be overwritten by FileUtils
            File file = new File(Constant.FILEPATH_PREFIX + newFilename);
            File metadataFile = new File(Constant.FILEPATH_PREFIX + newFilename + ".metadata");
            FileUtils.writeByteArrayToFile(file, chunkData);
            FileUtils.writeByteArrayToFile(metadataFile, metadata.toByteArray());
            nodeMessageSender.notifyFileStored(newFilename);
            log.info(newFilename + " has been stored.");
        } catch (IOException e) {
            log.info("[ERROR] Cannot store chuck");
            return false;
        }
        return true;
    }

    public boolean sendReplica(Message.ChunkMetadata metadata, ByteString chunkData, Message.Node2Node.DataType type) {
        recorder.addProcessedNumber(1);
        Message.ChunkPlacementDecision placementDecision = metadata.getPlacement();
        String targetHost = null;
        if (type == Message.Node2Node.DataType.REPLICA_1) {
            targetHost = placementDecision.getReplica1();
        } else if (type == Message.Node2Node.DataType.REPLICA_2) {
            targetHost = placementDecision.getReplica2();
        }
        if (targetHost == null) {
            return false;
        }
        Message.Node2Node.ChunkTransfer chunkTransfer = Message.Node2Node.ChunkTransfer.newBuilder()
                .setMetadata(metadata)
                .setChunkData(chunkData)
                .setDatatype(type)
                .build();
        Message.Node2Node node2Node = Message.Node2Node.newBuilder().setChunkTransfer(chunkTransfer).build();
        nodeMessageSender.sendReplica(targetHost, node2Node);
        log.info(String.format("[REPLICA] send %s/chunk%d (%s) to %s",
                metadata.getFilePath(),
                metadata.getChunkId(),
                type.name(),
                targetHost));
        return true;
    }

    public void sendRecovery(String filepath, String targetHost) { // filepath here would be hashed
        recorder.addProcessedNumber(1);
        filepath = Constant.FILEPATH_PREFIX + filepath;
        File chunkFile = new File(filepath);
        File metadataFile = new File(filepath + ".metadata");
        try {
            byte[] chunkData = FileUtils.readFileToByteArray(chunkFile);
            Message.ChunkMetadata metadata = Message.ChunkMetadata.parseFrom(FileUtils.readFileToByteArray(metadataFile));
            Message.Node2Node.ChunkTransfer chunkTransfer = Message.Node2Node.ChunkTransfer.newBuilder()
                    .setMetadata(metadata)
                    .setChunkData(ByteString.copyFrom(chunkData))
                    .setDatatype(Message.Node2Node.DataType.RECOVERY)
                    .build();
            Message.Node2Node node2Node = Message.Node2Node.newBuilder().setChunkTransfer(chunkTransfer).build();
            nodeMessageSender.sendReplica(targetHost, node2Node);
        } catch (IOException e) {
            log.info("[NODE] IOE happened when recovering chunk: " + filepath);
        }
    }

    public byte[] extractChunk(String chunkFilePath, int chunkId) {
        recorder.addProcessedNumber(1);
        chunkFilePath = Constant.FILEPATH_PREFIX + FileProcessUtil.getHashedFilename(chunkFilePath) + "/chunk" + chunkId;
        File file = new File(chunkFilePath);
        try {
            return FileUtils.readFileToByteArray(file);
        } catch (IOException e) {
            log.info("[EXTRACT] Cannot extract chunk " + chunkFilePath + chunkId);
            return null;
        }
    }

    public Message.ChunkMetadata extractChunkMetadata(String chunkFilePath, int chunkId) {
        recorder.addProcessedNumber(1);
        chunkFilePath = Constant.FILEPATH_PREFIX + FileProcessUtil.getHashedFilename(chunkFilePath) + "/chunk" + chunkId + ".metadata";
        File file = new File(chunkFilePath);
        try {
            return Message.ChunkMetadata.parseFrom(FileUtils.readFileToByteArray(file));
        } catch (IOException e) {
            log.info("[EXTRACT] Cannot extract metadata " + chunkFilePath + chunkId);
            return null;
        }
    }

    /**
     * This is for broken chunk to wait for repairing
     *
     * @param ctx     who asked to
     * @param request
     */
    public void askServerChunkPosition(ChannelHandlerContext ctx, Message.Client2Node.RetrieveChunkRequest request) {
        recorder.addProcessedNumber(1);
        ChunkInfo chunkInfo = new ChunkInfo(request.getFilePath(), request.getChunkId());
        repairingTable.put(chunkInfo, ctx.channel());
        nodeMessageSender.askReplicaPlaces(request);
        log.info("[REPAIRING] Ask controller the broken chunk's place");
    }

    public void repairChunk(Message.ChunkMetadata metadata, byte[] chunkData) {
        recorder.addProcessedNumber(1);
        log.info("[REPAIRING] chunk repairing...");
        ChunkInfo chunkInfo = new ChunkInfo(metadata.getFilePath(), metadata.getChunkId());
        if (!repairingTable.containsKey(chunkInfo)) {
            log.info("[STORAGE NODE] repairing table does not contain the chunk info");
            return;
        }
        if (!storeChunk(metadata, chunkData)) {
            log.info("[STORAGE NODE] fail to repairing chunk: " + metadata.getFilePath() + metadata.getChunkId());
        }
        Channel channel = repairingTable.remove(chunkInfo);
        if (channel != null) {
            log.info("[Good News!] The broken chunk has been repaired. Sending back to the client");
            Message.Node2Client.RetrieveChunkRespond respond = Message.Node2Client.RetrieveChunkRespond.newBuilder()
                    .setHasFile(true)
                    .setChunk(ByteString.copyFrom(chunkData))
                    .setMetadata(metadata)
                    .build();
            Message.Node2Client node2Client = Message.Node2Client.newBuilder()
                    .setRetrieveChunkRespond(respond)
                    .build();
            Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                    .setNode2Client(node2Client)
                    .build();
            channel.writeAndFlush(wrapper).syncUninterruptibly();
        }
        if (repairingTable.isEmpty()) {
            nodeMessageSender.closeAllWaitingChannels();
            log.info("[REPAIRED] All chunks has been repaired");
        }
    }

    public int getOpenedPort() {
        return openedPort;
    }

    private static class HeartBeatSender implements Runnable {
        private StorageNode storageNode;

        public HeartBeatSender(StorageNode storageNode) {
            this.storageNode = storageNode;
        }

        @Override
        public void run() {
            while (true) {
                storageNode.sendHeartbeat();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class ChunkInfo {
        private String filepath;
        private int chunkId;

        public ChunkInfo(String filepath, int chunkId) {
            this.filepath = filepath;
            this.chunkId = chunkId;
        }

        public String getFilepath() {
            return filepath;
        }

        public void setFilepath(String filepath) {
            this.filepath = filepath;
        }

        public int getChunkId() {
            return chunkId;
        }

        public void setChunkId(int chunkId) {
            this.chunkId = chunkId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ChunkInfo chunkInfo = (ChunkInfo) o;
            return chunkId == chunkInfo.chunkId &&
                    filepath.equals(chunkInfo.filepath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filepath, chunkId);
        }
    }
}
