package edu.usfca.cs.dfs.message.sender;

import edu.usfca.cs.dfs.node.StorageNode;
import edu.usfca.cs.dfs.proto.Message;
import edu.usfca.cs.dfs.proto.Message.Server2Node.ReplicaPossiblePosition;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

@ChannelHandler.Sharable
public class NodeMessageSender extends MessageSender {

    private Channel serverChannel;
    private StorageNode storageNode;

    private List<Channel> waitingResponseChannel;

    static Logger log = Logger.getLogger(NodeMessageSender.class);

    public NodeMessageSender(String hostName, int hostPort, StorageNode storageNode) {
        serverChannel = super.connectTo(hostName, hostPort);
        this.storageNode = storageNode;
        waitingResponseChannel = new ArrayList<>();
    }

    public void closeAllWaitingChannels() {
        for (Channel channel : waitingResponseChannel) {
            channel.close();
        }
        waitingResponseChannel = new ArrayList<>();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message.DFSMessagesWrapper dfs) {
        if (dfs.hasServer2Node()) {
            Message.Server2Node server2Node = dfs.getServer2Node();
            if (server2Node.hasReplicaPossiblePosition()) {
                ReplicaPossiblePosition replicaPossiblePosition = server2Node.getReplicaPossiblePosition();
                requestReplica(replicaPossiblePosition);
            } else if (server2Node.hasReplicaRecovery()) {
                Message.Server2Node.ReplicaRecovery recovery = server2Node.getReplicaRecovery();
                storageNode.sendRecovery(recovery.getFilepath(), recovery.getDestination());
            } else if (server2Node.hasFileDeletion()) {
                Message.Server2Node.FileDeletion fileDeletion = server2Node.getFileDeletion();
                storageNode.deleteFile(fileDeletion.getFilepath());
            }
        } else if (dfs.hasNode2Node()) {
            Message.Node2Node node2Node = dfs.getNode2Node();
            Message.Node2Node.ChunkTransfer chunkTransfer = node2Node.getChunkTransfer();
            if (chunkTransfer.getDatatype() == Message.Node2Node.DataType.REPAIR) {
                storageNode.repairChunk(chunkTransfer.getMetadata(), chunkTransfer.getChunkData().toByteArray());
            }
        }
    }

    /**
     * Target: server
     * Send a registration request to server to join the cluster
     *
     * @param nodeName node's name(It's nickname of the host(like node1, node2...), not node's address)
     * @param nodePort node's port
     */
    public void sendRegistration(String nodeName, int nodePort) {
        log.info(nodeName + " Register Send");
        Message.Node2Server.Registration registration = Message.Node2Server.Registration.newBuilder()
                .setHostName(nodeName)
                .setHostPort(nodePort)
                .build();
        Message.Node2Server reg = Message.Node2Server.newBuilder()
                .setRegistration(registration)
                .build();
        Message.DFSMessagesWrapper msgWrapper = Message.DFSMessagesWrapper.newBuilder()
                .setNode2Server(reg)
                .build();
        ChannelFuture write = serverChannel.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
    }

    /**
     * Target: server
     *
     * @param freeSpace       the free space that current node is having
     * @param processedNumber how many request processed so far
     */
    public void sendHeartbeat(long freeSpace, long processedNumber) {
        Message.Node2Server.Heartbeat heartbeat = Message.Node2Server.Heartbeat.newBuilder()
                .setFreeSpace(freeSpace)
                .setProcessedNumber(processedNumber)
                .build();
        Message.Node2Server reg = Message.Node2Server.newBuilder()
                .setHeartbeat(heartbeat)
                .build();
        Message.DFSMessagesWrapper msgWrapper = Message.DFSMessagesWrapper.newBuilder()
                .setNode2Server(reg)
                .build();
        ChannelFuture write = serverChannel.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
    }

    /**
     * Target: other nodes
     *
     * @param targetHost target's host, include hostname and port
     * @param node2Node  prepared replica
     */
    public void sendReplica(String targetHost, Message.Node2Node node2Node) {
        Channel targetChannel = connectTo(targetHost);
        if (targetChannel == null) {
            throw new RuntimeException("Cannot resolve host: " + targetHost);
        }
        Message.DFSMessagesWrapper msgWrapper = Message.DFSMessagesWrapper.newBuilder()
                .setNode2Node(node2Node)
                .build();
        ChannelFuture write = targetChannel.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
        targetChannel.close();
    }

    /**
     * Target: server
     * To notify server @filename has been stored
     *
     * @param filename the file's name in node
     */
    public void notifyFileStored(String filename) {
        Message.Node2Server.Notification notification = Message.Node2Server.Notification.newBuilder()
                .setText(filename)
                .setType(Message.Node2Server.Notification.NotificationType.FILE_STORED)
                .build();
        Message.Node2Server node2Server = Message.Node2Server.newBuilder()
                .setNotification(notification)
                .build();
        Message.DFSMessagesWrapper msgWrapper = Message.DFSMessagesWrapper.newBuilder()
                .setNode2Server(node2Server)
                .build();
        ChannelFuture write = serverChannel.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
    }

    /**
     * When a chunk is broken, this method would be called to ask the chunk's possible position
     *
     * @param request
     */
    public void askReplicaPlaces(Message.Client2Node.RetrieveChunkRequest request) {
        Message.Node2Server.RetrieveChunkRequest retrieveChunkRequest = Message.Node2Server.RetrieveChunkRequest.newBuilder()
                .setChunkId(request.getChunkId())
                .setFilePath(request.getFilePath())
                .build();
        Message.Node2Server node2Server = Message.Node2Server.newBuilder()
                .setRetrieveChunkRequest(retrieveChunkRequest)
                .build();
        Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                .setNode2Server(node2Server)
                .build();
        ChannelFuture write = serverChannel.writeAndFlush(wrapper);
        write.syncUninterruptibly();
    }

    public void requestReplica(ReplicaPossiblePosition replicaPossiblePositions) {
        log.info("[REPAIRING] chunk's position received, requesting...");
        List<String> hosts = new ArrayList<>(replicaPossiblePositions.getHostList());
        if (hosts.isEmpty()) {
            log.info("[REPAIRING] Cannot repair the chunk because no possible position");
        } else {
            String selfIp = ((InetSocketAddress) serverChannel.localAddress()).getAddress().getHostAddress();
            String selfHost = selfIp + ":" + storageNode.getOpenedPort();
            if (!hosts.remove(selfHost)) {
                log.info("[REPAIRING] Did not remove self host, could have issues here: " + selfHost);
            }
            for (String host : hosts) {
                Channel channel = connectTo(host);
                if (channel != null) {
                    log.info("[REPAIRING] Requesting to " + host);
                    Message.Node2Node.ChunkRequest chunkRequest = Message.Node2Node.ChunkRequest.newBuilder()
                            .setChunkId(replicaPossiblePositions.getChunkId())
                            .setFilepath(replicaPossiblePositions.getFilePath())
                            .build();
                    Message.Node2Node node2Node = Message.Node2Node.newBuilder()
                            .setChunkRequest(chunkRequest)
                            .build();
                    Message.DFSMessagesWrapper wrp = Message.DFSMessagesWrapper.newBuilder()
                            .setNode2Node(node2Node)
                            .build();
                    waitingResponseChannel.add(channel);
                    channel.writeAndFlush(wrp).syncUninterruptibly();
                }
            }
        }
    }
}
