package edu.usfca.cs.dfs.message.router;

import edu.usfca.cs.dfs.node.StorageNode;
import edu.usfca.cs.dfs.proto.Message;
import edu.usfca.cs.dfs.server.NodeManager;
import edu.usfca.cs.dfs.utils.ChannelHandlerContextUtils;
import edu.usfca.cs.dfs.utils.FileProcessUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

import java.util.List;

@ChannelHandler.Sharable
public class NodeMessageRouter extends DFSMessageRouter {
    private NodeManager nodeManager;
    private StorageNode storageNode;
    final static Logger log = Logger.getLogger(NodeMessageRouter.class);

    /**
     * For node manager routing messages from other nodes
     *
     * @param nodeManager
     */
    public NodeMessageRouter(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message.DFSMessagesWrapper dfsMessagesWrapper) throws Exception {
        if (nodeManager != null && dfsMessagesWrapper.hasNode2Server()) {
            node2ServerProcess(ctx, dfsMessagesWrapper.getNode2Server());
        }
    }

    /**
     * Process 3 different messages from node
     * 1. Registration: register the node to cluster
     * 2. Heartbeat: update node's status(free space and processed request number)
     * 3. Notification: usually get this when a node stored a file, server may put bloom filter
     *
     * @param ctx
     * @param nodeMessage
     */
    private void node2ServerProcess(ChannelHandlerContext ctx, Message.Node2Server nodeMessage) {
        if (nodeMessage.hasRegistration()) {
            log.info("[Server] register receive");
            Message.Node2Server.Registration reg = nodeMessage.getRegistration();
            nodeManager.registerNode(reg, ctx);
        } else if (nodeMessage.hasHeartbeat()) {
            Message.Node2Server.Heartbeat beat = nodeMessage.getHeartbeat();
            String host = ChannelHandlerContextUtils.getRemoteAddressWithPort(ctx.channel());
            if (!nodeManager.updateNode(host, beat.getFreeSpace(), beat.getProcessedNumber())) {
                log.error("[Node Message Router] Fail to update on " + host + " because connection may be lost.\n");
            }
        } else if (nodeMessage.hasNotification()) {
            Message.Node2Server.Notification notification = nodeMessage.getNotification();
            if (notification.getType() == Message.Node2Server.Notification.NotificationType.FILE_STORED) {
                String filePath = notification.getText();
                String address = ChannelHandlerContextUtils.getRemoteAddressWithPort(ctx.channel());
                nodeManager.putBloomFilter(address, filePath);
                nodeManager.putNodeStoredFiles(address, filePath);
            }
        } else if (nodeMessage.hasRetrieveChunkRequest()) {
            handleChunkRepairRequest(ctx, nodeMessage.getRetrieveChunkRequest());
        }
    }

    /**
     * When a node needs to repair its broken chunk, the request will be handled here
     *
     * @param ctx
     * @param request
     */
    private void handleChunkRepairRequest(ChannelHandlerContext ctx, Message.Node2Server.RetrieveChunkRequest request) {
        String filepath = request.getFilePath();
        int chunkId = request.getChunkId();
        String hashedFilepath = FileProcessUtil.getHashedFilename(filepath);
        List<String> possiblePosition = nodeManager.getFilePossiblePosition(hashedFilepath + "/chunk" + chunkId);
        Message.Server2Node.ReplicaPossiblePosition replicaPossiblePosition = Message.Server2Node.ReplicaPossiblePosition.newBuilder()
                .setFilePath(filepath)
                .setChunkId(chunkId)
                .addAllHost(possiblePosition)
                .build();
        Message.Server2Node server2Node = Message.Server2Node.newBuilder()
                .setReplicaPossiblePosition(replicaPossiblePosition)
                .build();
        Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                .setServer2Node(server2Node)
                .build();
        ChannelFuture write = ctx.channel().writeAndFlush(wrapper);
        write.syncUninterruptibly();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // super.channelInactive(ctx);
        String host = ChannelHandlerContextUtils.getRemoteAddressWithPort(ctx.channel());
        if (nodeManager.removeNode(host)) {
            log.info(String.format("[Node Manager] Node %s has been removed.\n", host));
        } else {
            log.info(String.format("[Node Manager] Cannot find the node %s.\n", host));
        }
    }
}
