package edu.usfca.cs.dfs.message.router;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.node.NodeProcessRecorder;
import edu.usfca.cs.dfs.node.StorageNode;
import edu.usfca.cs.dfs.proto.Message;
import edu.usfca.cs.dfs.proto.Message.Server2Client.RetrieveFileResponse.ChunkPossiblePosition;
import edu.usfca.cs.dfs.server.NodeManager;
import edu.usfca.cs.dfs.utils.ChecksumUtil;
import edu.usfca.cs.dfs.utils.Constant;
import edu.usfca.cs.dfs.utils.FileProcessUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@ChannelHandler.Sharable
public class ClientMessageRouter extends DFSMessageRouter {

    static Logger log = Logger.getLogger(ClientMessageRouter.class);

    private NodeManager nodeManager;

    private NodeProcessRecorder nodeProcessRecorder;
    private StorageNode storageNode;

    /**
     * for node manager handling messages from clients
     *
     * @param nodeManager
     */
    public ClientMessageRouter(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    /**
     * for storage node handling messages from client, usually chunk store request
     *
     * @param storageNode
     * @param nodeProcessRecorder
     */
    public ClientMessageRouter(StorageNode storageNode, NodeProcessRecorder nodeProcessRecorder) {
        this.nodeProcessRecorder = nodeProcessRecorder;
        this.storageNode = storageNode;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message.DFSMessagesWrapper dfsMessagesWrapper) {
        if (nodeManager != null && dfsMessagesWrapper.hasClient2Server()) {
            Message.Client2Server clientMessage = dfsMessagesWrapper.getClient2Server();
            if (clientMessage.hasStoreFileRequest()) {
                handleStoreFileRequest(ctx, clientMessage.getStoreFileRequest());
            } else if (clientMessage.hasCommand()) {
                handleClientCommand(ctx, clientMessage.getCommand());
            } else if (clientMessage.hasRetrieveFileRequest()) {
                handleFileRetrieveRequest(ctx, clientMessage.getRetrieveFileRequest());
            }
        } else if (storageNode != null && dfsMessagesWrapper.hasClient2Node()) {
            Message.Client2Node clientMessage = dfsMessagesWrapper.getClient2Node();
            if (clientMessage.hasStoreChunkRequest()) {
                handleStoreChunkRequest(ctx, clientMessage.getStoreChunkRequest());
            } else if (clientMessage.hasRetrieveChunkRequest()) {
                Message.Client2Node.RetrieveChunkRequest retrieveChunkRequest = clientMessage.getRetrieveChunkRequest();
                if (retrieveChunkRequest.getChunkMetadataOnly()) {
                    handleRetrieveMetadataRequest(ctx, retrieveChunkRequest);
                } else handleRetrieveChunkRequest(ctx, retrieveChunkRequest);
            } else if (clientMessage.getRetrieveStatistic()) {
                sendBackStatistic(ctx);
            }
        } else if (storageNode != null && dfsMessagesWrapper.hasNode2Node()) {
            node2NodeProcess(ctx, dfsMessagesWrapper.getNode2Node());
        }
    }

    /**
     * When node gets a chunk store request from client, it will store the file into it local.
     * If the file is stored successfully, this function will send replica to decided node
     * If it fails storing the file, it might send back fail reason to client
     *
     * @param ctx
     * @param clientRequest
     */
    private void handleStoreChunkRequest(ChannelHandlerContext ctx, Message.Client2Node.StoreChunkRequest clientRequest) {
        boolean isSuccessful = storageNode.storeChunk(clientRequest.getMetadata(), clientRequest.getChunkBytes().toByteArray());
        if (isSuccessful) {
            storageNode.sendReplica(clientRequest.getMetadata(), clientRequest.getChunkBytes(), Message.Node2Node.DataType.REPLICA_1);
        }
    }

    /**
     * server respond to client
     *
     * @param ctx
     * @param serverResponse
     */
    private void serverRespondToClient(ChannelHandlerContext ctx, Message.Server2Client serverResponse) {
        Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                .setServer2Client(serverResponse)
                .build();
        ChannelFuture write = ctx.channel().writeAndFlush(wrapper);
        write.syncUninterruptibly();
    }

    /**
     * When client send a request to store file, the request will be handled here first to see if the cluster is able to take the file
     * If yes, node manager(controller) will save the metadata in local disk
     * If not, the file will not be stored and response fail
     *
     * @param ctx
     * @param storeFileRequest
     */
    private void handleStoreFileRequest(ChannelHandlerContext ctx, Message.Client2Server.StoreFileRequest storeFileRequest) {
        // resolve metadata
        Message.FileMetadata metadata = storeFileRequest.getMetadata();
        String filePath = metadata.getFilePath();
        log.info("[CMR] received fill path: " + filePath);
        int chunkNumber = metadata.getChunkNumber();
        List<Message.ChunkPlacementDecision> placementDecisionList = nodeManager.decideChunkPlacement(chunkNumber);
        if (placementDecisionList == null) {
            Message.Server2Client.StoreFileResponse response = Message.Server2Client.StoreFileResponse.newBuilder()
                    .setSuccess(false)
                    .setFilePath(filePath)
                    .setFailReason("Cluster's space are not enough to store the file")
                    .build();
            Message.Server2Client server2Client = Message.Server2Client.newBuilder()
                    .setStoreFileResponse(response).build();
            serverRespondToClient(ctx, server2Client);
        } else {
            Message.Server2Client.StoreFileResponse response = Message.Server2Client.StoreFileResponse.newBuilder()
                    .setSuccess(true)
                    .addAllChunkPlacement(placementDecisionList)
                    .setFilePath(filePath)
                    .build();
            Message.Server2Client server2Client = Message.Server2Client.newBuilder()
                    .setStoreFileResponse(response).build();
            serverRespondToClient(ctx, server2Client);
        }
        String hashedFilePath = FileProcessUtil.getHashedFilename(filePath);
        File metadataFile = new File(Constant.FILEPATH_PREFIX + hashedFilePath + ".metadata");
        try {
            if (metadataFile.exists()) {
                nodeManager.deleteFile(hashedFilePath);
            }
            FileUtils.writeByteArrayToFile(metadataFile, metadata.toByteArray());
            nodeManager.addFilePathTiFileSystemTree(filePath);
        } catch (IOException e) {
            log.info("[ClientMessageRouter] IOE happened when writing metadata");
        }
    }

    private void handleClientCommand(ChannelHandlerContext ctx, Message.Client2Server.Command command) {
        Message.Server2Client.CommandResponse commandResponse = Message.Server2Client.CommandResponse.getDefaultInstance();
        if (command.getCommand().equals("-nodes")) {
            String nodesInfo = nodeManager.lookUpActiveNode();
            Message.Server2Client.CommandResponse.ActiveNodes activeNodes = Message.Server2Client.CommandResponse.ActiveNodes.newBuilder()
                    .setNodesInfo(nodesInfo)
                    .build();
            commandResponse = Message.Server2Client.CommandResponse.newBuilder()
                    .setActiveNodes(activeNodes)
                    .build();
        } else if (command.getCommand().equals("-ls")) {
            String path = command.getText();
            log.info("[Server receive ] -ls " + path);
            String dirs = nodeManager.getDirectories(path);
            Message.Server2Client.CommandResponse.ListDirectories listDirectories = Message.Server2Client.CommandResponse.ListDirectories.newBuilder()
                    .setDirectories(dirs)
                    .build();
            commandResponse = Message.Server2Client.CommandResponse.newBuilder()
                    .setListDirectories(listDirectories)
                    .build();
        } else if (command.getCommand().equals("-lsPosix")) {
            String path = command.getText();
            log.info("[Server receive ] -lsPosix " + path);
            Message.Server2Client.CommandResponse.PosixLs posixLs = Message.Server2Client.CommandResponse.PosixLs.newBuilder()
                    .addAllFileNames(nodeManager.getFilesUnder(path))
                    .build();
            commandResponse = Message.Server2Client.CommandResponse.newBuilder()
                    .setPosixLs(posixLs)
                    .build();
        }
        Message.Server2Client server2Client = Message.Server2Client.newBuilder()
                .setCommandResponse(commandResponse)
                .build();
        Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                .setServer2Client(server2Client)
                .build();
        ctx.channel().writeAndFlush(wrapper).syncUninterruptibly();
    }

    /**
     * When client send a retrieve file request, the request will be handled here first to see if the cluster is having the file
     * If yes, it will reply the file's metadata, and bloom filter result of each chunk in nodes
     *
     * @param ctx
     * @param request
     */
    private void handleFileRetrieveRequest(ChannelHandlerContext ctx, Message.Client2Server.RetrieveFileRequest request) {
        boolean isFirstChunk = request.getIsFirstChunk();
        String filePath = request.getFilePath();
        String hashedFilepath = FileProcessUtil.getHashedFilename(filePath);
        log.info("[File Retrieve Request Receive] " + filePath);
        Message.Server2Client.RetrieveFileResponse response;

        int chunkNumber = 0;
        if (isFirstChunk) chunkNumber = 1;
        else chunkNumber = request.getChunkNumber();
        List<ChunkPossiblePosition> chunkPossiblePositions = new ArrayList<>();
        for (int id = 1; id <= chunkNumber; id++) {
            String chunkFilename = hashedFilepath + "/chunk" + id;
            List<String> possiblePosition = nodeManager.getFilePossiblePosition(chunkFilename);
            ChunkPossiblePosition cpp = ChunkPossiblePosition.newBuilder()
                    .setChunkId(id)
                    .addAllHost(possiblePosition)
                    .build();
            chunkPossiblePositions.add(cpp);
        }

        boolean fileFound = true;
        if(isFirstChunk){
            ChunkPossiblePosition cpp = chunkPossiblePositions.get(0);
            List<String> possiblePosition = cpp.getHostList();

            if(possiblePosition.size() == 0) fileFound = false;
        }

        Message.FileMetadata fileMetadata = Message.FileMetadata.newBuilder()
                .setChunkNumber(chunkNumber)
                .setFilePath(filePath)
                .build();
        response = Message.Server2Client.RetrieveFileResponse.newBuilder()
                .setSuccess(true)
                .setFilePath(filePath)
                .setIsFirstChunkResponse(isFirstChunk)
                .setMetadata(fileMetadata)
                .addAllChunkPossiblePosition(chunkPossiblePositions)
                .build();
        Message.Server2Client server2Client = Message.Server2Client.newBuilder()
                .setRetrieveFileResponse(response)
                .build();
        Message.DFSMessagesWrapper dfsMessagesWrapper = Message.DFSMessagesWrapper.newBuilder()
                .setServer2Client(server2Client)
                .build();
        ChannelFuture write = ctx.channel().writeAndFlush(dfsMessagesWrapper);
        write.syncUninterruptibly();
    }

    public void handleRetrieveChunkRequest(ChannelHandlerContext ctx, Message.Client2Node.RetrieveChunkRequest request) {
        String filePath = request.getFilePath();
        String hashedFilePath = FileProcessUtil.getHashedFilename(request.getFilePath());
        int chunkId = request.getChunkId();
        byte[] chunk = storageNode.extractChunk(filePath, chunkId);
        Message.ChunkMetadata chunkMetadata = storageNode.extractChunkMetadata(filePath, chunkId);
        Message.Node2Client.RetrieveChunkRespond respond;
        if (chunk == null || chunkMetadata == null) {
            log.info(hashedFilePath + "/chunk" + chunkId + " not found");
            respond = Message.Node2Client.RetrieveChunkRespond.newBuilder()
                    .setHasFile(false)
                    .setMetadataOnly(false)
                    .build();
        } else {
            String actualChecksum = ChecksumUtil.getMD5Checksum(chunk);
            String expectedChecksum = chunkMetadata.getChecksum();
            if (!expectedChecksum.equals(actualChecksum)) {
                log.info("[BROKEN] broken chunk detected, execute repairing process. Id: " + chunkId);
                storageNode.askServerChunkPosition(ctx, request);
                return;
            }
            respond = Message.Node2Client.RetrieveChunkRespond.newBuilder()
                    .setHasFile(true)
                    .setChunk(ByteString.copyFrom(chunk))
                    .setMetadata(chunkMetadata)
                    .setMetadataOnly(false)
                    .build();
        }
        Message.Node2Client node2Client = Message.Node2Client.newBuilder()
                .setRetrieveChunkRespond(respond)
                .build();
        Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                .setNode2Client(node2Client)
                .build();
        ctx.writeAndFlush(wrapper);
    }

    public void handleRetrieveMetadataRequest(ChannelHandlerContext ctx, Message.Client2Node.RetrieveChunkRequest request) {
        String filePath = request.getFilePath();
        String hashedFilePath = FileProcessUtil.getHashedFilename(request.getFilePath());
        int chunkId = request.getChunkId();
        Message.ChunkMetadata chunkMetadata = storageNode.extractChunkMetadata(filePath, chunkId);
        Message.Node2Client.RetrieveChunkRespond respond;
        if (chunkMetadata == null) {
            log.info(hashedFilePath + "/chunk" + chunkId + " not found");
            respond = Message.Node2Client.RetrieveChunkRespond.newBuilder()
                    .setHasFile(false)
                    .setMetadataOnly(true)
                    .build();
        } else {
            respond = Message.Node2Client.RetrieveChunkRespond.newBuilder()
                    .setHasFile(true)
                    .setMetadata(chunkMetadata)
                    .setMetadataOnly(true)
                    .build();
        }
        Message.Node2Client node2Client = Message.Node2Client.newBuilder()
                .setRetrieveChunkRespond(respond)
                .build();
        Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                .setNode2Client(node2Client)
                .build();
        ctx.channel().writeAndFlush(wrapper).syncUninterruptibly();
    }

    private void sendBackStatistic(ChannelHandlerContext ctx) {
        int requestNum = nodeProcessRecorder.getProcessedNumber();
        Message.Node2Client node2Client = Message.Node2Client.newBuilder()
                .setRequestNumber(requestNum)
                .build();
        Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                .setNode2Client(node2Client)
                .build();
        ctx.writeAndFlush(wrapper).syncUninterruptibly();
    }

    /**
     * Handling message from other nodes
     *
     * @param ctx
     * @param node2Node
     */
    private void node2NodeProcess(ChannelHandlerContext ctx, Message.Node2Node node2Node) {
        if (node2Node.hasChunkTransfer()) {
            Message.Node2Node.ChunkTransfer chunkTransfer = node2Node.getChunkTransfer();
            if (chunkTransfer.getDatatype() == Message.Node2Node.DataType.REPLICA_1) {
                storageNode.storeChunk(chunkTransfer.getMetadata(), chunkTransfer.getChunkData().toByteArray());
                storageNode.sendReplica(chunkTransfer.getMetadata(), chunkTransfer.getChunkData(), Message.Node2Node.DataType.REPLICA_2);
            } else if (chunkTransfer.getDatatype() == Message.Node2Node.DataType.REPLICA_2) {
                storageNode.storeChunk(chunkTransfer.getMetadata(), chunkTransfer.getChunkData().toByteArray());
            } else if (chunkTransfer.getDatatype() == Message.Node2Node.DataType.RECOVERY) {
                storageNode.storeChunk(chunkTransfer.getMetadata(), chunkTransfer.getChunkData().toByteArray());
            }
        } else if (node2Node.hasChunkRequest()) {
            Message.Node2Node.ChunkRequest chunkRequest = node2Node.getChunkRequest();
            log.info("[REPAIR REQUEST] received a repair request from another node: " + chunkRequest.getFilepath() + "/chunk" + chunkRequest.getChunkId());
            byte[] data = storageNode.extractChunk(chunkRequest.getFilepath(), chunkRequest.getChunkId());
            Message.ChunkMetadata chunkMetadata = storageNode.extractChunkMetadata(chunkRequest.getFilepath(), chunkRequest.getChunkId());
            if (data == null || chunkMetadata == null) {
                log.info("[REPAIR REQUEST] Cannot find the chunk, do nothing here");
                return;
            }
            Message.Node2Node.ChunkTransfer chunkTransfer = Message.Node2Node.ChunkTransfer.newBuilder()
                    .setChunkData(ByteString.copyFrom(data))
                    .setMetadata(chunkMetadata)
                    .setDatatype(Message.Node2Node.DataType.REPAIR)
                    .build();
            Message.Node2Node n2n = Message.Node2Node.newBuilder().setChunkTransfer(chunkTransfer).build();
            Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder().setNode2Node(n2n).build();
            ctx.writeAndFlush(wrapper).addListener((ChannelFutureListener) channelFuture -> {
                if (channelFuture.isDone()) {
                    log.info("Chunk has sent");
                }
            });
        }
    }
}
