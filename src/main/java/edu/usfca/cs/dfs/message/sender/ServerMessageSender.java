package edu.usfca.cs.dfs.message.sender;

import edu.usfca.cs.dfs.basicClient.Client;
import edu.usfca.cs.dfs.proto.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

import java.util.ArrayList;

@ChannelHandler.Sharable
public class ServerMessageSender extends MessageSender {

    private Channel serverChannel;
    private Client client;

    static Logger log = Logger.getLogger(ServerMessageSender.class);

    public ServerMessageSender(Client client) {
        this.client = client;
    }

    public void connectToServer(String host, int port) {
        serverChannel = connectTo(host, port);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message.DFSMessagesWrapper dfsMessagesWrapper) {
        if (dfsMessagesWrapper.hasServer2Client()) {
            Message.Server2Client server2Client = dfsMessagesWrapper.getServer2Client();
            if (server2Client.hasRetrieveFileResponse()) {
                Message.Server2Client.RetrieveFileResponse retrieveFileResponse = server2Client.getRetrieveFileResponse();
                if (retrieveFileResponse.getIsFirstChunkResponse()) {
                    client.preRetrieveFile(retrieveFileResponse);
                } else {
                    client.retrieveFile(retrieveFileResponse);
                }
            } else if (server2Client.hasStoreFileResponse()) {
                Message.Server2Client.StoreFileResponse response = server2Client.getStoreFileResponse();
                client.sendChunksToNodes(response.getFilePath(), response.getChunkPlacementList());
            } else if (server2Client.hasCommandResponse()) {
                Message.Server2Client.CommandResponse commandResponse = server2Client.getCommandResponse();
                if (commandResponse.hasActiveNodes()) {
                    client.receiveCommandResult(commandResponse.getActiveNodes().getNodesInfo());
                } else if (commandResponse.hasListDirectories()) {
                    client.receiveCommandResult(commandResponse.getListDirectories().getDirectories());
                }
                else if (commandResponse.hasPosixLs()) {
                    posixLsRespondHandler(commandResponse.getPosixLs());
                }
            }
        }
    }

    private void posixLsRespondHandler(Message.Server2Client.CommandResponse.PosixLs posixLs){
        ArrayList<String> res = new ArrayList<>(posixLs.getFileNamesList());
        client.setPosixPathList(res);
        client.setPosixListPrepared(true);
    }

    public void sendStoreFileRequest(Message.FileMetadata metadata) {
        Message.Client2Server.StoreFileRequest sfr = Message.Client2Server.StoreFileRequest.newBuilder()
                .setMetadata(metadata)
                .build();
        Message.Client2Server request = Message.Client2Server.newBuilder()
                .setStoreFileRequest(sfr)
                .build();
        Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                .setClient2Server(request)
                .build();
        if (serverChannel != null) {
            ChannelFuture write = serverChannel.writeAndFlush(wrapper);
            write.syncUninterruptibly();
        }
    }

    public void sendRetrieveFileRequest(String filePath, int chunkNumber, boolean isFirstChunk, boolean isFromFuse) {
        Message.Client2Server.RetrieveFileRequest request = Message.Client2Server.RetrieveFileRequest
                .newBuilder()
                .setFilePath(filePath)
                .setChunkNumber(chunkNumber)
                .setIsFirstChunk(isFirstChunk)
                .setIsFromFuse(isFromFuse)
                .build();
        Message.Client2Server client2Server = Message.Client2Server.newBuilder()
                .setRetrieveFileRequest(request)
                .build();
        Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                .setClient2Server(client2Server)
                .build();
        if (serverChannel != null) {
            ChannelFuture write = serverChannel.writeAndFlush(wrapper);
            write.syncUninterruptibly();
        }
    }

    public void sendRetrieveFileRequestStep1(String filePath, boolean isFromFuse) {
        sendRetrieveFileRequest(filePath, 0, true, isFromFuse);
    }

    public void sendRetrieveFileRequestStep2(String filePath, int chunkNumber) {
        sendRetrieveFileRequest(filePath, chunkNumber, false, false);
    }

    public void sendCommand(Message.Client2Server.Command command) {
        Message.Client2Server client2Server = Message.Client2Server.newBuilder()
                .setCommand(command)
                .build();
        Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                .setClient2Server(client2Server)
                .build();
        if (serverChannel != null) {
            ChannelFuture write = serverChannel.writeAndFlush(wrapper);
            write.syncUninterruptibly();
        }
    }

    public boolean isServerNull() {
        return serverChannel == null;
    }
}
