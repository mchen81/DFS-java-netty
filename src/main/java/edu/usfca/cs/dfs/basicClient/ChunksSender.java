package edu.usfca.cs.dfs.basicClient;

import edu.usfca.cs.dfs.message.sender.MessageSender;
import edu.usfca.cs.dfs.proto.Message;
import edu.usfca.cs.dfs.utils.ChannelHandlerContextUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@ChannelHandler.Sharable
public class ChunksSender extends MessageSender {

    private Set<String> nodeHosts;

    private int chunkNumber;

    private volatile int chunkSentNumber = 0;

    public ChunksSender(int chunkNumber, Set<String> nodeHosts) {
        this.nodeHosts = nodeHosts;
        this.chunkNumber = chunkNumber;
    }

    private Map<String, Channel> channelMap;

    static Logger log = Logger.getLogger(ChunksSender.class);

    public boolean isFinished() {
        return chunkNumber == chunkSentNumber;
    }

    public int getSentChunkNumber(){
        return chunkSentNumber;
    }

    /**
     * Create connections based on given host set @nodeHosts
     */
    public void startNodesConnections() {
        channelMap = new HashMap<>();
        for (String nodeHost : nodeHosts) {
            Channel channel = connectTo(nodeHost);
            channelMap.put(nodeHost, channel);
            log.info("[Chunk Sender] connection established: " + ChannelHandlerContextUtils.getRemoteAddressWithPort(channel));
        }
    }

    public void disconnectAll() {
        for (Channel channel : channelMap.values()) {
            channel.disconnect();
        }
    }

    /**
     * send the chunk to decided main node
     *
     * @param storeChunkRequest
     */
    public void sendChunk(Message.Client2Node.StoreChunkRequest storeChunkRequest) {
        if (channelMap != null) {
            String targetHost = storeChunkRequest.getMetadata().getPlacement().getMainNode();
            Channel targetChannel = channelMap.get(targetHost);
            if (targetChannel != null) {
                int chunkId = storeChunkRequest.getMetadata().getChunkId();
                Message.Client2Node client2Node = Message.Client2Node.newBuilder()
                        .setStoreChunkRequest(storeChunkRequest)
                        .build();
                Message.DFSMessagesWrapper wrapper = Message.DFSMessagesWrapper.newBuilder()
                        .setClient2Node(client2Node)
                        .build();
                targetChannel.writeAndFlush(wrapper).addListener((ChannelFutureListener) channelFuture -> {
                    if (channelFuture.isDone()) {
                        chunkSentNumber++;
                    }
                });
            }
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message.DFSMessagesWrapper dfsMessagesWrapper) throws Exception {
    }
}
