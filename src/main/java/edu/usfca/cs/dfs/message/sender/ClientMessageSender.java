package edu.usfca.cs.dfs.message.sender;

import edu.usfca.cs.dfs.basicClient.Client;
import edu.usfca.cs.dfs.proto.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

import java.util.Hashtable;

@ChannelHandler.Sharable
public class ClientMessageSender extends MessageSender {

    private Client client;
    static Logger log = Logger.getLogger(ClientMessageSender.class);
    Hashtable<String, Channel> channelMap;

    public ClientMessageSender(Client client) {
        this.client = client;
        this.channelMap = new Hashtable<>();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message.DFSMessagesWrapper dfsMessagesWrapper) {
        if (dfsMessagesWrapper.hasNode2Client()) {
            Message.Node2Client node2Client = dfsMessagesWrapper.getNode2Client();
            if (node2Client.hasRetrieveChunkRespond()) {
                Message.Node2Client.RetrieveChunkRespond retrieveChunkRespond = node2Client.getRetrieveChunkRespond();
                if (retrieveChunkRespond.getMetadataOnly()) {
                    client.receiveChunkMetadataHandler(retrieveChunkRespond);
                }
            }
        }
        //TODO: Should/Can I close the channel???
        // ctx.channel().close();
    }

    public void sendRequest(String ip, int port, Message.DFSMessagesWrapper wrapper) {
        String ipWithPort = ip + ":" + port;
        Channel channel = connectTo(ipWithPort);
        //TODO: do we have to do syncUninterruptibly?
        channel.writeAndFlush(wrapper).syncUninterruptibly();
    }

    public void sendRequest(String ipWithPort, Message.DFSMessagesWrapper wrapper) {
        Channel channel = connectTo(ipWithPort);
        //TODO: do we have to do syncUninterruptibly?
        //        System.out.println("sendRequest: " + wrapper);
        //        System.out.println("sendRequest: " + ipWithPort);
        //        System.out.println("sendRequest: " + channel);
        channel.writeAndFlush(wrapper).syncUninterruptibly();
    }
}

