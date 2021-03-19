package edu.usfca.cs.dfs.message.sender;

import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.proto.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Hashtable;

public abstract class MessageSender extends SimpleChannelInboundHandler<Message.DFSMessagesWrapper> {

    private Hashtable<String, Channel> channelTable;


    public MessageSender() {
        this.channelTable = new Hashtable<>();
    }

    public Channel connectTo(String hostWithPort) {
        String[] hostAndPort = hostWithPort.split(":");
        try {
            if (hostAndPort.length == 2) {
                return connectTo(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
            } else {
                return null;
            }
        } catch (RuntimeException e) {
            System.out.println(e);
            return null;
        }
    }

    public Channel connectTo(String hostname, int port) {
        String key = hostname + ":" + port;

        Channel channel = null;

        if(channelTable.containsKey(key)){
            channel = channelTable.get(key);
            if(channel.isActive()) {
                return channel;
            }
        }

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(this);
        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);
        // System.out.println("Connecting to " + hostname + ":" + port);
        ChannelFuture cf = bootstrap.connect(hostname, port);
        cf.syncUninterruptibly();

        channelTable.put(key, cf.channel());

        return cf.channel();
    }
}
