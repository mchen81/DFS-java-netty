package edu.usfca.cs.dfs.message.router;

import edu.usfca.cs.dfs.net.ServerMessageRouter;
import edu.usfca.cs.dfs.proto.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;
import java.net.InetSocketAddress;

public abstract class DFSMessageRouter extends SimpleChannelInboundHandler<Message.DFSMessagesWrapper> {

    private ServerMessageRouter messageRouter;

    public void start(int port) throws IOException {
        messageRouter = new ServerMessageRouter(this);
        messageRouter.listen(port);
        System.out.println(this.getClass().getName() + " is listening on port " + port + "...");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        /* A connection has been established */
        InetSocketAddress addr
                = (InetSocketAddress) ctx.channel().remoteAddress();
        //System.out.println("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        //System.out.println("Connection lost: " + addr);
    }
}
