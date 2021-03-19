package edu.usfca.cs.dfs.net;

import edu.usfca.cs.dfs.proto.Message;
import edu.usfca.cs.dfs.utils.Constant;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

public class MessagePipeline extends ChannelInitializer<SocketChannel> {

    private ChannelInboundHandlerAdapter inboundHandler;

    public MessagePipeline(ChannelInboundHandlerAdapter inboundHandler) {
        this.inboundHandler = inboundHandler;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        /* Here, we limit message sizes to 8192: */
        pipeline.addLast(new LengthFieldBasedFrameDecoder(Constant.NETTY_MAX_LENGTH_FRAME, 0, 4, 0, 4));
        pipeline.addLast(
                new ProtobufDecoder(
                        Message.DFSMessagesWrapper.getDefaultInstance()));
        pipeline.addLast(new LengthFieldPrepender(4));
        pipeline.addLast(new ProtobufEncoder());
        pipeline.addLast(inboundHandler);
    }
}
