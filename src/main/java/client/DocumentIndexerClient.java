package client;

import common.Config;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;

public class DocumentIndexerClient {

    public static void print(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    public static void main(String[] args) {
        DocumentIndexerClient client = new DocumentIndexerClient();
        client.start(Integer.parseInt(args[0]));
    }

    public void start(int id) {
        String host = "127.0.0.1";
        int port = Config.DIServerPort + id;

        Bootstrap bootstrap = new Bootstrap();
        EventLoopGroup group = new NioEventLoopGroup();

        bootstrap.group(group);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel socketChannel) {
                ChannelPipeline channelPipeline = socketChannel.pipeline();

                channelPipeline.addLast("frame delimiter", new DelimiterBasedFrameDecoder(256, Delimiters.lineDelimiter()));
                channelPipeline.addLast("String Decoder", new StringDecoder());
                channelPipeline.addLast("Client Handler", new ClientHandler());
                channelPipeline.addLast("String Encoder", new StringEncoder());
            }
        });

        try {
            ChannelFuture channelFuture = bootstrap.connect(host, port).sync();
            Channel ch = channelFuture.sync().channel();

            ch.writeAndFlush("leader\n");

            int lines = 10;

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            while(lines-- > 0) {
                ch.writeAndFlush(br.readLine() + "\n");
            }

            channelFuture.channel().closeFuture().sync();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
    }
}
