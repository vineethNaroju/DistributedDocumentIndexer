package server;

import common.Config;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import server.ratis.DIRaftServer;

import java.util.Date;
import java.util.Scanner;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DocumentIndexerServer {

    private final DIRaftServer raftServer;
    private final int id;

    DocumentIndexerServer(int id) throws Exception {
        this.id = id;
        raftServer = new DIRaftServer(id);
    }

    public void println(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    public static void main(String[] args) throws Exception {
        int id = Integer.parseInt(args[0]);
        DocumentIndexerServer server = new DocumentIndexerServer(id);
        server.start();
    }

    public void start() throws Exception {
//        for(int i=0; i<100;i++) {
//            Thread.sleep(1000);
//            println(raftServer.checkStatus());
//        }
//        new Scanner(System.in, UTF_8.name()).nextLine();


        ServerBootstrap bootstrap;
        EventLoopGroup bossGroup, workerGroup;

        int port = Config.DIServerPort + id;

        bootstrap = new ServerBootstrap();
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());

        bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class);

        bootstrap.handler(new LoggingHandler(LogLevel.INFO));
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel socketChannel) {
                ChannelPipeline channelPipeline = socketChannel.pipeline();

                channelPipeline.addLast("frame delimiter", new DelimiterBasedFrameDecoder(256, Delimiters.lineDelimiter()));

                channelPipeline.addLast("String Decoder", new StringDecoder());

                channelPipeline.addLast("Server Handler", new ServerHandler(raftServer));

                channelPipeline.addLast("String Encoder", new StringEncoder());

            }
        });

        try {

            ChannelFuture channelFuture = bootstrap.bind(port).sync();

            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) {
                    println("operationComplete");
                }
            });

            channelFuture.channel().closeFuture().sync();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
