package org.kpull.transcend.eventsourcing.core

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.LineBasedFrameDecoder
import java.nio.charset.Charset

class CommandServerHandler(private val processor: JsonProcessor<*>) : ChannelInboundHandlerAdapter() {

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        val jsonBuffer = msg as ByteBuf
        processor.submitAsString(jsonBuffer.toString(Charset.defaultCharset()), { result ->
            val sendBuffer = ctx.alloc().buffer(result.length * 2)
            sendBuffer.writeCharSequence(result, Charset.defaultCharset())
            ctx.writeAndFlush(sendBuffer)
        }) { error ->
            val sendBuffer = ctx.alloc().buffer(error.length * 2)
            sendBuffer.writeCharSequence(error, Charset.defaultCharset())
            ctx.writeAndFlush(sendBuffer)
        }
        jsonBuffer.release()
    }

}

class TcpJsonCommandServer(private val processor: JsonProcessor<*>) {

    fun start(port: Int) {
        val bossGroup = NioEventLoopGroup()
        val workerGroup = NioEventLoopGroup()

        try {
            val b = ServerBootstrap()
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel::class.java)
                    .childHandler(object : ChannelInitializer<SocketChannel>() {
                        override fun initChannel(ch: SocketChannel) {
                            ch.pipeline()
                                    .addLast(LineBasedFrameDecoder(500000, true, true))
                                    .addLast(CommandServerHandler(processor))
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)

            val f = b.bind(port).sync()

            f.channel().closeFuture().sync()
        } finally {
            workerGroup.shutdownGracefully()
            bossGroup.shutdownGracefully()
        }
    }

}