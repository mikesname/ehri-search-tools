package eu.ehri.project.indexing.akka;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Resources;
import org.junit.Test;

import java.util.concurrent.CompletionStage;

/**
 *
 */
public class AkkaDevTest {

    private static final ActorSystem system = ActorSystem.create("Test");
    private static final Materializer mat  = ActorMaterializer.create(system);

    @Test
    public void jsonNodeFlow() throws Exception {


        ByteString bytes = ByteString.fromString("[{\"foo\": \"bar\"}, {\"bar\": \"baz\"}]");
        final Source<ByteString, akka.NotUsed> byteStringSource = Source.single(bytes);

        byteStringSource
                .via(AkkaDev.bytesToJsonNode())
                .runForeach(jn -> System.out.println("Node: " + jn), mat)
                .toCompletableFuture()
                .get();
    }

    @Test
    public void testNodeConverterFlow() throws Exception {

        byte[] inputdoc3 = Resources.toByteArray(Resources.getResource("inputdoc3.json"));
        final Source<ByteString, akka.NotUsed> byteStringSource = Source.single(ByteString.fromArray(inputdoc3));

        Source<JsonNode, NotUsed> nodeSource = byteStringSource
                .via(AkkaDev.bytesToJsonNode())
                .via(AkkaDev.converterFlow());

        Sink.combine()

        Source<String, NotUsed> out = AkkaDev.writeStream(nodeSource, "http://localhost:9000/echo");

        Sink<String, CompletionStage<String>> head = Sink.head();
        String s = out.runWith(head, mat).toCompletableFuture().get();
        System.out.println(s);





    }

}