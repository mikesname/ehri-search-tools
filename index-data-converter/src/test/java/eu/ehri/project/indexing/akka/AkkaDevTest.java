package eu.ehri.project.indexing.akka;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.model.Uri;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.javadsl.Concat;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CompletionStage;

/**
 *
 */
public class AkkaDevTest {

    private static final ActorSystem system = ActorSystem.create("Test");
    private static final Materializer mat = ActorMaterializer.create(system);

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

        AkkaDev pipe = new AkkaDev(system, mat);

        Uri baseUri = Uri.create("http://localhost:7474");
        Uri srcUri1 = Uri.create("http://localhost:7474/ehri/classes/UserProfile?limit=-1");
        Uri srcUri2 = Uri.create("http://localhost:7474/ehri/classes/Group?limit=-1");

        Source<JsonNode, NotUsed> httpSrc = pipe
                .httpRequestSource(srcUri1, srcUri2)
                .via(pipe.requestsToJsonNode(baseUri));

        Source<JsonNode, CompletionStage<IOResult>> fileSrc = StreamConverters
                .fromInputStream(() -> Resources.getResource("inputdoc3.json").openStream())
                .via(pipe.bytesToJsonNode());

        Source<JsonNode, NotUsed> combinedSrc = Source.combine(
                httpSrc, fileSrc, Lists.newArrayList(), Concat::<JsonNode>create);

        Flow<JsonNode, JsonNode, NotUsed> toDocs = pipe.jsonNodeToDoc();
        Flow<JsonNode, ByteString, NotUsed> docToBytes = pipe.jsonNodeToBytes(false);

        Uri sinkUri = Uri.create("http://localhost:8983/solr/portal/update?commit=true");
        Sink<ByteString, CompletionStage<String>> byteSink = pipe.httpSinkFlow(sinkUri);

        CompletionStage<String> out = combinedSrc.via(toDocs)
                .map(d -> {
                    System.out.println("DOC: " + d);
                    return d;
                }).via(docToBytes).runWith(byteSink, mat);

        String result = out.toCompletableFuture().get();
        System.out.println(result);
    }

}