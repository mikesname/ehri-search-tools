package eu.ehri.project.indexing.akka;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.common.EntityStreamingSupport;
import akka.http.javadsl.common.JsonEntityStreamingSupport;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpMethod;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.stream.ActorMaterializer;
import akka.stream.Attributes;
import akka.stream.Inlet;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.SinkShape;
import akka.stream.impl.SourceQueueAdapter;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import eu.ehri.project.indexing.converter.impl.JsonConverter;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


/**
 *
 */
public class AkkaDev {

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final JsonEntityStreamingSupport jsonSupport = EntityStreamingSupport.json();

    private static final ActorSystem actorSystem = ActorSystem.create("json-conversions");
    private static final Materializer mat = ActorMaterializer.create(actorSystem);

    public static Flow<JsonNode, JsonNode, akka.NotUsed> converterFlow() {
        JsonConverter jsonConverter = new JsonConverter();
        return Flow
                .of(JsonNode.class)
                .flatMapConcat(n -> Source.from(jsonConverter.convert(n)));
    }

    public static Source<JsonNode, akka.NotUsed> httpSource(String url) {
        return httpSource(url, Lists.newArrayList());
    }

    public static Source<JsonNode, akka.NotUsed> httpSource(String url, List<HttpHeader> headers) {

        HttpRequest httpRequest = HttpRequest.GET(url).addHeaders(headers);
        CompletableFuture<HttpResponse> future = Http.get(actorSystem)
                .singleRequest(httpRequest, mat).toCompletableFuture();

        return Source
                .fromCompletionStage(future)
                .flatMapConcat(r -> r.entity().getDataBytes())
                .via(bytesToJsonNode())
                .via(converterFlow());
    }

    public static Source<String, NotUsed> writeStream(Source<JsonNode, ?> source, String url) {
        HttpEntity.Chunked data = HttpEntities.create(ContentTypes.APPLICATION_JSON, source
            .via(jsonNodeToBytes())
            .via(jsonSupport.framingRenderer()));

        HttpRequest request = HttpRequest.POST(url).withEntity(data);
        CompletionStage<HttpResponse> f = Http.get(actorSystem)
                .singleRequest(request, mat);



        return Source.fromCompletionStage(f)
                .flatMapConcat(r -> r.entity()
                        .getDataBytes().fold("", (s, a) -> s + a.utf8String()));
    }

    static class HttpSink extends GraphStage<SinkShape<ByteString>> {

        private final String url;
        private final HttpMethod method;

        public HttpSink(String url, HttpMethod method) {
            this.url = url;
            this.method = method;
        }

        public static final Inlet<ByteString> in = Inlet.create("Bytes.in");

        private static final SinkShape<ByteString> shape = SinkShape.of(in);

        @Override
        public SinkShape<ByteString> shape() {
            return shape;
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) {


            final Source<ByteString, SourceQueueWithComplete<ByteString>> queue = Source
                    .queue(1000, OverflowStrategy.backpressure());

            return new GraphStageLogic(shape) {
                @Override
                public void preStart() {
                    super.preStart();
                }

                @Override
                public void postStop() {
                    super.postStop();
                }

                {
                    setHandler(in, new AbstractInHandler() {
                        @Override
                        public void onPush() throws Exception {


                        }
                    });
                }
            };
        }
    }

    public static Flow<ByteString, JsonNode, akka.NotUsed> bytesToJsonNode() {
        return Flow
                .of(ByteString.class)
                .via(jsonSupport.framingDecoder())
                .map((Function<ByteString, JsonNode>) bs ->
                        mapper.<JsonNode>readValue(bs.toArray(), JsonNode.class));
    }

    public static Flow<JsonNode, ByteString, akka.NotUsed> jsonNodeToBytes() {
        return Flow
                .of(JsonNode.class)
                .map((Function<JsonNode, ByteString>) n ->
                    ByteString.fromArray(mapper.<JsonNode>writeValueAsBytes(n)));
    }
}
