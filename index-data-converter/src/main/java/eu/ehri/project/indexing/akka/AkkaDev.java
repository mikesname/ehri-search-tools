package eu.ehri.project.indexing.akka;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.HostConnectionPool;
import akka.http.javadsl.Http;
import akka.http.javadsl.common.EntityStreamingSupport;
import akka.http.javadsl.common.JsonEntityStreamingSupport;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.RequestEntity;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.settings.ClientConnectionSettings;
import akka.http.javadsl.settings.ConnectionPoolSettings;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import eu.ehri.project.indexing.converter.impl.JsonConverter;
import scala.PartialFunction;
import scala.concurrent.duration.FiniteDuration;
import scala.util.Try;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;


/**
 *
 */
public class AkkaDev {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectWriter writer = mapper.writer().withDefaultPrettyPrinter();
    private static final JsonEntityStreamingSupport jsonSupport = EntityStreamingSupport.json(Integer.MAX_VALUE);
    private static final FiniteDuration timeout = FiniteDuration.create(20, TimeUnit.MINUTES);

    private final ActorSystem actorSystem;
    private final Materializer mat;
    private final ConnectionPoolSettings connectionPoolSettings;

    public AkkaDev(ActorSystem actorSystem, Materializer mat) {
        this.actorSystem = actorSystem;
        this.mat = mat;
        this.connectionPoolSettings = ConnectionPoolSettings
                .create(actorSystem)
                .withIdleTimeout(timeout)
                .withConnectionSettings(ClientConnectionSettings.create(actorSystem)
                        .withConnectingTimeout(timeout)
                        .withIdleTimeout(timeout));
    }

    Flow<Pair<HttpRequest, Uri>, Pair<Try<HttpResponse>, Uri>, HostConnectionPool> cachedHostFlow(Uri solrUri) {
        return Http.get(actorSystem).<Uri>cachedHostConnectionPool(
                ConnectHttp.toHost(solrUri.host().toString(), solrUri.port()),
                connectionPoolSettings, actorSystem.log(), mat);
    }

    Sink<ByteString, CompletionStage<String>> httpSinkFlow(Uri uri) {
        return Flow.of(ByteString.class)
                .prefixAndTail(0)
                .map(pair -> {
                    RequestEntity entity = HttpEntities.create(ContentTypes.APPLICATION_JSON, pair.second());
                    HttpRequest request = HttpRequest.create().withMethod(HttpMethods.POST)
                            .withEntity(entity);
                    return Pair.create(request, uri);
                })
                .via(cachedHostFlow(uri))
                .filter(p -> p.first().isSuccess()) // FIXME: Handle errors?
                .map(p -> p.first().get())
                .flatMapConcat(r -> r.entity().withoutSizeLimit().getDataBytes())
                .toMat(Sink.fold(ByteString.empty(), ByteString::concat), Keep.right())
                .mapMaterializedValue(f -> f.thenApply(ByteString::utf8String));
    }

    private static final Flow<ByteString, JsonNode, akka.NotUsed> bytesToJsonNode = Flow
            .of(ByteString.class)
            .via(jsonSupport.framingDecoder())
            .map(bytes -> mapper.readValue(bytes.toArray(), JsonNode.class))
            .named("bytes-to-json-node");

    private static final Flow<JsonNode, ByteString, akka.NotUsed> jsonNodeToBytes = Flow
            .of(JsonNode.class)
            .map(node -> ByteString.fromArray(writer.writeValueAsBytes(node)))
            .named("json-node-to-bytes");

    public static Flow<JsonNode, JsonNode, akka.NotUsed> converterFlow() {
        JsonConverter jsonConverter = new JsonConverter();
        return Flow
                .of(JsonNode.class)
                .flatMapConcat(n -> Source.from(jsonConverter.convert(n)));
    }

    public Source<JsonNode, akka.NotUsed> httpSource(String url) {
        return httpSource(url, Lists.newArrayList());
    }

    public Source<JsonNode, akka.NotUsed> httpSource(String url, List<HttpHeader> headers) {

        HttpRequest httpRequest = HttpRequest.GET(url).addHeaders(headers);
        CompletableFuture<HttpResponse> future = Http.get(actorSystem)
                .singleRequest(httpRequest, mat).toCompletableFuture();

        return Source
                .fromCompletionStage(future)
                .flatMapConcat(r -> r.entity().getDataBytes())
                .via(bytesToJsonNode())
                .via(converterFlow());
    }

    public Source<String, NotUsed> writeStream(Source<JsonNode, ?> source, String url) {
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


    public Flow<ByteString, JsonNode, akka.NotUsed> bytesToJsonNode() {
        return Flow
                .of(ByteString.class)
                .via(jsonSupport.framingDecoder())
                .map((Function<ByteString, JsonNode>) bs ->
                        mapper.<JsonNode>readValue(bs.toArray(), JsonNode.class));
    }

    public Flow<JsonNode, ByteString, akka.NotUsed> jsonNodeToBytes() {
        return Flow
                .of(JsonNode.class)
                .map((Function<JsonNode, ByteString>) n ->
                        ByteString.fromArray(mapper.<JsonNode>writeValueAsBytes(n)));
    }
}
