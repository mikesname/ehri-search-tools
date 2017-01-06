package eu.ehri.project.indexing.akka;

import akka.Done;
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
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.RequestEntity;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.model.headers.RawHeader;
import akka.http.javadsl.settings.ClientConnectionSettings;
import akka.http.javadsl.settings.ConnectionPoolSettings;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import eu.ehri.project.indexing.converter.impl.JsonConverter;
import scala.concurrent.duration.FiniteDuration;
import scala.util.Try;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 *
 */
public class AkkaDev {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectWriter writer = mapper.writer().withDefaultPrettyPrinter();
    private static final ObjectWriter prettyWriter = writer.withDefaultPrettyPrinter();
    private static final JsonEntityStreamingSupport jsonSupport = EntityStreamingSupport.json(Integer.MAX_VALUE);
    private static final FiniteDuration timeout = FiniteDuration.create(20, TimeUnit.MINUTES);

    private final ActorSystem actorSystem;
    private final Materializer mat;
    private final ConnectionPoolSettings connectionPoolSettings;
    private final JsonConverter jsonConverter;

    public AkkaDev(ActorSystem actorSystem, Materializer mat) {
        this.actorSystem = actorSystem;
        this.mat = mat;
        ClientConnectionSettings clientConnectionSettings = ClientConnectionSettings
                .create(actorSystem)
                .withConnectingTimeout(timeout)
                .withIdleTimeout(timeout);
        this.connectionPoolSettings = ConnectionPoolSettings
                .create(actorSystem)
                //.withConnectionSettings(clientConnectionSettings)
                .withIdleTimeout(timeout);
        this.jsonConverter = new JsonConverter();
    }

    private Flow<Pair<HttpRequest, Uri>, Pair<Try<HttpResponse>, Uri>, HostConnectionPool> cachedHostFlow(Uri uri) {
        return Http
                .get(actorSystem)
                .<Uri>cachedHostConnectionPool(
                    ConnectHttp.toHost(uri.toString()), mat);
    }

    public Sink<JsonNode, CompletionStage<?>> jsonNodeHttpSink(Uri uri) {
        return jsonNodeToBytes(false).toMat(httpSinkFlow(uri), (u, m) -> m);
    }

    public Sink<JsonNode, CompletionStage<IOResult>> jsonNodeOutputStreamSink(Supplier<OutputStream> outputStream, boolean pretty) {
        return jsonNodeToBytes(pretty)
                .toMat(StreamConverters.fromOutputStream(outputStream::get, true), (u, m) -> m);
    }

    public Source<JsonNode, NotUsed> inputStreamJsonNodeSource(Supplier<InputStream> stream) {
        return StreamConverters
                .fromInputStream(stream::get)
                .via(bytesToJsonNode())
                .mapMaterializedValue(v -> NotUsed.getInstance());
    }

    public Sink<ByteString, CompletionStage<String>> httpSinkFlow(Uri uri) {
        return Flow.of(ByteString.class)
                .prefixAndTail(0)
                .map(pair -> {
                    RequestEntity entity = HttpEntities.create(ContentTypes.APPLICATION_JSON, pair.second());
                    Uri relativeUri = uri.toRelative();
                    HttpRequest request = HttpRequest
                            .create()
                            .withMethod(HttpMethods.POST)
                            .withUri(relativeUri)
                            .withEntity(entity);
                    return Pair.create(request, relativeUri);
                })
                .via(cachedHostFlow(uri))
                .filter(p -> p.first().isSuccess()) // FIXME: Handle errors?
                .map(p -> p.first().get())
                .flatMapConcat(r -> r.entity().withoutSizeLimit().getDataBytes())
                .toMat(Sink.fold(ByteString.empty(), ByteString::concat), Keep.right())
                .mapMaterializedValue(f -> f.thenApply(ByteString::utf8String));
    }

    public static final Flow<ByteString, JsonNode, akka.NotUsed> bytesToJsonNode = Flow
            .of(ByteString.class)
            .via(jsonSupport.framingDecoder())
            .map(bytes -> mapper.readValue(bytes.toArray(), JsonNode.class))
            .named("bytes-to-json-node");

    public static Flow<JsonNode, ByteString, akka.NotUsed> jsonNodeToBytes(boolean pretty) {
        final ObjectWriter objectWriter = pretty ? prettyWriter : writer;
        return Flow
                .of(JsonNode.class)
                .map(node -> ByteString.fromArray(objectWriter.writeValueAsBytes(node)))
                .via(jsonSupport.framingRenderer())
                .named("json-node-to-bytes");
    }

    public Flow<Pair<HttpRequest, Uri>, JsonNode, akka.NotUsed> requestsToJsonNode(Uri uri) {
        return Flow
                .<Pair<HttpRequest, Uri>>create()
                .via(cachedHostFlow(uri))
                .filter(p -> p.first().isSuccess()) // FIXME: Handle errors?
                .map(p -> {
                    HttpResponse response = p.first().get();
                    Uri u = p.second();
                    return response;
                })
                .flatMapConcat(r ->
                        r.entity().withoutSizeLimit().getDataBytes().via(bytesToJsonNode));
    }

    public Source<Pair<HttpRequest, Uri>, akka.NotUsed> httpRequestSource(Uri... uris) {
        List<Pair<HttpRequest, Uri>> requests = Lists.newArrayList(uris).stream().map(uri -> {
            Uri relativeUri = uri.toRelative();
            HttpRequest r = HttpRequest
                    .create()
                    .withMethod(HttpMethods.GET)
                    .addHeader(RawHeader.create("X-Stream", "true"))
                    .addHeader(RawHeader.create("X-User", "admin"))
                    .withUri(relativeUri);
            return Pair.create(r, relativeUri);
        }).collect(Collectors.toList());
        return Source.from(requests);
    }

    public Flow<JsonNode, JsonNode, akka.NotUsed> jsonNodeToDoc() {
        return Flow.of(JsonNode.class).mapConcat(jsonConverter::convert);
    }

    public Source<String, NotUsed> writeStream(Source<JsonNode, ?> source, String url) {
        HttpEntity.Chunked data = HttpEntities.create(ContentTypes.APPLICATION_JSON, source
                .via(jsonNodeToBytes(false))
                .via(jsonSupport.framingRenderer()));

        HttpRequest request = HttpRequest.POST(url).withEntity(data);
        CompletionStage<HttpResponse> f = Http.get(actorSystem)
                .singleRequest(request, mat);

        return Source.fromCompletionStage(f)
                .flatMapConcat(r -> r.entity()
                        .getDataBytes().fold("", (s, a) -> s + a.utf8String()));
    }


    public static Flow<ByteString, JsonNode, akka.NotUsed> bytesToJsonNode() {
        return Flow
                .of(ByteString.class)
                .via(jsonSupport.framingDecoder())
                .map((Function<ByteString, JsonNode>) bs ->
                        mapper.<JsonNode>readValue(bs.toArray(), JsonNode.class));
    }
}
