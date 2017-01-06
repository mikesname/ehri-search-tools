package eu.ehri.project.indexing;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.model.Uri;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.javadsl.Concat;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import eu.ehri.project.indexing.akka.AkkaDev;
import eu.ehri.project.indexing.index.Index;
import eu.ehri.project.indexing.index.impl.SolrIndex;
import eu.ehri.project.indexing.utils.Stats;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Pull data from the EHRI REST API and index it in Solr.
 * <p/>
 * Designed allow very flexible input/output options without
 * incurring excessive complexity in the main logic. Orchestrates
 * a source, a converter, and one or more sink objects to get some JSON
 * data, convert it to another format, and put it somewhere.
 */
public class IndexHelper {

    // Default service end points.
    // TODO: Store these in a properties file?
    public static final String DEFAULT_SOLR_URL = "http://localhost:8983/solr/portal";
    public static final String DEFAULT_EHRI_URL = "http://localhost:7474/ehri";

    enum ErrCodes {
        BAD_SOURCE_ERR(3),
        BAD_SINK_ERR(4),
        BAD_CONVERSION_ERR(5),
        BAD_STATE_ERR(6),
        INDEX_ERR(7);

        private final int code;

        ErrCodes(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    /**
     * Turn a list of specs into a set of EHRI web service URLs to download
     * JSON data from.
     * <p/>
     * Specs can be:
     * <ul>
     * <li>An item class name, e.g. &quot;DocumentaryUnit&quot;</li>
     * <li>Item ids prefixed with an &quot;@&quot;</li>
     * <li>An item type <i>and</i> ID, which denotes downloading
     * the contents of that item, e.g. it's child items, and
     * their descendants.</li>
     * </ul>
     *
     * @param serviceUrl The base REST URL
     * @param specs      A list of specs
     * @return A list of URLs
     */
    public static List<URI> urlsFromSpecs(String serviceUrl, String... specs) {
        List<URI> urls = Lists.newArrayList();
        List<String> ids = Lists.newArrayList();
        for (String spec : specs) {
            // Item type and id - denotes fetching child items (?)
            if (spec.contains("|")) {
                Iterable<String> split = Splitter.on("|").limit(2).split(spec);
                String type = Iterables.get(split, 0);
                String id = Iterables.get(split, 1);
                URI url = UriBuilder.fromPath(serviceUrl)
                        .segment("classes")
                        .segment(type).segment(id).segment("list")
                        .queryParam("limit", -1)
                        .queryParam("all", true).build();
                urls.add(url);
            } else if (spec.startsWith("@")) {
                ids.add(spec.substring(1));
            } else {
                URI url = UriBuilder.fromPath(serviceUrl)
                        .segment("classes")
                        .segment(spec)
                        .queryParam("limit", -1).build();
                urls.add(url);
            }
        }

        // Unlike types or children, multiple ids are done in one request.
        if (!ids.isEmpty()) {
            UriBuilder idBuilder = UriBuilder.fromPath(serviceUrl).segment("entities");
            for (String id : ids) {
                idBuilder = idBuilder.queryParam("id", id);
            }
            urls.add(idBuilder.queryParam("limit", -1).build());
        }
        return urls;
    }


    @SuppressWarnings("static-access")
    public static void main(String[] args) throws IOException, ParseException, ExecutionException, InterruptedException {

        // Long opts
        final String PRINT = "print";
        final String PRETTY = "pretty";
        final String CLEAR_ALL = "clear-all";
        final String CLEAR_KEY_VALUE = "clear-key-value";
        final String CLEAR_ID = "clear-id";
        final String CLEAR_TYPE = "clear-type";
        final String FILE = "file";
        final String REST_URL = "rest";
        final String HEADERS = "H";
        final String SOLR_URL = "solr";
        final String INDEX = "index";
        final String NO_CONVERT = "noconvert";
        final String VERBOSE = "verbose";
        final String VERSION = "version";
        final String STATS = "stats";
        final String HELP = "help";

        Options options = new Options();
        options.addOption("p", "print", false,
                "Print converted JSON to stdout. The default action in the omission of --index.");
        options.addOption("D", CLEAR_ALL, false,
                "Clear entire index first (use with caution.)");
        options.addOption(Option.builder("K").longOpt(CLEAR_KEY_VALUE)
                .argName("key=value")
                .numberOfArgs(2)
                .valueSeparator()
                .desc("Clear items with a given key=value pair. Can be used multiple times.")
                .build());
        options.addOption("c", CLEAR_ID, true,
                "Clear an individual id. Can be used multiple times.");
        options.addOption("C", CLEAR_TYPE, true,
                "Clear an item type. Can be used multiple times.");
        options.addOption("P", PRETTY, false,
                "Pretty print out JSON given by --print (implies --print).");
        options.addOption("s", SOLR_URL, true,
                "Base URL for Solr service (minus the action segment.)");
        options.addOption("f", FILE, true,
                "Read input from a file instead of the REST service. Use '-' for stdin.");
        options.addOption("r", REST_URL, true,
                "Base URL for EHRI REST service.");
        options.addOption(Option.builder(HEADERS)
                .argName("header=value")
                .numberOfArgs(2)
                .valueSeparator()
                .desc("Set a header for the REST service.")
                .build());
        options.addOption("i", INDEX, false,
                "Index the data. This is NOT the default for safety reasons.");
        options.addOption("n", NO_CONVERT, false,
                "Don't convert data to index format.");
        options.addOption("v", VERBOSE, false,
                "Print individual item ids to show progress.");
        options.addOption(Option.builder().longOpt(VERSION)
                .desc("Print the version number and exit.")
                .build());
        options.addOption("S", STATS, false, "Print indexing stats.");
        options.addOption("h", HELP, false, "Print this message.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        final String toolName = IndexHelper.class.getPackage().getImplementationTitle();
        final String toolVersion = IndexHelper.class.getPackage().getImplementationVersion();

        if (cmd.hasOption(VERSION)) {
            System.out.println(toolName + " " + toolVersion);
            System.exit(0);
        }

        String usage = toolName + " [OPTIONS] <spec> ... <specN>";
        String help = "\n" +
                "Each <spec> should consist of:\n" +
                "   * an item type (all items of that type)\n" +
                "   * an item id prefixed with '@' (individual items)\n" +
                "   * a type|id (bar separated - all children of an item)\n\n\n" +
                "The default URIs for Solr and the REST service are:\n" +
                " * " + DEFAULT_EHRI_URL + "\n" +
                " * " + DEFAULT_SOLR_URL + "\n\n";

        if (cmd.hasOption(HELP)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(usage, null, options, help);
            System.exit(1);
        }

        String ehriUrl = cmd.getOptionValue(REST_URL, DEFAULT_EHRI_URL);
        String solrUrl = cmd.getOptionValue(SOLR_URL, DEFAULT_SOLR_URL);
        Properties restHeaders = cmd.getOptionProperties(HEADERS);

        final ActorSystem system = ActorSystem.create("Test");
        final Materializer mat = ActorMaterializer.create(system);

        AkkaDev streams = new AkkaDev(system, mat);

        // Initialize the index...
        Index index = new SolrIndex(solrUrl);

        List<Source<JsonNode, ?>> sources = Lists.newArrayList();
        List<Flow<JsonNode, JsonNode, ?>> converters = Lists.newArrayList();

        if (cmd.hasOption(FILE)) {
            try {
                for (String fileName : cmd.getOptionValues(FILE)) {
                    if (fileName.trim().equals("-")) {
                        sources.add(streams.inputStreamJsonNodeSource(() -> System.in));
                    } else {
                        System.err.println("Adding file source: " + fileName);
                        FileInputStream fileInputStream = new FileInputStream(new File(fileName));
                        sources.add(streams.inputStreamJsonNodeSource(() -> fileInputStream));
                    }
                }
            } catch (FileNotFoundException e) {
                System.err.println(e.getMessage());
                System.exit(ErrCodes.BAD_SOURCE_ERR.getCode());
            }
        }

        Uri[] uris = urlsFromSpecs(ehriUrl, cmd.getArgs())
                .stream()
                .map(u -> Uri.create(u.toString()))
                .collect(Collectors.toList())
                .toArray(new Uri[cmd.getArgs().length]);
        sources.add(streams
                .httpRequestSource(uris)
                .via(streams.requestsToJsonNode(Uri.create(ehriUrl))));

        // Determine if we need to actually index the data...
//        if (cmd.hasOption(INDEX)) {
//            sinks.add(streams.jsonNodeHttpSink(Uri.create(solrUrl + "/update?commit=true")));
//        }

//        // Determine if we're printing the data...
//        if (!cmd.hasOption(INDEX) || cmd.hasOption(PRINT) || cmd.hasOption(PRETTY)) {
//            System.err.println("Adding out sink...");
//            sinks.add(streams.jsonNodeOutputStreamSink(() -> System.out, cmd.hasOption(PRETTY)));
//        }

        if (!cmd.hasOption(NO_CONVERT)) {
            converters.add(streams.jsonNodeToDoc());
        }

        Source<JsonNode, NotUsed> allSrc = combineSources(sources);
        Flow<JsonNode, JsonNode, NotUsed> allConverters = chainConverters(converters);

        final Stats stats = new Stats();
        Sink<JsonNode, CompletionStage<Done>> statsSink = cmd.hasOption(VERBOSE) || cmd.hasOption(STATS)
                ? Sink.foreach(f -> stats.incrementCount())
                : Sink.<JsonNode>ignore();

        Sink<JsonNode, CompletionStage<IOResult>> printSink = !cmd.hasOption(INDEX) || cmd.hasOption(PRINT) || cmd.hasOption(PRETTY)
                ? streams.jsonNodeOutputStreamSink(() -> System.out, true)
                : Sink.<JsonNode>ignore().mapMaterializedValue(f -> f.thenApply(d -> IOResult.createSuccessful(0L)));

        Sink<JsonNode, CompletionStage<?>> solrSink = cmd.hasOption(INDEX)
                ? streams.jsonNodeHttpSink(Uri.create(solrUrl + "/update?commit=true"))
                : Sink.<JsonNode>ignore().mapMaterializedValue(f -> f.thenApply(d -> IOResult.createSuccessful(0L)));

        allSrc.via(allConverters)
                .alsoToMat(printSink, Keep.right())
                .alsoToMat(statsSink, Keep.right())
                .toMat(solrSink, Keep.right())
                .run(mat)
                .toCompletableFuture()
                .get();

        if (cmd.hasOption(VERBOSE) || cmd.hasOption(STATS)) {
            stats.printReport(System.err);
        }

        system.terminate();
    }

    private static Flow<JsonNode, JsonNode, NotUsed> chainConverters(List<Flow<JsonNode, JsonNode, ?>> converters) {
        if (converters.size() == 0) {
            return Flow.of(JsonNode.class);
        } else if (converters.size() == 1) {
            return converters.get(0).mapMaterializedValue(f -> NotUsed.getInstance());
        } else {
            Flow<JsonNode, JsonNode, ?> first = converters.get(0);
            Flow<JsonNode, JsonNode, ?> second = converters.get(1);
            List<Flow<JsonNode, JsonNode, ?>> rest = converters.subList(2, converters.size());
            return first.via(second)
                    .mapMaterializedValue(f -> NotUsed.getInstance())
                    .via(chainConverters(rest));
        }
    }

    private static Source<JsonNode, NotUsed> combineSources(List<Source<JsonNode, ?>> sources) {
        if (sources.size() == 0) {
            return Source.empty();
        } else if (sources.size() == 1) {
            return sources.get(0).mapMaterializedValue(f -> NotUsed.getInstance());
        } else {
            Source<JsonNode, ?> first = sources.get(0);
            Source<JsonNode, ?> second = sources.get(1);
            List<Source<JsonNode, ?>> rest = sources.subList(2, sources.size());
            return Source.combine(first, second, rest, Concat::create);
        }
    }
}
