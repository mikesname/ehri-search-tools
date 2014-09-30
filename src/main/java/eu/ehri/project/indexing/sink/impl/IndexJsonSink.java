package eu.ehri.project.indexing.sink.impl;

import com.google.common.io.FileBackedOutputStream;
import eu.ehri.project.indexing.index.Index;
import eu.ehri.project.indexing.sink.Sink;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class IndexJsonSink implements Sink<JsonNode> {

    private final Index index;
    private final FileBackedOutputStream out;
    private final OutputStreamJsonSink npw;
    private int writeCount = 0;

    public IndexJsonSink(Index index) {
        this.index = index;
        this.out = new FileBackedOutputStream(1024 * 1024);
        this.npw = new OutputStreamJsonSink(out);
    }

    public void write(JsonNode node) throws SinkException {
        writeCount++;
        npw.write(node);
    }

    public void finish() throws SinkException {
        npw.finish();
        final AtomicBoolean done = new AtomicBoolean(false);

        new Thread() {
            @Override
            public void run() {
                String[] cursor = {"|", "/", "-", "\\"};
                int cursorPos = 0;
                while (!done.get()) {
                    System.err.println("Updating index... " + cursor[cursorPos]);
                    cursorPos = cursorPos == 3 ? 0 : cursorPos + 1;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }.start();

        try {
            try {
                if (writeCount > 0) {
                    index.update(out.getSupplier().getInput(), true);
                    writeCount = 0;
                }
            } catch (Exception e) {
                throw new SinkException("Error updating Solr", e);
            }
            try {
                out.reset();
            } catch (IOException e) {
                throw new RuntimeException("Error closing temp stream for index: ", e);
            }
        } finally {
            done.getAndSet(true);
        }
    }
}