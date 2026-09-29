package com.github.guoyyit.causalbroadcastjava.persistence;

import com.github.guoyyit.causalbroadcastjava.core.ServiceState;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Persists the service state to local storage and reloads it on restart.
 * Writes are atomic: the snapshot is written to a temporary file and then
 * moved over the target, so a crash can never leave a torn state file.
 */
public class StateStore {

    private static final String STATE_FILE_NAME = "state.json";

    private final ObjectMapper mapper = new ObjectMapper();
    private final Path dataDir;
    private final Path stateFile;

    public StateStore(Path dataDir) {
        this.dataDir = dataDir;
        this.stateFile = dataDir.resolve(STATE_FILE_NAME);
    }

    public synchronized ServiceState load() {
        if (!Files.exists(stateFile)) {
            return new ServiceState();
        }
        try {
            return mapper.readValue(stateFile.toFile(), ServiceState.class);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load state from " + stateFile, e);
        }
    }

    public synchronized void save(ServiceState state) {
        try {
            Files.createDirectories(dataDir);
            Path tmp = dataDir.resolve(STATE_FILE_NAME + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), state);
            try {
                Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to save state to " + stateFile, e);
        }
    }

    public Path getStateFile() {
        return stateFile;
    }
}
