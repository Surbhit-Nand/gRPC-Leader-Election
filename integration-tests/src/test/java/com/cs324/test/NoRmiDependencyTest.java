package com.cs324.test;

import com.cs324.common.BootstrapRemote;
import com.cs324.common.ClusterDirectory;
import com.cs324.common.Constants;
import com.cs324.common.WorkerAddress;
import com.cs324.common.WorkerControl;
import com.cs324.common.WorkerEvent;
import com.cs324.worker.WorkerRemoteImpl;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Guards the gRPC-only boundary: no RMI imports, supertypes, serialization, or port constant.
@SuppressWarnings("deprecation")
class NoRmiDependencyTest {

    private static final Pattern RMI_IMPORT = Pattern.compile("^\\s*import\\s+java\\.rmi\\.", Pattern.MULTILINE);

    @Test
    void noProductionSourceImportsRmi() throws Exception {
        Path root = Paths.get("").toAbsolutePath();
        if (!Files.exists(root.resolve("shared/src"))) {
            root = root.getParent();
        }
        try (Stream<Path> files = Files.walk(root)) {
            List<Path> offenders = files
                    .filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !path.toString().contains("target"))
                    .filter(NoRmiDependencyTest::importsRmi)
                    .toList();
            assertTrue(offenders.isEmpty(), "RMI imports found in: " + offenders);
        }
    }

    private static boolean importsRmi(Path path) {
        try {
            return RMI_IMPORT.matcher(Files.readString(path)).find();
        } catch (Exception e) {
            throw new IllegalStateException("Could not read " + path, e);
        }
    }

    @Test
    void productionTypesDoNotUseRmiSupertypesOrSerialization() throws Exception {
        Class<?> remote = Class.forName("java.rmi.Remote");
        assertFalse(remote.isAssignableFrom(WorkerControl.class));
        assertFalse(remote.isAssignableFrom(ClusterDirectory.class));
        assertFalse(remote.isAssignableFrom(BootstrapRemote.class));
        assertFalse(remote.isAssignableFrom(WorkerRemoteImpl.class));
        assertFalse(Serializable.class.isAssignableFrom(WorkerAddress.class));
        assertFalse(Serializable.class.isAssignableFrom(WorkerEvent.class));
    }

    @Test
    void sharedConstantsExposeOnlyGrpcPorts() {
        for (var field : Constants.class.getDeclaredFields()) {
            assertFalse(field.getName().toUpperCase().contains("RMI"), field.getName());
        }
    }
}
