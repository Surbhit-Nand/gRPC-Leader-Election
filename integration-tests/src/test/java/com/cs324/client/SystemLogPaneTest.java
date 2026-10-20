package com.cs324.client;

import org.junit.jupiter.api.Test;
import javax.swing.SwingUtilities;
import javax.swing.text.StyleConstants;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class SystemLogPaneTest {
    @Test void eventColorsDistinguishFailureRecoveryAndSuccess() {
        assertEquals(SystemLogPane.Level.ERROR, SystemLogPane.levelOf("Worker 3 failed or unreachable; recovering unfinished task"));
        assertEquals(SystemLogPane.Level.ERROR, SystemLogPane.levelOf("Coordinator Worker 1 failed or disconnected"));
        assertEquals(SystemLogPane.Level.WARNING, SystemLogPane.levelOf("New coordinator elected: Worker 4"));
        assertEquals(SystemLogPane.Level.WARNING, SystemLogPane.levelOf("Task reassigned from Worker 3 to Worker 4 [MAX]"));
        assertEquals(SystemLogPane.Level.SUCCESS, SystemLogPane.levelOf("Reassigned task completed by Worker 4 [MAX]"));
        assertEquals(SystemLogPane.Level.SUCCESS, SystemLogPane.levelOf("Worker 4 joined"));
        assertEquals(SystemLogPane.Level.SUCCESS, SystemLogPane.levelOf("Coordinator state recovered"));
        assertEquals(SystemLogPane.Level.INFO, SystemLogPane.levelOf("Job 5 slice distributed via gRPC to Worker 4 [PRIMESUM]"));
    }

    @Test void logRendersColoredTextAndJobTagAndKeepsBoundedHistory() throws Exception {
        SwingUtilities.invokeAndWait(() -> {
            SystemLogPane pane=new SystemLogPane();
            pane.appendEvents(List.of(new ClusterMonitor.ClusterEvent(Instant.EPOCH,"Task completed [MAX]")),true);
            String text=pane.getText();
            var document=pane.getStyledDocument();
            assertEquals(SystemLogPane.Level.SUCCESS.color,StyleConstants.getForeground(
                    document.getCharacterElement(text.indexOf("Task")).getAttributes()));
            assertTrue(StyleConstants.isBold(document.getCharacterElement(text.indexOf("[MAX]")).getAttributes()));
            assertEquals(document.getLength(),pane.getCaretPosition());
            for(int i=0;i<110;i++) pane.appendEvents(List.of(new ClusterMonitor.ClusterEvent(Instant.EPOCH,"Progress " + i)),true);
            assertFalse(pane.getText().contains("Task completed"));
            assertEquals(100,pane.getText().lines().count());
            assertTrue(pane.getText().stripTrailing().endsWith("Progress 109"));
        });
    }
}
