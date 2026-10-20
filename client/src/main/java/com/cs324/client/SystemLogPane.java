package com.cs324.client;

import java.awt.Color;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Locale;
import javax.swing.JTextPane;
import javax.swing.text.*;

// Bounded, selectable system log; rendered as styled text rather than HTML.
final class SystemLogPane extends JTextPane {
    enum Level {
        SUCCESS(Color.BLACK), INFO(Color.BLACK),
        WARNING(Color.BLACK), ERROR(new Color(196, 37, 59));
        final Color color;
        Level(Color color) { this.color=color; }
    }
    private final ArrayDeque<ClusterMonitor.ClusterEvent> entries = new ArrayDeque<>();
    private static final DateTimeFormatter CLOCK=DateTimeFormatter.ofPattern("HH:mm:ss");

    static Level levelOf(String text) {
        String line=text.toLowerCase(Locale.ROOT);
        if (line.contains("failed") || line.contains("failure") || line.contains("unreachable")
                || line.contains("disconnected") || line.contains("error") || line.contains("coordinator lost")) return Level.ERROR;
        if (line.contains("completed") || line.contains("recovered") || line.contains("joined")
                || line.contains("registered") || line.contains("registration") || line.contains("reachable again")) return Level.SUCCESS;
        if (line.contains("election") || line.contains("elected") || line.contains("coordinator announced")
                || line.contains("announced coordinator") || line.contains("adopted coordinator")
                || line.contains("warning") || line.contains("recover") || line.contains("reassign")) return Level.WARNING;
        return Level.INFO;
    }

    void clearLog() {
        entries.clear();
        setText("");
    }

    void appendEvents(List<ClusterMonitor.ClusterEvent> events, boolean follow) {
        if (events.isEmpty()) return;
        entries.addAll(events);
        while (entries.size()>100) entries.removeFirst();
        int caret=getCaretPosition();
        StyledDocument document=getStyledDocument();
        try {
            document.remove(0,document.getLength());
            for (var event:entries) {
                insert(document, "[" + CLOCK.format(event.time().atZone(ZoneId.systemDefault())) + "] ", ClientStyle.MUTED, false, null);
                Color color=levelOf(event.text()).color;
                insert(document, "\u25cf ", color, true, null);
                String text=event.text();
                int tag=text.lastIndexOf(" [");
                if (tag>=0 && text.substring(tag).matches(" \\[(MAX|PRIMESUM|PRIMECOUNT)\\]")) {
                    insert(document,text.substring(0,tag)+" ",color,false,null);
                    insert(document,text.substring(tag+1),color,true,ClientStyle.SURFACE);
                } else insert(document,text,color,false,null);
                insert(document,"\n",color,false,null);
            }
            setCaretPosition(follow?document.getLength():Math.min(caret,document.getLength()));
        } catch (BadLocationException impossible) { throw new IllegalStateException(impossible); }
    }
    private static void insert(StyledDocument doc,String text,Color color,boolean bold,Color background) throws BadLocationException {
        SimpleAttributeSet style=new SimpleAttributeSet();
        StyleConstants.setForeground(style,color);
        StyleConstants.setBold(style,bold);
        if(background!=null) StyleConstants.setBackground(style,background);
        doc.insertString(doc.getLength(),text,style);
    }
}
