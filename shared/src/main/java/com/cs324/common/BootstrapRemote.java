package com.cs324.common;

/**
 * Compatibility alias for the former directory interface name.
 *
 * @deprecated use {@link ClusterDirectory}; this interface is local Java only and
 *             has never required RMI since the gRPC migration.
 */
@Deprecated
public interface BootstrapRemote extends ClusterDirectory {
}
