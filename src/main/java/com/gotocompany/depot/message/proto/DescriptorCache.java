package com.gotocompany.depot.message.proto;

import com.google.protobuf.Descriptors;

import java.util.Map;

/**
 * Resolves Protobuf {@link Descriptors.Descriptor} instances from descriptor maps, transparently
 * falling back to a type-name alias lookup when a descriptor is not registered directly under the
 * requested name.
 *
 * <p>The Stencil client exposes descriptors keyed by their Java/proto class name and, via a
 * companion alias map, by their fully qualified Protobuf type name. This helper hides that two-step
 * resolution so callers such as {@link ProtoFieldParser} can request a descriptor using either
 * identifier.</p>
 *
 * <p>Instances are stateless and therefore safe to share between threads.</p>
 */
public class DescriptorCache {
    /**
     * Returns the descriptor registered for the supplied proto name, resolving aliases when needed.
     *
     * <p>The lookup proceeds in two steps:</p>
     * <ul>
     *   <li>If {@code allDescriptors} contains an entry keyed directly by {@code protoName}, that
     *       descriptor is returned.</li>
     *   <li>Otherwise {@code protoName} is translated through {@code typeNameToPackageNameMap} into
     *       the key under which the descriptor is registered, and the descriptor stored under that
     *       key is returned.</li>
     * </ul>
     *
     * @param allDescriptors map of all known descriptors keyed by class or package name as supplied
     *     by the Stencil client
     * @param typeNameToPackageNameMap alias map translating a fully qualified Protobuf type name to
     *     the descriptor key under which it is registered
     * @param protoName the descriptor identifier to resolve, either a direct key or a fully qualified
     *     type name present in the alias map
     * @return the matching {@link Descriptors.Descriptor}, or {@code null} if the name is neither a
     *     known key nor a known alias
     */
    public Descriptors.Descriptor fetch(Map<String, Descriptors.Descriptor> allDescriptors, Map<String, String> typeNameToPackageNameMap, String protoName) {
        if (allDescriptors.get(protoName) != null) {
            return allDescriptors.get(protoName);
        }
        String packageName = typeNameToPackageNameMap.get(protoName);
        if (packageName == null) {
            return null;
        }
        return allDescriptors.get(packageName);
    }
}
