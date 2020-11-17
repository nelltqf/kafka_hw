package com.muffledmuffin;

import org.apache.kafka.common.acl.AclOperation;

import java.util.Set;

public class ClusterInfo {

    private final String id;

    private final String nodes;

    private final Set<AclOperation> aclOperations;

    private ClusterInfo(String id, String nodes, Set<AclOperation> aclOperations) {
        this.id = id;
        this.nodes = nodes;
        this.aclOperations = aclOperations;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getId() {
        return id;
    }

    public String getNodes() {
        return nodes;
    }

    public Set<AclOperation> getAclOperations() {
        return aclOperations;
    }

    public static class Builder {

        private String id;

        private String nodes;

        private Set<AclOperation> aclOperations;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder nodes(String nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder authorizedOperations(Set<AclOperation> aclOperations) {
            this.aclOperations = aclOperations;
            return this;
        }

        public ClusterInfo build() {
            return new ClusterInfo(id, nodes, aclOperations);
        }
    }
}