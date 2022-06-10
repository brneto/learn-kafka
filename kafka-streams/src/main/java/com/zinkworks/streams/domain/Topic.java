package com.zinkworks.streams.domain;

import lombok.Data;

import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Data
public class Topic {
    private final String name;
    private final Optional<Integer> partitions;
    private final Optional<Short> replicationFactor;
}
