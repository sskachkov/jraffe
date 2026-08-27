package io.github.sskachkov.jraffe.kvstore;

public sealed interface Request permits CVASRequest, GetRequest, SetRequest, UnknownRequest {}
