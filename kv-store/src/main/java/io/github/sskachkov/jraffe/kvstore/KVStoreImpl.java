package io.github.sskachkov.jraffe.kvstore;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KVStoreImpl implements KVStore {
    private final Map<ByteBuffer, byte[]> data = new HashMap<>();

    @Override
    public byte[] apply(byte[] command) {
        Request request = decodeCommand(command);
        switch (request) {
            case GetRequest req -> {
                GetResponse response = get(req);
                return GetCommandCodec.encodeResponse(response);
            }
            case SetRequest req -> {
                SetResponse set = set(req);
                return SetCommandCodec.encodeResponse(set);
            }
            case CVASRequest req -> {
                CVASResponse cvas = cvas(req);
                return CVASCommandCodec.encodeResponse(cvas);
            }
            case UnknownRequest req -> {
                return ErrorResponseCodec.encodeResponse(new ErrorResponse("unknown command: " + req.opcode()));
            }
        }
    }

    private static Request decodeCommand(byte[] command) {
        if (command.length == 0) {
            return new UnknownRequest(Opcodes.ERROR);
        }
        return switch (command[0]) {
            case Opcodes.GET -> GetCommandCodec.decodeRequest(command);
            case Opcodes.SET -> SetCommandCodec.decodeRequest(command);
            case Opcodes.CVAS -> CVASCommandCodec.decodeRequest(command);
            default -> new UnknownRequest(command[0]);
        };
    }

    private synchronized CVASResponse cvas(CVASRequest request) {
        ByteBuffer key = ByteBuffer.wrap(request.key());
        if (!data.containsKey(key)) {
            return CVASResponse.keyNotFound();
        }
        byte[] currentValue = data.get(key);

        if (!Arrays.equals(currentValue, request.fromValue())) {
            return CVASResponse.valueMismatch(currentValue);
        }
        this.data.put(key, request.toValue());
        return CVASResponse.success();
    }

    private synchronized GetResponse get(GetRequest request) {
        byte[] value = data.get(ByteBuffer.wrap(request.key()));
        return new GetResponse(value != null, value != null ? value : new byte[0]);
    }

    private synchronized SetResponse set(SetRequest request) {
        data.put(ByteBuffer.wrap(request.key()), request.value());
        return new SetResponse();
    }
}
