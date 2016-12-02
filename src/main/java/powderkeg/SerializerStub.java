package powderkeg;

import clojure.lang.IFn;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SerializerStub<T> extends Serializer<T> {

    private final IFn readfn;
    private final IFn writefn;
    
    public SerializerStub(IFn readfn, IFn writefn) {
        super();
        this.readfn = readfn;
        this.writefn = writefn;
    }

    public void write(Kryo kryo, Output output, T object) {
        writefn.invoke(this, kryo, output, object);
    }

    @SuppressWarnings("unchecked")
    public T read(Kryo kryo, Input input, Class<T> type) {
        return (T) readfn.invoke(this, kryo, input, type);
    }

}
