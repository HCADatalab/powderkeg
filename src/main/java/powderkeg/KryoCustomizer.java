package powderkeg;

import org.apache.spark.SparkEnv;
import org.apache.spark.serializer.KryoRegistrator;

import clojure.lang.IFn;

import com.esotericsoftware.kryo.Kryo;

import static clojure.java.api.Clojure.*;

public class KryoCustomizer implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        IFn namespace = var("clojure.core", "namespace");
        IFn symbol = var("clojure.core", "symbol");
        IFn require = var("clojure.core", "require");
        require.invoke(symbol.invoke("powderkeg.kryo")); // because freeze/unfreeze helpers
        Object varsym = symbol.invoke(SparkEnv.get().conf().get("powderkeg.kryo.customizer", "powderkeg.kryo/customizer"));
        Object nssym = symbol.invoke(namespace.invoke(varsym));
        require.invoke(nssym);
        var(varsym).invoke(kryo);
    }

}
