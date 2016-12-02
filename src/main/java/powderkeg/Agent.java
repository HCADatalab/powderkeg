package powderkeg;

import java.lang.instrument.Instrumentation;

import powderkeg.Agent;

public class Agent {
    static {
        if (Agent.class.getClassLoader() != ClassLoader.getSystemClassLoader())
            throw new IllegalStateException("This class is expected to be loaded by the system classloader.");
    }
	public static Instrumentation instrumentation;
	public static void agentmain(String args, Instrumentation instrumentation) {
		Agent.instrumentation = instrumentation;
	}
}
