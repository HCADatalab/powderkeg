(ns ^:powderkeg/no-sync powderkeg.ouroboros
  "The JVM that instruments itself."
  (:require [clojure.java.io :as io]))

(def ^:private no-changes {:classes (sorted-map) :vars #{}
                           :all-classes (sorted-map) :all-vars #{}})

(def ^:private repl-changes (atom no-changes))

(defn- snapshot-vars
  "Returns a map of symbols to [meta value]"
  [ns+syms]
  (reduce
    (fn [res [ns sym]]
      (let [v (ns-resolve ns sym)
            m (meta v)]
        (cond-> res
          (not (or (:macro m) (:powderkeg/no-sync m)))
          (assoc-in [(symbol (ns-name ns)) sym] [(dissoc m :ns :name) @v]))))
    {}
    ns+syms))

(defn latest-changes!
  "Retrieve changes (classes, vars) that happened since last call."
  []
  (loop []
    (let [{:keys [vars classes all-vars all-classes] :as changes} @repl-changes]
      (if (compare-and-set! repl-changes changes 
            (assoc no-changes
              :all-vars (into all-vars vars)
              :all-classes (into all-classes classes)))
        {:vars (snapshot-vars vars) :classes classes} 
        (recur)))))

(defn reboot!
  []
  (loop []
    (let [{:keys [vars classes all-vars all-classes] :as changes} @repl-changes]
      (or (compare-and-set! repl-changes changes 
            (assoc no-changes
              :vars (into all-vars vars)
              :classes (into all-classes classes))) 
        (recur)))))

(def no-transform
  "ClassFileTransfomer which does no transform but give access to bytecode."
  (reify java.io.Serializable
    java.lang.instrument.ClassFileTransformer
    (transform [_ loader classname class domain bytes]
      (when (instance? clojure.lang.DynamicClassLoader loader)
        (swap! repl-changes update :classes assoc classname (aclone bytes)))
      nil)))

(defn log-var-change [ns sym]
  (swap! repl-changes update :vars conj [ns sym]))

;; modify the bytecode of clojure.lang.Var to log var changes
(defn watch-vars [bytes]
  (let [rdr (clojure.asm.ClassReader. bytes)
        cw (clojure.asm.ClassWriter. 0)
        class-visitor
        (proxy [clojure.asm.ClassVisitor] [clojure.asm.Opcodes/ASM4 cw]
          (visitMethod [access method-name mdesc sig exs]
            (let [mv (.visitMethod cw access method-name mdesc sig exs)]
              (proxy [clojure.asm.MethodVisitor] [clojure.asm.Opcodes/ASM4 mv]
                (visitFieldInsn [opcode owner name desc]
                  ; emit original store
                  (.visitFieldInsn mv opcode owner name desc)
                  (when (= [clojure.asm.Opcodes/PUTSTATIC owner name]
                          [opcode "clojure/lang/Var" "rev"])
                    (cond 
                      (= [method-name mdesc] ["<init>" "(Lclojure/lang/Namespace;Lclojure/lang/Symbol;Ljava/lang/Object;)V"])
                      (doto mv
                        ; get the var
                        (.visitLdcInsn "powderkeg.ouroboros")
                        (.visitLdcInsn "log-var-change")
                        (.visitMethodInsn clojure.asm.Opcodes/INVOKESTATIC "clojure/java/api/Clojure" "var" "(Ljava/lang/Object;Ljava/lang/Object;)Lclojure/lang/IFn;")
                        ; push args
                        (.visitVarInsn clojure.asm.Opcodes/ALOAD 1)
                        (.visitVarInsn clojure.asm.Opcodes/ALOAD 2)                        ; call fn
                        (.visitMethodInsn clojure.asm.Opcodes/INVOKEINTERFACE "clojure/lang/IFn" "invoke" "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;")
                        ; discard return value
                        (.visitInsn clojure.asm.Opcodes/POP))  
                      (= "<init>" method-name) (throw (ex-info "Unexpected constructor" {:desc mdesc}))
                      (zero? (bit-and clojure.asm.Opcodes/ACC_STATIC access))
                      (doto mv
                        ; get the var
                        (.visitLdcInsn "powderkeg.ouroboros")
                        (.visitLdcInsn "log-var-change")
                        (.visitMethodInsn clojure.asm.Opcodes/INVOKESTATIC "clojure/java/api/Clojure" "var" "(Ljava/lang/Object;Ljava/lang/Object;)Lclojure/lang/IFn;")
                        ; push args
                        (.visitVarInsn clojure.asm.Opcodes/ALOAD 0)
                        (.visitFieldInsn clojure.asm.Opcodes/GETFIELD "clojure/lang/Var" "ns" "Lclojure/lang/Namespace;")
                        (.visitVarInsn clojure.asm.Opcodes/ALOAD 0)
                        (.visitFieldInsn clojure.asm.Opcodes/GETFIELD "clojure/lang/Var" "sym" "Lclojure/lang/Symbol;")
                        ; call fn
                        (.visitMethodInsn clojure.asm.Opcodes/INVOKEINTERFACE "clojure/lang/IFn" "invoke" "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;")
                        ; discard return value
                        (.visitInsn clojure.asm.Opcodes/POP))
                      (= "<clinit>" method-name) nil
                      :else (throw (ex-info "Unexpected static method" {:name method-name :desc mdesc})))))))))]
    (.accept rdr class-visitor 0)
    (.toByteArray cw)))

(def var-transform
  "ClassFileTransfomer which instruments clojure.lang.Var."
  (reify java.io.Serializable
    java.lang.instrument.ClassFileTransformer
    (transform [_ loader classname class domain bytes]
      (when (= "clojure/lang/Var" classname)
        (watch-vars bytes)))))

(defn ^java.util.jar.Manifest manifest
  "Creates an MANIFEST.MF out of a map"
  [manifest-map]
  (let [man (java.util.jar.Manifest.)]
    (reduce-kv (fn [^java.util.jar.Attributes attrs k v]
                 (doto attrs (.put (java.util.jar.Attributes$Name. (name k)) (str v))))
      (doto (.getMainAttributes man)
        (.put java.util.jar.Attributes$Name/MANIFEST_VERSION "1.0")) manifest-map)
    man))

(defn jar! 
  "Writes a jar to out. Only main attributes are supported."
  [out manifest-map entries]
  (let [entries-map (into (sorted-map) entries)]
    (with-open [out (io/output-stream out)
                jar (java.util.jar.JarOutputStream. out (manifest manifest-map))]
      (letfn [(^String emit-dirs [^String dir ^String path]
                (if-not (.startsWith path dir)
                  (recur (subs dir 0 (inc (.lastIndexOf dir "/" (- (count dir) 2)))) path)
                  (let [i (.indexOf path "/" (count dir))]
                    (if (neg? i)
                      dir
                      (let [dir (subs path 0 (inc i))]
                        (.putNextEntry jar (java.util.jar.JarEntry. dir))
                        (recur dir path))))))]
        (reduce-kv (fn [^String dir ^String path data]
                     (let [dir (emit-dirs dir path)]
                       (.putNextEntry jar (java.util.jar.JarEntry. path))
                       (io/copy data jar)
                       dir)) "" entries-map)))))

(defn tmp-file [prefix suffix & {:keys [delete-on-exit] :or {delete-on-exit true}}]
  (let [f (java.io.File/createTempFile prefix suffix)]
    (when delete-on-exit (.deleteOnExit f))
    f))


(when-not *compile-files*
  (binding [*out* *err*]
    (println "Preparing for self instrumentation.")
    (let [loader (.getContextClassLoader (java.lang.Thread/currentThread))
          tools-jar (java.io.File. (System/getProperty "java.home") "../lib/tools.jar")
          tools-jar-loader (java.net.URLClassLoader. (into-array [(.toURL tools-jar)]))
          [_ pid] (re-matches #"([^@]*).*" (.getName (java.lang.management.ManagementFactory/getRuntimeMXBean)))
          vm (-> (Class/forName "com.sun.tools.attach.VirtualMachine" true tools-jar-loader)
               (.getMethod "attach" (into-array [String]))
               (.invoke nil (object-array [pid])))
          f (tmp-file "powderkeg-agent-" ".jar")]
      (with-open [bytes-in (.getResourceAsStream loader "powderkeg/Agent.class")
                  out (io/output-stream f)]
        (jar! out
          {:Agent-Class "powderkeg.Agent"
           :Can-Redefine-Classes true
           :Can-Retransform-Classes true}
          {"powderkeg/Agent.class" bytes-in}))
      (.loadAgent vm (.getAbsolutePath f))
      (println "Ouroboros succesfully eating its own tail!"))))

;; DO NOT MERGE WITH THE ABOVE BLOCK AS IT MODIFIES THE CLASSPATH AND THE NEXT BLOCK NEEDS TO
;; SEE THE MODIFICATIONS.

(when-not *compile-files*
  (binding [*out* *err*]
    (println "Counting classes.")
     (let [all-classes (.getAllLoadedClasses powderkeg.Agent/instrumentation)
           all-dyn-classes (into-array Class (filter #(instance? clojure.lang.DynamicClassLoader (.getClassLoader %)) all-classes))]
       (print "Retrieving bytecode of" (count all-dyn-classes) "classes dynamically defined by Clojure (out of" (count all-classes) "classes)... ")
       (.addTransformer powderkeg.Agent/instrumentation no-transform true)
       (.retransformClasses powderkeg.Agent/instrumentation all-dyn-classes)
       (println "done!"))
    
     (print "Instrumenting clojure.lang.Var... ")
     (.addTransformer powderkeg.Agent/instrumentation var-transform true)
     (.retransformClasses powderkeg.Agent/instrumentation (into-array [clojure.lang.Var]))
     (println "done!")))
