(ns powderkeg.macros)

(defmacro compile-cond [& choices]
  (let [x (Object.)
        expr
        (reduce (fn [x [test expr]]
                  (if (eval test) (reduced expr) x))
                x (partition 2 choices))]
    (when (= x expr)
      (throw (ex-info "No valid choice." {:form &form})))
    expr))

