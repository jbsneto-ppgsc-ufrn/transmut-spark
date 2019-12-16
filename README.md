# Spark Mutation Testing Tool


### Notes
1. References (parameters, variables and values) must have unique names;
2. Programs to be mutated must be encapsulated in methods;
3. All RDDs must have their own reference (must be declared as a parameter, variable or value);
4. Only one transformation must be called in a statement:
	* For example: `val rdd2 = rdd.filter( (a: String) => !a.isEmpty )`
4. Anonymous (lambda) functions must have their input parameters explicitly typed:
	* Incorrect: `rdd.map( a => a * a )`
	* Correct: `rdd.map( (a: Int) => a * a )`


### Supported Transformations

| Transformation | Interface                                                |
|----------------|----------------------------------------------------------|
| filter         | `filter(f: (T) ⇒ Boolean): RDD[T]`                         |
| distinct       | `distinct(): RDD[T]`                                       |
| sortBy         | `sortBy[K](f: (T) ⇒ K, ascending: Boolean = true): RDD[T]` |
| sortByKey      | `sortByKey(ascending: Boolean = true): RDD[(K, V)]`        |
| union          | `union(other: RDD[T]): RDD[T]`                             |
| intersection   | `intersection(other: RDD[T]): RDD[T]`                      |
| subtract       | `subtract(other: RDD[T]): RDD[T]`                          |
| reduceByKey    | `reduceByKey(func: (V, V) ⇒ V): RDD[(K, V)]`               |
| combineByKey* (mergeCombiners)   | `combineByKey[C](createCombiner: (V) ⇒ C, mergeValue: (C, V) ⇒ C, mergeCombiners: (C, C) ⇒ C): RDD[(K, C)]`                                                         |

<!--| aggregateByKey* (combOp) | `aggregateByKey[U](zeroValue: U)(seqOp: (U, V) ⇒ U, combOp: (U, U) ⇒ U): RDD[(K, U)]` |
-->
\* \- Only the parameter in parentheses is mutated, the rest remains the same.