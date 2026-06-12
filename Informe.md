# Ejercio 1
    
## inciso a

![alt text](<DiagramaDeFlujo.jpeg>)


## inciso b

 | Transformación                                  | Abstracción |
 | ----------------------------------------------- | ----------- |
 | Archivo JSON → List[Option[Subscription]]       | No encaja   |
 | List[Option[Subscription]] → List[Subscription] | flatMap     |
 | List[Subscription] → List[Post]                 | flatMap     |
 | List[Post] → List[Post] (filtrados)             | flatMap     |
 | Archivos de entidades → List[NamedEntity]       | No encaja   |
 | List[Post] + Diccionario → List[NamedEntity]    | flatMap     |
 | List[NamedEntity] → Map[(String,String), Int]   | reduceByKey |
 | List[NamedEntity] → Map[String, Int]            | reduceByKey |

Las transformaciones clasificadas como flatMap corresponden a etapas donde cada elemento de entrada puede producir 0, 1 o más elementos de salida. Por ejemplo, una suscripción puede generar una cantidad variable de posts, un post puede generar una cantidad variable de entidades detectadas y un valor de tipo Option puede producir un elemento (si es Some) o ninguno (si es None). De manera similar, durante el filtrado de posts algunos elementos son conservados mientras que otros son descartados.

Las transformaciones clasificadas como `reduceByKey` corresponden a operaciones de agregación y conteo, donde múltiples elementos son agrupados mediante una clave y combinados para obtener estadísticas finales.En este laboratorio se utilizan para contabilizar ocurrencias de entidades, ya sea agrupándolas por (tipo, nombre) o únicamente por tipo.

Los pasos que no encajan en ninguna de las abstracciones son aquellos que operan sobre datos externos y no representan transformaciones sobre elementos del pipeline. En nuestro caso, esto ocurre cuando se lee el archivo JSON para obtener las suscripciones y cuando se leen los archivos de datos para construir el diccionario de entidades. Estas operaciones son de entrada/salida (I/O) y generan las colecciones iniciales que luego serán procesadas por el resto del pipeline.


## inciso c

Las únicas barreras de sincronización del pipeline son las etapas correspondientes a las operaciones de tipo 'reduceByKey', es decir, los conteos de entidades por '(tipo, nombre)' y por 'tipo'.

Esto se debe a que una reducción requiere combinar información producida por distintos workers. Cada worker puede procesar una parte de los datos y obtener resultados parciales, pero el resultado final sólo puede obtenerse una vez que todos los workers hayan terminado y sus resultados hayan sido agrupados y combinados.

Por el contrario, las etapas clasificadas como flatMap pueden ejecutarse de manera independiente entre workers. En estas transformaciones cada worker procesa los elementos que le fueron asignados sin necesidad de conocer los resultados producidos por otros workers. Por ejemplo, durante el filtrado de posts vacíos cada worker puede decidir localmente si un post debe conservarse o descartarse. De manera similar, en la detección de entidades cada worker puede determinar qué entidades aparecen en los posts que le fueron asignados. En ninguno de estos casos es necesario combinar resultados parciales para determinar si una salida individual es válida.

Cabe destacar que una dependencia entre etapas del pipeline no implica necesariamente una barrera de sincronización. Por ejemplo, la detección de entidades requiere disponer tanto de la lista de posts filtrados como del diccionario de entidades, pero esto representa una dependencia de datos entre etapas y no una barrera de sincronización.


## inciso d

El mecanismo de extensión de Spark (extension point) es la función que el desarrollador le proporciona a una transformación para indicar qué procesamiento debe realizarse sobre los datos. Como esta función debe ejecutarse en distintos workers, Spark necesita poder serializarla, es decir, convertirla en una secuencia de bytes para enviarla a través de la red y reconstruirla en cada worker.

Además, es recomendable que estas funciones eviten depender de variables mutables externas. Una variable mutable (var) es aquella cuyo valor puede cambiar durante la ejecución del programa. En un entorno distribuido cada worker trabaja con su propia copia de las variables utilizadas por la función, por lo que modificar una variable mutable no garantiza que los demás workers vean ese cambio. Esto puede producir resultados incorrectos o difíciles de predecir.

También conviene evitar efectos secundarios, como escribir archivos, imprimir mensajes por pantalla o modificar estructuras externas a la función. Dado que Spark puede ejecutar tareas en distintos workers o incluso reintentarlas ante fallos, estos efectos podrían producirse múltiples veces y generar comportamientos inesperados.

En general, las funciones más adecuadas para utilizar como extension points son aquellas que reciben datos de entrada, realizan un cálculo y devuelven un resultado sin depender de variables mutables externas ni producir efectos secundarios.

# Ejercicio 2

## Respondemos: Que pasaría si dejamos propagar una exepcion?

Las excepciones producidas durante la descarga de feeds o el parseo de posts se capturaron localmente y se transformaron en valores vacíos (None o List()). De esta forma un fallo no interrumpe el procesamiento de las demás subscripciones. Si las excepciones se dejaran propagar, Spark podría abortar la tarea correspondiente y cancelar parte del procesamiento, impidiendo obtener resultados del resto de los feeds válidos.

## Notas y observaciones sobre el ejercicio 2:

La adaptación del esqueleto original a Spark requirió modificar algunas operaciones sobre colecciones debido a que los datos dejaron de almacenarse en listas de Scala (List) y pasaron a representarse mediante RDDs (Resilient Distributed Datasets).

Aunque muchas transformaciones conservan una sintaxis similar (map, flatMap, filter), algunas operaciones deben expresarse de forma diferente porque los datos ya no se encuentran completamente en memoria en una única máquina. Por ejemplo, las listas permiten utilizar métodos como length, nonEmpty o count con predicados directamente, mientras que en Spark es necesario utilizar acciones como count() sobre el RDD completo o combinar transformaciones (filter) con acciones (count()) para obtener resultados equivalentes.

En general, la lógica del programa se mantuvo prácticamente igual a la del esqueleto original. Los cambios realizados estuvieron orientados principalmente a adaptar las operaciones al modelo distribuido de Spark.


# Decisiones de diseño

En este apartado vamos a desarrollar sobre las desiciones de diseño tomadas, intentando justificar cada una de ellas de la mejor manera.

1) En el ejercicio 2, para implementar la descarga paralela de feeds se consideraron dos alternativas. La primera consistía en aplicar directamente un `flatMap` sobre el `RDD[Subscription]` para obtener un `RDD[Post]`, este es el pipeline que marcaba la consigna (`RDD[Subscription]`--flatmap-->`RDD[Post]`). Sin embargo, esta solución descartaba información necesaria para calcular posteriormente las estadísticas solicitadas en el inciso c, como la cantidad de feeds descargados exitosamente y la cantidad de fallos.

Por este motivo se decidió conservar una estructura intermedia de tipo `RDD[(Boolean, List[Post])]`, donde el valor booleano indica si la descarga del feed fue exitosa y la lista contiene los posts obtenidos. Esta estructura se genera mediante una transformación `map`, ya que cada suscripción produce exactamente un resultado de salida.

A partir de esta estructura intermedia se obtienen las estadísticas requeridas y posteriormente se aplica un `flatMap` sobre las listas de posts para construir el `RDD[Post]` utilizado por el resto del pipeline. De esta manera se preserva toda la información necesaria para el análisis posterior. Finalmente el pipeline podria verse como:
(`RDD[Subscription]`-map->`RDD[(Boolean, List[Post])]`-flatMap->`RDD[Post]`) y en definitiva llegamos a lo mismo con una transformacion más de por medio a cambio de aprovechar al maximo el esqueleto provisto por la catedra.

Esta decisión reduce la duplicación de trabajo y permite reutilizar gran parte del cálculo de estadísticas existente.

2) En el ejercicio 2 se reutilizó el mecanismo de manejo de errores ya existente en JsonParser.parsePosts, modificando únicamente el mensaje reportado y agregando la URL de la suscripción como parámetro. De esta forma se evitó duplicar lógica en el Main y se mantuvo el tratamiento de errores asociado al parseo dentro del módulo de parseo.

Esto implicó un cambio mínimo en el main el cual fue pasar tambén la URL como parametro al llamado de la función parsePosts, precisamente en el momento en que se descargan los feeds y se parsean los posts.

# Ejercicio 3

1) La operación reduceByKey en Spark actúa como una barrera de sincronización porque requiere que todos los datos con la misma clave se agrupen antes de poder aplicar la función de reducción.
En el cluster ocurre lo siguiente:
Primero, cada worker procesa sus particiones localmente. Luego, se realiza un shuffle(que es el proceso de redistribuir, agrupar y mover los datos a través de la red entre los diferentes nodos (workers) del clúster), donde los datos se redistribuyen entre nodos de forma que todas las tuplas con la misma clave queden en la misma partición. Durante este shuffle, los nodos deben intercambiar datos entre sí a través de la red. Ninguna tarea de reducción puede completarse hasta que todos los datos correspondientes a cada clave hayan llegado.

Esto implica una sincronización global parcial: el sistema debe esperar a que termine el movimiento de datos antes de continuar con la reducción.
Esta barrera es inevitable en este problema porque necesitamos contar ocurrencias por clave. Para poder sumar correctamente los valores asociados a una clave, es necesario que todos los valores estén en el mismo lugar, lo cual requiere el shuffle.

2) La función que se pasa a reduceByKey debe cumplir ciertas propiedades:
Asociatividad:
El orden en que se agrupan las operaciones no debe afectar el resultado.
Ejemplo: (a + b) + c = a + (b + c)
Conmutatividad:
El orden de los operandos no debe afectar el resultado.
Ejemplo: a + b = b + a

Estas propiedades son necesarias porque Spark ejecuta la reducción en paralelo y en distintos nodos. Los datos pueden combinarse en distinto orden dependiendo de cómo se distribuyan. También puede haber combinaciones parciales antes del shuffle.
Si la función no cumple estas propiedades, el resultado podría ser incorrecto o no determinístico. En este trabajo, se utiliza la suma (_ + _), que cumple ambas propiedades, por lo que es segura.

3) El diccionario de entidades se carga inicialmente en el driver (por ejemplo, desde un archivo o una estructura en memoria). Luego, cuando se utiliza dentro de una transformación como flatMap, Spark lo envía a los workers mediante un mecanismo de serialización, es decir, el diccionario se define en el driver, se serializa y distribuye a cada worker que lo necesite y cada worker trabaja con su propia copia local del diccionario.
Esto puede implicar un costo si el diccionario es grande, ya que se envía a cada nodo.

# Ejercicio 4

![alt text](<ej4_stats_screenshot.png>)

## Respuestas:
**¿Por qué los Accumulators solo deben usarse para métricas y no para tomar decisiones lógicas dentro de las etapas distribuidas del pipeline? ¿En qué situación un Accumulator puede dar un valor incorrecto?**

Porque el valor de un acumulador que lee un worker es una copia local y no refleja el estado global. Por lo tanto, una decisión basada en ese valor dentro de una transformación sería impredecible.

Un acumulador puede dar un valor incorrecto en la re-ejecución de tareas, si un nodo falla.

**¿En qué momento del pipeline está disponible el valor de un Accumulator para ser leído por el driver?**

Después de que una acción de Spark ha finalizado. Las transformaciones son perezosas y solo definen un plan de ejecución. Es la acción (`count()`, `collect()`, etc.) la que dispara la ejecución del plan. 

Por ejemplo, en nuestro código el valor de los acumuladores es confiable solo después de la línea 

```
Main.s
(...) 
70 val countedFilteredPosts = filteredPostsRDD.count()
(...) 
``` 

ya que `count()` es la primera acción que fuerza su cómputo.
