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
