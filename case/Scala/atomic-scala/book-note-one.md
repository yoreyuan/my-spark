Atomic Scala Learn Programming in a Languager of the Future Second Edition
===
[示例和习题](https://github.com/AtomicScala)


# 运行Scala
**REPL**(Read-Evaluate-Print-Loop)：
退出REPL  :quit

- - - - 

# 类和对象
在面向对象编程语言中，我们会考虑**待解决问题中的"名词"**，并将这些名词转译为保存数据和执行行动的对象。
面向对象语言面向的就是创建和使用对象。

Scala不仅是面向对象语言，它还是函数式语言。
在函数式语言中，我们会考虑"动词"，**即希望执行的动作**，并且通常会将这些动作描述成数学上的等式。

### Scala的面向对象
对象包含存储数据用的val和var（称为域），并且使用该方法来执行操作。
类定义了域和方法，他们使得类在本质上就是用户定义的新数据类型。构建某个类的val或var称为**创建对象或创建实例**。

在Scala的REPL中输入`val r = Range(0,10).` 敲下`Tab`键可看到：
```
scala> val r = Range(0,10).
++            by              count       find              hashCode       isEmpty              lift               partition           reverse           sliding        tails             toMap           view           
++:           canEqual        diff        flatMap           head           isInclusive          map                patch               reverseIterator   sortBy         take              toSeq           withFilter     
+:            collect         distinct    flatten           headOption     isTraversableAgain   max                permutations        reverseMap        sortWith       takeRight         toSet           zip            
/:            collectFirst    drop        fold              inclusive      iterator             maxBy              prefixLength        runWith           sorted         takeWhile         toStream        zipAll         
:+            combinations    dropRight   foldLeft          indexOf        last                 min                product             sameElements      span           terminalElement   toString        zipWithIndex   
:\            companion       dropWhile   foldRight         indexOfSlice   lastElement          minBy              reduce              scan              splitAt        to                toTraversable                  
WithFilter    compose         end         forall            indexWhere     lastIndexOf          mkString           reduceLeft          scanLeft          start          toArray           toVector                       
addString     contains        endsWith    foreach           indices        lastIndexOfSlice     nonEmpty           reduceLeftOption    scanRight         startsWith     toBuffer          transpose                      
aggregate     containsSlice   equals      genericBuilder    init           lastIndexWhere       numRangeElements   reduceOption        segmentLength     step           toIndexedSeq      union                          
andThen       copyToArray     exists      groupBy           inits          lastOpton           orElse             reduceRight         seq               stringPrefix   toIterable        unzip                          
apply         copyToBuffer    filter      grouped           intersect      length               padTo              reduceRightOption   size              sum            toIterator        unzip3                         
applyOrElse   corresponds     filterNot   hasDefiniteSize   isDefinedAt    lengthCompare        par                repr                slice             tail           toList            updated                        

scala> val r = Range(0,10).

```

### Scala Doc
[api 文档](https://www.scala-lang.org/api/2.11.8/index.html#package)

自带方法
```bash
## 获取hashCode，以十进制显示。

```

### 导入和包
重新制定名字，例：  `import scala.util.{Random => R}`

### Vector
Vector是一个容器(集合)，即保存其他对象的对象。

许多其他编程预压会强制你做额外的工作，这令人颇为不快，因为在你看来，编程语言可以解决这个问题，但他们像是为了泄愤而故意让你做这些额外的工作。
正式处于这个原因以及许多其他的原因，熟悉其他语言的程序员会发现Scala就像一缕清风，殷勤地询问"有什么可以为你效劳吗？"而从来不想其他语言对待牲口般地挥舞着皮鞭强迫你去钻圈。😂


- - - - 
 
 # 模式匹配








