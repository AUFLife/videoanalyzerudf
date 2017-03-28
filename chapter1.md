视频分析项目UDF化：

研究：
	* Maven配置：
	* Java与Scala类型转换：
	* 配置文件读取：


### 1. Maven配置
#### 1.1 问题分析
此次利用Hive的自定义函数实现视频分析采用的是Scala源码中的方法，因为Scala的数据类型和Java的数据类型存在差距，如果将Java的数据类型转化为Scala的数据类型，并成功调用Scala中定义的函数，成为一个问题
#### 1.2 解决方案
通过搜寻资料，了解到Java与Scala之间数据类型的差异，并知道JavaConversion对象提供了用于在Scala和Java集合之间来回转换的一组方法。我们可以通过引入JavaConversion和JavaConverters._

##### 1.2.1 第一种方法，引入collection.JavaCollections._完成双向自动转型

collection.JavaCoversions 是 Scala 2.8 开始加入的，它的定义是

```scala
object `JavaCoversions` extends `WrapAsScala` with `WrapAsJava`
``` 

##### 1.2.1.1 双向转换
```scala
首先引入 import collection.JavaConversions._完成双向自动转型
scala.collection.Iterable <=> java.lang.Iterable
scala.collection.Iterable <=> java.util.Collection
scala.collection.Iterator <=> java.util.{Iterator, Enumeration}
scala.collection.mutable.Buffer <=> java.util.List
scala.collection.mutable.Set <=> java.util.Set
scala.collection.mutable.Map <=> java.util.{Map, Dictionary}
scala.collection.mutable.ConcurrentMap <=> java.util.concurrent.ConcurrentMap
```

##### 1.2.1.2 单向转换
```
scala.collection.Seq => java.util.List
scala.collection.mutable.Sqp => java.util.List
scala.collection.Set => java.util.Set
scala.collection.Map => java.util.Map
java.util.Properties => scala.collection.mutable.Map[String, String]
```

#### 1.2.2 第二种方法，引入collection.JavaConverters._进而显示调用saJava()或asScala()方法完成转型

``` scala
import collection.JavaConverters._

 val order: java.util.List[String] = Seq("101", "102").asJava
 val name: Seq[String] = new java.util.ArrayList[String]().asScala
```


##### 1.2.3 案例
在Scala中调用Java可以直接调用，这里讨论下在Java中调用Scala：
在Java中使用Scala类时要考虑的四个要点 
  * 类参数
  * 类常量
  * 类变量
  * 异常

特质：

单例对象：
单例对象是Scala实现静态方法/单例模式的方式。一个Scala单例对象会被编译成由"$"结尾的类，例子如下：
class TestSingleton(name: String) {
  def name = name
}
object TestSingleton {
  def apply = new TestSingleton("foo")
}
因此我们可以这样在Java中访问：
String name = TestSingleton$.MODULE$.apply("foo")
说明：TestSingleton$意为Scala中的Object对象
      MODULE$意为Scala对象中的静态字段

---

* java的iterator转换为scala的iterator类型

```
scala.collection.Iterator<Event> iterTest = JavaConverters.asScalaIteratorConverter(iterator).asJava();
```

* java的Map转换为scala的immytable.Map类型
  scala.collection.mutable.Map<String, String> mapTest = JavaConverters.mapAsScalaMapConverter(map).asScala();

```
// 首先要将java.util.Map转为collection.mutable.map
scala.collection.mutable.Map<String, String> mapTest = JavaConverters.mapAsScalaMapConver(map).asScala();
// 生成一个newBuilder类用于构造一个新的Map，并将元Map中的数据一一添加
Object object = Map$.MODULE$.<String, String> newBuilder().$plus$plus$eq(mapTest.toSeq());
Object resultTest = ((scala.collection.mutable.Builder) object).result();
scala.collection.immutable.Map<String, String> reusltTest2 = (scala.collection.immutable.Map) resultTest;

```
或者
```

```


* java list 转换为scala Seq

```
--利用了java list到scala buffer双向转换，再转换为scala的Seq类型
List<Event> eList = KeyEventMap.get(key);
scala.collection.mutable.Buffer<Event> buffer = JavaConverters.asScalaBufferConverter(eList).asScala();
scala.collection.Seq<Event> seq = buffer;
```

3. 配置文件读取

视频项目的目录结构比较特别，是在/configuration目录下，有公共的icp.json用于设置端，每个端有各自的文件夹目录。
配置文件读取方式分为两种：
### 将配置文件打入Jar包，读取Jar包中配置文件
```
	// 1 
	String path = Resource.class.getProtectionDomain().getCodeSource().getLocation().getFile(); // 当前类编译后目录，/D:/Spark/hive/AnalyzerVideoByHive/target/classes/

	// 2
	JarFile jf = new jf.entries();					// 返回所有entries的枚举集合(Enumeration)
	Enumeration enu = jf.entries();					// 遍历这个枚举集合
	while(enu.hasMoreElements()) {
		JarEntry element = (JarEntry) enu.nextElement();
		String name = element.getName()/			// 得到文件路径
		boolean result = Pattern.matches(source + "/*" + name)	// 匹配以Source开头的文件或目录
		if (result) {
			InputStream is = this.getClass().getClassLoader().getResourceAsStream(name);
			String context = IOUtils.toString(is);	//获取内容
			if ("".equals(context) && context != null) {
				map.put(name, context);				// （路径，内容）
			}
		}
	}


```
也可以写为：
```
	String path = Resource.class.getResource("/resource");	// 等于第1步，/D:/Spark/hive/AnalyzerVideoByHive/target/classes/resource
```

---

### 将配置文件放入hdfs上，从hdfs上读取

// 1 读取本地文件
```

  String path = " ";
        File file = new File(filepath);
        String[] list = file.list();
        File[] files = file.listFiles();

        `for (File f : files) {
            System.out.println(f.getPath());
            String s = f.getPath();
            if (!f.isDirectory()) {
                String content = IOUtils.toString(new FileInputStream(s));
                map.put(s, content);
            } else {
                readfile1(s, map);
            }
        }
        `
        System.out.println(map);
        return map;

```

// 2 读取HDFS文件
```
	
	// 遍历目录下的所有文件
        FSDataInputStream inputStream = null;
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(filePath));
            for (FileStatus file : status) {
                if (!file.isDirectory()) {
                    inputStream = fs.open(file.getPath());
                    String content = IOUtils.toString(inputStream);
                    map.put(file.getPath().toString(), content);
                } else {
                    FileStatus[] files = fs.listStatus(file.getPath());
                    for (FileStatus fis : files) {
                        if (!fis.isDirectory()) {
                            inputStream = fs.open(fis.getPath());
                            String content = IOUtils.toString(inputStream);
                            map.put(fis.getPath().toString(), content);
                        } else {
                            readHdfsFile(fis.getPath().toString(), map);
                        }
                    }
                }

            }
        } catch (IOException e) {
```









---

步骤：
1. 事件识别与生成
2. 事件表修复
3. 过程表生成
4. 服务器表生成
5. UDF与UDAF实现

