
use jni::*;
use jni::objects::{JValue, JString};
use jni::strings::JavaStr;
use jni::sys::jint;

pub fn start_jvm() {
    let args = InitArgsBuilder::new()
        .version(JNIVersion::V8)
        .option("-Djava.class.path=/home/sammy/icebridge/jars/app-uber.jar")
        .build().unwrap();
    let vm = JavaVM::new(args).unwrap();
    let mut env = vm.attach_current_thread().unwrap();
    log::warn!("jvm version: {:?}", env.get_version().unwrap());

    let x = JValue::from(-10);
    let val: jint = env.call_static_method("java/lang/Math", "abs", "(I)I", &[x]).unwrap()
      .i().unwrap();
    
    log::warn!("result value: {:?}", val);
    let hadoop_configuration = env.find_class("org/apache/hadoop/conf/Configuration").unwrap();
    let conf = env.new_object(hadoop_configuration, "()V", &[]).unwrap();

    let hadoop_catalog = env.find_class("org/apache/iceberg/hadoop/HadoopCatalog").unwrap();
    let path = env.new_string("/home/sammy/iceberg").unwrap();
    let catalog = env.new_object(hadoop_catalog, "(Lorg/apache/hadoop/conf/Configuration;Ljava/lang/String;)V", &[JValue::from(&conf), JValue::from(&path)]).unwrap();
    let val = env.call_method(&catalog, "toString", "()Ljava/lang/String;",&[]).unwrap();
    let val = val.l().unwrap().into();
    let val = env.get_string(&val).unwrap();
    log::warn!("result value: {:?}", val.to_str());
}