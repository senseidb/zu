thrift  --gen java -o src/test src/test/thrift/zu-finagle-test.thrift
cp -R src/test/gen-java/* src/test/java
rm -rf src/test/gen-java
