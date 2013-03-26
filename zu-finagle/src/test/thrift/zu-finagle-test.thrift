namespace java zu.finagle.test

struct TestReq{
  1: optional string s
}

struct TestResp{
  1: required i32 len
}
