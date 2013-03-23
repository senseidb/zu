namespace java com.senseidb.zu.finagle.rpc

struct ZuTransport{
  1: required string name
  2: optional binary data
}

service ZuThriftService {
  ZuTransport send(1: ZuTransport req);
}
