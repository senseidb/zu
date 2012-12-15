package zu.core.cluster;

import java.net.InetSocketAddress;
import java.util.List;

public interface PartitionInfoReader {
  List<Integer> getPartitionFor(InetSocketAddress addr);
}
