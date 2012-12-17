package zu.finagle;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.twitter.finagle.Service;

public class ZuFinalgeServiceRegistry {

	private static Map<String,ZuFinalgeServiceRegistry> registryMap = Collections.synchronizedMap(new HashMap<String,ZuFinalgeServiceRegistry>());
	
	private Map<InetSocketAddress, Service<?,?>> svcRegistry = Collections.synchronizedMap(new HashMap<InetSocketAddress,Service<?,?>>());
	
	private ZuFinalgeServiceRegistry(){
		
	}
	
	public static ZuFinalgeServiceRegistry getInstance(String clusterName){
		ZuFinalgeServiceRegistry registry = registryMap.get(clusterName);
		
		if (registry == null){
			registry = new ZuFinalgeServiceRegistry();
			registryMap.put(clusterName, registry);
		}
		
		return registry;
	}
	
	public <Req,Res> Service<Req,Res> getService(InetSocketAddress addr, ZuFinagleServiceFactory<Req,Res> svcFactory){
		Service<?,?> svc = svcRegistry.get(addr);
		if (svc == null && svcFactory != null){
			svc = svcFactory.buildFinagleService(addr);
			svcRegistry.put(addr, svc);
		}
		return (Service<Req,Res>)svc;
	}
	
	public <Req,Res> Service<Req,Res> getService(InetSocketAddress addr){
		return getService(addr, null);
	}
	
	public <Req,Res> Service<Req,Res> removeService(InetSocketAddress addr){
		return (Service<Req,Res>)svcRegistry.remove(addr);
	}
}
