# RPC with service discovery and registry
----------------------

## Features:

* Configurable service discovery and registry

* Pluggable loadbalancing

* Auto discovery services and clients

* Http and idl wire protocl like thrift, protobuf

* Asynchronous io, parallel execution, request pipelining

* Connection pooling

* Automatic reconnection

----------------------
## Protocol:
----------------------
### Thrift

Define data types and services in a thrift definition file

### Http

Leverage JAX-RS annotations

### Protobuf

Define data types and services in a proto definition file

## Service discovery and registry
----------------------

Your service discovery needs to implemment interface **ServiceDiscovery**:
	    
	    public List<InetSocketAddress> getServerList(String service, String proto) throws IOException;



Your service registry needs to implement interface **ServiceRegistration**:
		
		public void registerServer(String service, String proto, InetSocketAddress address);

    	public ListenableFuture<?> unregisterServer(String service, String proto, InetSocketAddress address);
   
Currently **EtcdServiceDiscovery** and **EtcdServiceRegistration** provided

## Load balancing
---------------------
Your custom load balancing needs to implment interface **LoadBalancingPolicy**:

		public interface LoadBalancingPolicy extends Host.StateListener {
    		public Iterator<Host> queryPlan();

    		public void init(Collection<Host> hosts);
		}

Currentlty **RoundRobinPolicy** provided

## Auto discovery services and clients
---------------------

All you need to do is to set up service discovery and registration, then services implemented will be discovered and called by clients

## Examples
----------------------
### Thrfit
#### client
#### server
	Refer to test/java/cn/v5/lbrpc/thrfit/ThriftShortHandTest.java
### Http
#### client
#### server
	Refer to test/java/cn/v5/lbrpc/http/HttpShortHandTest.java

