CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra

all: tcp udp #quic

tcp: tcp/broker_tcp tcp/publisher_tcp tcp/subscriber_tcp

udp: udp/broker_udp udp/publisher_udp udp/subscriber_udp

# quic: quic/broker_quic quic/publisher_quic quic/subscriber_quic

tcp/broker_tcp: tcp/broker_tcp.cpp
	$(CXX) $(CXXFLAGS) -o tcp/broker_tcp tcp/broker_tcp.cpp

tcp/publisher_tcp: tcp/publisher_tcp.cpp
	$(CXX) $(CXXFLAGS) -o tcp/publisher_tcp tcp/publisher_tcp.cpp

tcp/subscriber_tcp: tcp/subscriber_tcp.cpp
	$(CXX) $(CXXFLAGS) -o tcp/subscriber_tcp tcp/subscriber_tcp.cpp

udp/broker_udp: udp/broker_udp.cpp
	$(CXX) $(CXXFLAGS) -o udp/broker_udp udp/broker_udp.cpp

udp/publisher_udp: udp/publisher_udp.cpp
	$(CXX) $(CXXFLAGS) -o udp/publisher_udp udp/publisher_udp.cpp

udp/subscriber_udp: udp/subscriber_udp.cpp
	$(CXX) $(CXXFLAGS) -o udp/subscriber_udp udp/subscriber_udp.cpp

# quic/broker_quic: quic/broker_quic.cpp
# 	$(CXX) $(CXXFLAGS) -o quic/broker_quic quic/broker_quic.cpp

# quic/publisher_quic: quic/publisher_quic.cpp
# 	$(CXX) $(CXXFLAGS) -o quic/publisher_quic quic/publisher_quic.cpp

# quic/subscriber_quic: quic/subscriber_quic.cpp
# 	$(CXX) $(CXXFLAGS) -o quic/subscriber_quic quic/subscriber_quic.cpp

clean:
	rm -f tcp/broker_tcp tcp/publisher_tcp tcp/subscriber_tcp
	rm -f udp/broker_udp udp/publisher_udp udp/subscriber_udp
# 	rm -f quic/broker_quic quic/publisher_quic quic/subscriber_quic

.PHONY: all tcp udp clean #quic clean