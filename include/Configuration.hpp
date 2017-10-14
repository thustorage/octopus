/***********************************************************************
* 
* 
* Tsinghua Univ, 2016
*
***********************************************************************/
#ifndef CONFIGURATION_HEADER
#define CONFIGURATION_HEADER

#include <iostream>  
#include <boost/property_tree/ptree.hpp>  
#include <boost/property_tree/xml_parser.hpp>  
#include <boost/typeof/typeof.hpp>
#include <unordered_map>
#include <string>

using namespace std;  
using namespace boost::property_tree;

class Configuration {
private:
	ptree pt;
	unordered_map<uint16_t, string> id2ip;
	unordered_map<string, uint16_t> ip2id;
	int ServerCount;
public:
	Configuration();
	~Configuration();
	string getIPbyID(uint16_t id);
	uint16_t getIDbyIP(string ip);
	unordered_map<uint16_t, string> getInstance();
	int getServerCount();
};

#endif