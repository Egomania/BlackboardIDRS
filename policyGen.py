import json
import random
import sys

filenamePolicy = "configs/policyGenTest1.json"
filenameInf = "configs/infGenTest1.json"

templates = ["router", "host", "service", "vm", "ids"]

networks = [
    {"name":"n1", "host": 100,  "router" : ["r1"], "ids": ["nids"], "ip": "172.16.0."},
    {"name":"n2", "host": 100,  "router" : ["r2"], "ids": ["nids"], "ip": "172.16.1."},
    {"name":"n3", "host": 100,  "router" : ["r3"], "ids": ["nids"], "ip": "172.16.2."},
    {"name":"s", "host": 0, "router" : ["rs"], "ids": ["nids"], "ip": "172.17.0."},
    {"name":"b",  "host": 0,   "router" : ["r1", "r2", "r3", "rs"], "ids": ["nids"], "ip": "172.20.0."}
]

services = [
    {"name": "s1", "dep" : ["s2","s3"], "port" : 443, "network" : ["s"], "host" : "sh1"},
    {"name": "s3", "dep" : ["s4"], "port" : 88, "network" : ["s"], "host" : "sh2"},
    {"name": "s2", "dep" : ["s5"], "port" : 587, "network" : ["s"], "host" : "sh3"},
    {"name": "s4", "dep" : ["s6"], "port" : 2424, "network" : ["s"], "host" : "sh4"},
    {"name": "s5", "dep" : ["s6"], "port" : 5432, "network" : ["s"], "host" : "sh4"},
    {"name": "s6", "dep" : [], "port" : 666, "network" : ["s"], "host" : "evil"}
]

attacks = 100
consequences = 300
maxAttackCon = 5
maxRespCon = 5
maxDeployed = 10
responses = [
 {"userbased": 100, "impl": 10, "router" : 0},
 {"hostbased": 100, "impl": 10, "router" : 0},
 {"networkbased": 300, "impl": 4, "router" : 0},
 {"servicebased": 200, "impl": 5, "router" : 4},
 {"passive":10, "impl":5, "router" : 4}
]

if consequences < maxAttackCon:
    maxAttackCon = consequences

if (attacks * maxAttackCon) < consequences:
    print ("To much conseqences for attacks and con per attack ratio.")
    sys.exit(0)


def getMac():
    mac = [ 0x1e, 0x00, random.randint(0x00, 0x7f), random.randint(0x00, 0x7f), random.randint(0x00, 0xff), random.randint(0x00, 0xff) ]
    macAdr = ':'.join(map(lambda x: "%02x" % x, mac))
    return macAdr

def getIP(netName, offset):
    for elem in networks:
        if elem['name'] == netName:
            return elem['ip'] + str(255 - offset)

def getDevInfo(name, serviceHostsJSON):
    for elem in serviceHostsJSON:
        if elem['device']['name'] == name:
            dev = {}
            order = len(elem['device']['interfaces']) + 1
            dev['order'] = order
            nets = []
            for net in elem['device']['interfaces']:
                nets.append(net['interface']['l3'])
            dev['nets'] = nets
            dev['device'] = elem['device']
            return dev

dataPolicy = {}
dataInf = {}

templatesJSON = []
templateList = []

for elem in templates:
    name = elem
    template = {"template" : {"name" : name}}
    templatesJSON.append(template)
    templateList.append(name)

dataInf['templates'] = templatesJSON


networksJSON = []
networkList = []

for elem in networks:
    name = elem['name']
    network = {"network" : {"name" : name, "prefix" : 64}}
    networksJSON.append(network)
    networkList.append(name)

dataInf['networks'] = networksJSON

deviceJSON = []
deviceList = []
routerList = []
idsList = []
ipList = {}

deviceCounter = 0
routers = []
idses = []
for elem in networks:

    netName = elem['name']
    ip = elem['ip']
    ipList[netName] = []
    startR = 1
    for entry in elem['router']:
        found = False
        for router in routers:
            if router["device"]['name'] == entry:
                found = True
                order = len(router["device"]['interfaces']) + 1
                iface = {"order" : order, "l2" : entry+"_eth"+str(order), "mac" : getMac(), "l3" : netName, "ip" : ip + str(startR)}
                router["device"]['interfaces'].append({"interface" : iface})
                ipList[netName].append(ip + str(startR))
                startR = startR + 1
        if not found:
            name = entry
            iface = {"order" : 1, "l2" : name+"_eth0", "mac" : getMac(), "l3" : netName, "ip" : ip + str(startR)}
            ipList[netName].append(ip + str(startR))
            startR = startR + 1
            router = {"device": {"name" : name, "template": "router", "interfaces": [ {"interface" : iface} ] }}
            routers.append(router)
            routerList.append(name)

    for entry in elem['ids']:
        found = False
        for ids in idses:
            if ids["device"]['name'] == entry:
                found = True
                order = len(ids["device"]['interfaces']) + 1
                iface = {"order" : order, "l2" : entry+"_eth"+str(order), "mac" : getMac(), "l3" : netName, "ip" : ip + str(startR)}
                ids["device"]['interfaces'].append({"interface" : iface})
                ipList[netName].append(ip + str(startR))
                startR = startR + 1
        if not found:
            name = entry
            iface = {"order" : 1, "l2" : name+"_eth0", "mac" : getMac(), "l3" : netName, "ip" : ip + str(startR)}
            ipList[netName].append(ip + str(startR))
            startR = startR + 1
            ids = {"device": {"name" : name, "template": "ids", "interfaces": [ {"interface" : iface} ] }}
            idses.append(ids)
            idsList.append(name)

    start = len(elem['router']) + len(elem['ids']) + 1
    for i in range(0,elem['host']):
        name = "h_" + str(deviceCounter)
        deviceCounter = deviceCounter + 1
        iface = {"order" : 1, "l2" : name+"_eth0", "mac" : getMac(), "l3" : netName, "ip" : ip + str(start)}
        ipList[netName].append(ip + str(start))
        start = start + 1
        device = {"device": {"name" : name, "template": "host", "interfaces": [ {"interface" : iface} ] }}


        deviceList.append(name)
        deviceJSON.append(device)

for elem in routers:
    deviceJSON.append(elem)

for elem in idses:
    deviceJSON.append(elem)

serviceCounter = {}
serviceHosts = []
serviceHostsJSON = []
serviceJSON = []
for elem in services:

    name = elem['host']
    service = elem['name']
    nets = elem['network']
    ifaces = [] 

    serviceInst = {"service": {"name": service, "port": elem['port'], "servicedependsonservice" : elem['dep']}}
    serviceJSON.append(serviceInst)
    
    if name in serviceHosts:
        dev = getDevInfo(name, serviceHostsJSON)
        order = dev['order']
        netsAvail = dev['nets']
        for net in nets:
            if net in netsAvail:
                for entry in serviceHostsJSON:
                    if entry['device']['name'] == name:
                        for interAvail in entry['device']['interfaces']:
                            if interAvail['interface']['l3'] == net:
                                interAvail['interface']['service'].append(service)
            else:
                if net in serviceCounter.keys():
                    serviceCounter[net] = serviceCounter[net] + 1
                else:
                    serviceCounter[net] = 1
                iface = {"order" : order, "l2" : name+"_eth0", "mac" : getMac(), "l3" : net, "ip" : getIP(net, serviceCounter[net]), "service" : [service]}
                for entry in serviceHostsJSON:
                    if entry['device']['name'] == name:
                        entry['device']['interfaces'].append({"interface" : iface})

    else:
        order = 0
        for net in nets:
            order = order + 1
            if net in serviceCounter.keys():
                serviceCounter[net] = serviceCounter[net] + 1
            else:
                serviceCounter[net] = 1
        
            iface = {"order" : order, "l2" : name+"_eth0", "mac" : getMac(), "l3" : net, "ip" : getIP(net, serviceCounter[net]), "service" : [service]}
            ifaces.append({"interface" :  iface})

        device = {"device": {"name" : name, "template" : "service", "interfaces": ifaces }}
        serviceHostsJSON.append(device)
        serviceHosts.append(name)

for elem in serviceHostsJSON:
    deviceJSON.append(elem)

dataInf['devices'] = deviceJSON
dataInf['services'] = serviceJSON

consJSON = []
conseqList = []
for i in range(consequences):
    name = "c" + str(i)
    conseq = {"consequence" : {"name" : name}}
    consJSON.append(conseq)
    conseqList.append(name)

dataPolicy['consequences'] = consJSON


attacksJSON = []
attackList = []
openCons = list(conseqList)
allConAlloc = False
for i in range(attacks):
    name = "a" + str(i)
    lenConList = random.randint(1,maxAttackCon)
    conList = []
    for j in range(lenConList):
        newCon = random.choice(openCons)
        openCons.remove(newCon)
        if len(openCons) == 0:
            openCons = list(conseqList)
            allConAlloc = True
        if newCon not in conList:
            conList.append(newCon)
    attack = {"attack" : {"name" : name, "attackhasconsequences" : conList}}
    attacksJSON.append(attack)
    attackList.append(name)

if not allConAlloc:
    while len(openCons) != 0:
        attackCand = random.choice(attacksJSON)
        if len(attackCand['attack']['attackhasconsequences']) < maxAttackCon:
            newCon = random.choice(openCons)
            openCons.remove(newCon)
            attackCand['attack']['attackhasconsequences'].append(newCon)

dataPolicy['attacks'] = attacksJSON

responsesJSON = []
respCounter = 1
openCons = list(conseqList)
allConAlloc = False
for elem in responses:
    try:
        num = elem['userbased']
        target = ['user']
    except:
        try:
            num = elem['hostbased']
            target = ['host']
        except:
            try:
                num = elem['networkbased']
                target = ['network']
            except:
                try:
                    num = elem['servicebased']
                    target = ['service']
                except:
                    num = elem['passive']
                    target = []
    
    for i in range(num):
        name = "r" + str(respCounter)
        respCounter = respCounter + 1
        
        lenConList = random.randint(1,maxRespCon)
        conList = []
        for j in range(lenConList):
            newCon = random.choice(openCons)
            openCons.remove(newCon)
            if len(openCons) == 0:
                openCons = list(conseqList)
                allConAlloc = True
            if newCon not in conList:
                conList.append(newCon)

        impls = []
        implCounter = 1
        candRoutersCount = elem['router']
        for i in range(elem['impl']):
            implName = name + "_" + str(implCounter)
            implCounter = implCounter + 1
            deployedOn = []
            lendeployList = random.randint(1,maxDeployed)
            for j in range(lendeployList):
                if candRoutersCount > 0:
                    candDev = routerList
                    candRoutersCount = candRoutersCount - 1
                else:
                    if "service" in target:
                        candDev = serviceHosts
                    else:
                        if "network" in target:
                            candDev = routerList
                        if not target:
                            candDev = idsList + deviceList
                        else:
                            candDev = deviceList
                newDev = random.choice(candDev)
                if newDev not in deployedOn:
                    deployedOn.append(newDev)
            impl = {"implementation" : {"name" : implName, "deployedOn" : deployedOn}}

            impls.append(impl)

        respJSON = {"response" : {"name" : name, "target" : target, "responsemitigatesconsequences" : conList, "implementations" : impls}}

        responsesJSON.append(respJSON)

if not allConAlloc:
    while len(openCons) != 0:
        respCand = random.choice(responsesJSON)
        if len(respCand['response']['responsemitigatesconsequences']) < maxRespCon:
            newCon = random.choice(openCons)
            openCons.remove(newCon)
            respCand['response']['responsemitigatesconsequences'].append(newCon)


dataPolicy['responses'] = responsesJSON


with open(filenamePolicy, 'w') as outfile:
    json.dump(dataPolicy, outfile, sort_keys=True, indent=2)

with open(filenameInf, 'w') as outfile:
    json.dump(dataInf, outfile, sort_keys=True, indent=2)

    
