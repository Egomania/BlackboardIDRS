import json
import random
import sys
import os
from string import Template

generateGPLMTscripts = True
GPLMTFolder = "GPLMT"
GPLMTdomain=".asgard.net.in.tum.de"
GPLMTuser="surf"
GPLMTTargetFile = "targetsGen.xml"
GPLMTTasklistFile = "tasklistsGen.xml"

filenamePolicy = "configs/policyGenAGI.json"
filenameInf = "configs/infGenAGI.json"

templates = ["router", "host", "service", "vm", "ids", "executor"]

networks = [
    {"name":"serviceNetwork", "host": 0,  "router" : ["MainCabinSwitch"], "ids": ["nids"], "ip": "10.0.1.", "executor" : 0},
    {"name":"cabinNetwork", "host": 200,  "router" : ["MainCabinSwitch"], "ids": ["nids"], "ip": "10.0.0.", "executor" : 0}
#    {"name":"n1", "host": 100,  "router" : ["r1"], "ids": ["nids"], "ip": "172.16.0.", "executor" : 5},
#    {"name":"n2", "host": 100,  "router" : ["r2"], "ids": ["nids"], "ip": "172.16.1.", "executor" : 5},
#    {"name":"n3", "host": 100,  "router" : ["r3"], "ids": ["nids"], "ip": "172.16.2.", "executor" : 5},
#    {"name":"s", "host": 0, "router" : ["rs"], "ids": ["nids"], "ip": "172.17.0.", "executor" : 5},
#    {"name":"b",  "host": 0,   "router" : ["r1", "r2", "r3", "rs"], "ids": ["nids"], "ip": "172.20.0.", "executor" : 5}
]

services = [
{"name": "pa", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "pa"},
{"name": "icc", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "icc"},
{"name": "signs", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "signs"},
{"name": "infoDisplay", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "infoDisplay"},
{"name": "readingLights", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "readingLights"},
{"name": "pax", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "pax"},
{"name": "music", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "music"},
{"name": "pram", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "pram"},
{"name": "chime", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "chime"},
{"name": "seatNo", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "seatNo"},
{"name": "mMode", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "mMode"},
{"name": "light", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "light"},
{"name": "bite", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "bite"},
{"name": "cvms", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "cvms"},
{"name": "coas", "dep" : [], "port" : 30501, "network" : ["serviceNetwork"], "host" : "coas"}

#    {"name": "s1", "dep" : ["s2","s3"], "port" : 443, "network" : ["s"], "host" : "sh1"},
#    {"name": "s3", "dep" : ["s4"], "port" : 88, "network" : ["s"], "host" : "sh2"},
#    {"name": "s2", "dep" : ["s5"], "port" : 587, "network" : ["s"], "host" : "sh3"},
#    {"name": "s4", "dep" : ["s6"], "port" : 2424, "network" : ["s"], "host" : "sh4"},
#    {"name": "s5", "dep" : ["s6"], "port" : 5432, "network" : ["s"], "host" : "sh4"},
#    {"name": "s6", "dep" : [], "port" : 666, "network" : ["s"], "host" : "evil"},
#    {"name": "s7", "dep" : [], "port" : 1, "network" : ["s"], "host" : "sh5"},
#    {"name": "s8", "dep" : [], "port" : 2, "network" : ["s"], "host" : "sh5"},
#    {"name": "s9", "dep" : [], "port" : 3, "network" : ["s"], "host" : "sh5"},
#    {"name": "s10", "dep" : ["s7", "s8", "s9"], "port" : 4, "network" : ["s"], "host" : "sh6"}
]
users = 0
attacks = 0
consequences = 0
maxAttackCon = 0
maxRespCon = 0
maxDeployed = 0
responses = [
 {"userbased": 0, "impl": 0},
 {"hostbased": 0, "impl": 0},
 {"networkbased": 0, "impl": 0},
 {"servicebased": 0, "impl": 0},
 {"passive":0, "impl":0}
]
metrics = ['cost']
conflicts = 0
preconditions = [0,0,0]
maxPreconditions = 0
preconditionsImpls = 0

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
executorList = []
executorCounter = 0
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

    for i in range(0,elem['executor']):
        name = "exec_" + str(executorCounter)
        executorCounter = executorCounter + 1
        iface = {"order" : 1, "l2" : name+"_eth0", "mac" : getMac(), "l3" : netName, "ip" : ip + str(start)}
        ipList[netName].append(ip + str(start))
        start = start + 1
        device = {"device": {"name" : name, "template": "executor", "interfaces": [ {"interface" : iface} ] }}
        executorList.append(name)
        deviceJSON.append(device)


for elem in routers:
    deviceJSON.append(elem)

for elem in idses:
    deviceJSON.append(elem)

serviceCounter = {}
serviceHosts = []
serviceHostsJSON = []
serviceJSON = []
serviceList = []
for elem in services:

    name = elem['host']
    service = elem['name']
    nets = elem['network']
    ifaces = [] 

    serviceInst = {"service": {"name": service, "port": elem['port'], "servicedependsonservice" : elem['dep']}}
    serviceJSON.append(serviceInst)
    serviceList.append(service)    

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

usersJSON = []

for i in range(users):
    name = "u_" + str(i)
    loggedOn = random.choice(deviceList)
    usesServices = []
    for j in range(random.randint(1,len(serviceList))):
        service = random.choice(serviceList)
        if service not in usesServices:
            usesServices.append(service)
    user = {"user" : {"name" : name, "loggedOn": loggedOn, "uses": usesServices}}
    usersJSON.append(user)


dataInf['users'] = usersJSON

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
responseList = []
passiveList = []
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
        responseList.append(name)
        respCounter = respCounter + 1
        if len(target) == 0:
            passiveList.append(name)
            lenConList = len(conseqList)
        else:
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
        for i in range(elem['impl']):
            implName = name + "_" + str(implCounter)
            implCounter = implCounter + 1
            deployedOn = []
            lendeployList = random.randint(1,maxDeployed)
            if len(target) == 0:
                executingEntity = random.choice(idsList)
                for pasEnt in deviceList:
                    deployedOn.append(pasEnt)
                for pasEnt in serviceHosts:
                    deployedOn.append(pasEnt)
                for pasEnt in routerList:
                    deployedOn.append(pasEnt)
            else:
                if "network" in target:
                    networktoProtect = random.choice(networkList)
                    potentialRouterList = []
                    for potentialDev in deviceJSON:
                        for ifaceOfPotentialDev in potentialDev['device']['interfaces']:
                            if ifaceOfPotentialDev['interface']['l3'] == networktoProtect:
                                if potentialDev['device']['name'] not in routerList:
                                    deployedOn.append(potentialDev['device']['name'])
                                else:
                                    potentialRouterList.append(potentialDev['device']['name'])
                    executingEntity = random.choice(potentialRouterList)
                else:
                    if "service" in target:
                        candDev = serviceHosts
                    else:
                        candDev = deviceList
                    for j in range(lendeployList):
                        newDev = random.choice(candDev)
                        if newDev not in deployedOn:
                            deployedOn.append(newDev)
                    executingEntity = random.choice(executorList)

            metricValues = {}
            for metricName in metrics:
                if len(target) == 0:
                    metricValues[metricName] = 1
                else:
                    metricValues[metricName] = random.random()
            impl = {"implementation" : {"name" : implName, "deployedOn" : deployedOn, "metrics" : metricValues, "executor" : executingEntity}}

            impls.append(impl)

        respJSON = {"response" : {"name" : name, "target" : target, "responsemitigatesconsequences" : conList, "implementations" : impls, "conflicts" : [], "preconditions" : []}}

        responsesJSON.append(respJSON)

if not allConAlloc:
    while len(openCons) != 0:
        respCand = random.choice(responsesJSON)
        if len(respCand['response']['responsemitigatesconsequences']) < maxRespCon:
            newCon = random.choice(openCons)
            openCons.remove(newCon)
            respCand['response']['responsemitigatesconsequences'].append(newCon)

conflictMapper = {}
i = 0
while i < conflicts:
    c1 = random.choice(responseList)
    c2 = random.choice(responseList)
    if c1 in passiveList:
        continue
    if c2 in passiveList:
        continue
    if c1 == c2:
        continue
    if (c1 in conflictMapper.keys() and c2 in conflictMapper[c1]) or (c2 in conflictMapper.keys() and c1 in conflictMapper[c2]):
        continue
    i = i + 1
    if c1 not in conflictMapper.keys():
        conflictMapper[c1] = []
    if c2 not in conflictMapper.keys():
        conflictMapper[c2] = []
    conflictMapper[c1].append(c2)
    conflictMapper[c2].append(c1)

    
for elem in responsesJSON:
    name = elem['response']['name']
    if name in conflictMapper.keys():
        for entry in conflictMapper[name]:
            elem['response']['conflicts'].append(entry)

preConditionsList = {}
for elem in preconditions:
    level = str(preconditions.index(elem))
    preConditionsList[level] = []
    for i in range(elem):
        name = "p" + level + "_" + str(i)
        target = []
        conList = []
        impls = []
        implCounter = 1
        for i in range(preconditionsImpls):
            implName = name + "_" + str(implCounter)
            implCounter = implCounter + 1
            deployedOn = []
            executingEntity = random.choice(executorList)
            metricValues = {}
            for metricName in metrics:
                if len(target) == 0:
                    metricValues[metricName] = 1
                else:
                    metricValues[metricName] = random.random()
            impl = {"implementation" : {"name" : implName, "deployedOn" : deployedOn, "metrics" : metricValues, "executor" : executingEntity}}

        impls.append(impl)


        respJSON = {"response" : {"name" : name, "target" : target, "responsemitigatesconsequences" : conList, "implementations" : impls, "conflicts" : [], "preconditions" : []}}

        responsesJSON.append(respJSON)
        preConditionsList[level].append(name)

for elem in responsesJSON:
    lenPreconditionsList = random.randint(1,maxPreconditions)
    selectionList = []
    if "p" in elem['response']['name']:
        num = elem['response']['name'].split("_")[0].split("p")[1]
        if int(num) > len(preconditions) - 2:
            continue
        else:
            selectionList = preConditionsList[str(int(num) + 1)]
    else:
        selectionList = preConditionsList['0']
    for i in range(lenPreconditionsList):
        preCon = random.choice(selectionList)
        if preCon not in elem['response']['preconditions']:
            elem['response']['preconditions'].append(preCon)

dataPolicy['responses'] = responsesJSON


with open(filenamePolicy, 'w') as outfile:
    json.dump(dataPolicy, outfile, sort_keys=True, indent=2)

with open(filenameInf, 'w') as outfile:
    json.dump(dataInf, outfile, sort_keys=True, indent=2)

if generateGPLMTscripts:

    GPLMTTargetFolder = GPLMTFolder + "/targets"
    GPLMTTasklistsFolder = GPLMTFolder + "/tasklists"
    GPLMTExecutionPlansFolder = GPLMTFolder + "/executionplans"

    for elem in [GPLMTFolder, GPLMTTargetFolder, GPLMTTasklistsFolder, GPLMTExecutionPlansFolder]:
        if not os.path.exists(elem):
            os.makedirs(elem)

    GPLMTFile = Template(
        "<?xml version='1.0' encoding='utf-8'?>"
        "<experiment>"
            "<targets> $target </targets>"
            "<tasklists> $tasklist </tasklists>"
            "<steps> </steps>"
        "</experiment>"
    )

    target = Template(
        "<target name='$name' type='ssh'>"
            "<user>$user</user>"
            "<host>$host</host>"
        "</target>"
    )

    tasklist = Template(
        "<tasklist name='$name'>"
            "<seq>"
                "<run> echo I am $hostCMD and executing tasklist $cmd </run>"
            "</seq>"
        "</tasklist>"
    )

    targetList = ""
    # generate TargetFile
    for elem in deviceJSON:
        name = elem['device']['name']
        targetList = targetList + target.substitute(name=name, host=name+GPLMTdomain, user=GPLMTuser) + "\n"

    with open(GPLMTTargetFolder+"/"+GPLMTTargetFile, 'w') as outfile:
        outfile.write(GPLMTFile.substitute(target=targetList, tasklist=""))

    tasklistList = ""
    # generate TargetFile
    for elem in responsesJSON:
        cmd = elem['response']['name']
        tasklistList = tasklistList + tasklist.substitute(name=cmd, hostCMD="$HOSTNAME", cmd=cmd) + "\n"

    with open(GPLMTTasklistsFolder+"/"+GPLMTTasklistFile, 'w') as outfile:
        outfile.write(GPLMTFile.substitute(tasklist=tasklistList, target=""))

    
