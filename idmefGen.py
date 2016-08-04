import time
import datetime
import random
import sys
import psycopg2
import os

StartSize = 1000
StopSize = 5000
StepSize = 500
startTime = time.time()
timestamp = startTime
# random, dos, path, flooding
profile = 'dos'
folder = './producer/simTest/'

initSourceIPs = None
initTargetIPs = None

fromDB = True
#backend=orient
#port=2424
backend="psql"
port=5432
server="localhost"
user="surf"
pwd="ansii"
database="responsetest"

internalNets = ["172.16.","172.17.","172.20.1."]
externalNets = ["2.160.", "8."]

numTargets = 10
numSourcesInside = 10
numSourcesOutside = 10
numAttacksInt = 2
numAttacks = 8
infectProb = 100

if profile not in ['dos', 'flooding', 'path', 'random']:
    print ("Wrong Profile: ", profile)
    sys.exit(0)

if fromDB:
    if backend == 'orient':
        pass
    elif backend == 'psql':
        conn = psycopg2.connect(database=database, user=user, password=pwd, port=port, host=server)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
    else:
        print ("Wrong backend: ", backend)

if not os.path.exists(folder):
    os.makedirs(folder)

if fromDB:
    print ("Generate from DB using: ", backend)
else:
    print ("Generate random Values")

print ("Generate Sources and Targets ... ")

sourceIPs = []
targetIPs = []

if not fromDB:

    while len(sourceIPs) < numSourcesInside:
        ip = random.choice(internalNets)
        rangeGen = 4 - ip.count('.')
        ip += ".".join(str(random.randint(0, 255)) for _ in range(rangeGen))
        if ip not in sourceIPs:
            sourceIPs.append(ip)

    while len(targetIPs) < numTargets:
        ip = random.choice(internalNets)
        rangeGen = 4 - ip.count('.')
        ip += ".".join(str(random.randint(0, 255)) for _ in range(rangeGen))
        if ip not in targetIPs:
            targetIPs.append(ip)

else:

    if backend == 'orient':
        pass
    elif backend == 'psql':
        if profile == 'dos':
            victims = []
            query = "select ip.name from ip, mac, iptomac, interface, mactointerface, device, devicehasinterface, template, deviceusestemplate where ip.id = iptomac.fromnode and iptomac.tonode = mac.id and mac.id = mactointerface.fromnode and mactointerface.tonode = interface.id and device.id = devicehasinterface.fromnode and devicehasinterface.tonode = interface.id and device.id = deviceusestemplate.fromnode and deviceusestemplate.tonode = template.id and template.name in ('router', 'service','ids','server');"
            cur.execute(query)
            result = cur.fetchall()
            for elem in result:
                victims.append(elem[0])
            targetIPs = []
            for i in range(numTargets):
                if len(victims) == 0:
                    break
                target = random.choice(victims) 
                targetIPs.append(target)
                victims.remove(target)
        else:
            query = "select name from ip"
            cur.execute(query)
            result = cur.fetchall()
            for elem in result:
                if len(targetIPs) < numTargets:
                    targetIPs.append(elem[0])
                    continue
                if len(sourceIPs) < numSourcesInside:
                    sourceIPs.append(elem[0])
                    continue
                break
            
    else:
        print ("Wrong backend: ", backend)

while len(sourceIPs) - numSourcesInside < numSourcesOutside:
    ip = random.choice(externalNets)
    rangeGen = 4 - ip.count('.')
    ip += ".".join(str(random.randint(0, 255)) for _ in range(rangeGen))
    if ip not in sourceIPs:
        sourceIPs.append(ip)

print ("Generated Sources : ")
print (sourceIPs)

print ("Generated Targets : ")
print (targetIPs)


initSourceIPs = sourceIPs
initTargetIPs = targetIPs

print ("Generate Attacks ... ")

attackInf = []
attack = []

if profile == 'dos':
    randomAttacksInt = 0
    randomAttacks = 2

if not fromDB:
    for i in range(numAttacksInt):
        attackInf.append("ai_" + str(i))

    for i in range(numAttacks):
        attack.append("a_" + str(i))
else:

    if backend == 'orient':
        pass
    elif backend == 'psql':
        query = "select name from attack;"
        cur.execute(query)
        result = cur.fetchall()
        for elem in result:
            if len(attackInf) < numAttacksInt:
                attackInf.append(elem[0])
                continue
            if len(attack) < numAttacks:
                attack.append(elem[0])
                continue
            break
    else:
        print ("Wrong backend: ", backend)


print ("Generated infection Attacks : ")
print (attackInf)

print ("Generated non-infection Attacks : ")
print (attack)

attacks = attack + attackInf

print ("Generated non-infection Attacks : ")
print (attacks)

for size in range(StartSize, StopSize + 1, StepSize):

    ts = datetime.datetime.fromtimestamp(startTime).strftime('%Y-%m-%d_%H:%M:%S')
    filename = profile + "_" + str(ts) + "_" + str(size) + ".xml"

    newSources = []

    print ("Create ", filename)

    with open(folder + filename, 'w+') as fileToWrite:
        fileToWrite.write("<xml>\n")
        for i in range(size):
            target = random.choice(targetIPs)
            source = random.choice(list(set(sourceIPs) - set([target])))
            attackType = random.choice(attacks)

            if profile == 'path':
                if attackType in attackInt and len(targetIPs) > 1:
                    prob = random.randrange(1,infectProb)
                    if prob == 1:
                        sourceIPs = list(set(sourceIPs) - set([source]))
                        newSources.append(target)
                        targetIPs = list(set(targetIPs) - set([target]))
                        if len(sourceIPs) == 0:
                            sourceIPs = newSources
                            newSources = []

            elif profile == 'random':
                if attackType in attackInt and len(targetIPs) > 1:
                    prob = random.randrange(1,infectProb)
                    if prob == 1:
                        sourceIPs.append(target)

            else:
                pass


            msgID = i
            addTime = 1 / random.randrange(1,10)
            timestamp = timestamp + addTime
            tsIDMEF = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            idmef = (
            "<IDMEF-Message>"
            "<Alert messageid='" + str(msgID) + "'>"
            "<CreateTime>" + tsIDMEF + "</CreateTime>"
            "<Analyzer model='test'/>"
            "<Target><Node><Address category='ipv4-addr'><address>" + target + "</address></Address></Node></Target>"
            "<Source><Node><Address category='ipv4-addr'><address>" + source + "</address></Address></Node></Source>"
            "<Classification text='" + attackType + "'></Classification>"
            "</Alert>"
            "</IDMEF-Message>"
            )
            fileToWrite.write(idmef + "\n")
        fileToWrite.write("</xml>\n")

    sourceIPs = initSourceIPs
    targetIPs = initTargetIPs
