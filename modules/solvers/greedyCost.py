import numpy as np
import sys
import gc
import logging
import subprocess
import time
import os
import xmltodict
import ast

import abc
from modules.solvers.SolverBase import SolverBase

class Solver(SolverBase):

    def script(self, fileName, solverConfig, dumpSolution, solPath):

        timeListe = list()

        logging.info("Start Problem CREATION.")

        start = time.time()

        with open(fileName) as f:
            lines = f.readlines()
            adminList = ast.literal_eval(''.join(lines[0]))
            damage = ast.literal_eval(''.join(lines[1]))
            conflicts = ast.literal_eval(''.join(lines[2]))
            responseList = ast.literal_eval(''.join(lines[3]))

        problem = (adminList, damage, conflicts, responseList)

        end = time.time()

        exec_time = end - start
    
        logging.info("Created in: %s", exec_time)

        timeListe.append(exec_time)
        timeListe.append(start)
        timeListe.append(end)


        logging.info("Start Problem SOLVING.")


        start = time.time()

        erg = self.solve_problem(problem, solverConfig)

        end = time.time()
    
        exec_time_second = end - start
    
        logging.info("Solved in: %s", exec_time)

        timeListe.append(exec_time)
        timeListe.append(start)
        timeListe.append(end)

        solution = self.evaluate_problem(problem, erg)

        costs = solution[1]
        numberResponses = solution[0]


        timeListe.append(numberResponses)
        timeListe.append(costs)

        if dumpSolution:
            parts = fileName.split("/")
            solName = parts[len(parts)-1]
            solCut = solName[9:]
            ext = solCut.split(".")[0]
            outFile = solPath + "/SCRIPT_" + ext

            self.dump_solution(erg, outFile)
        
        return timeListe


    def dump_problem(self, problem, prefix):

        if type(problem) is list:
             text = str(problem[0]) + "\n" + str(problem[1]) + "\n" + str(problem[2]) + "\n" + str(problem[3])
        else:
            text = str(problem[0][0]) + "\n" + str(problem[0][1]) + "\n" + str(problem[0][2]) + "\n" + str(problem[0][3])
        with open(prefix + ".lp", "w") as text_file:
            text_file.write("{0}".format(text))

    def dump_solution(self, problem, prefix):
        text = str(problem[1]) + "\n" + str(problem[2])

        with open(prefix + ".sol", "w") as text_file:
            text_file.write("{0}".format(text))        

    def delete_problem(self, problem):
        pass

    def create_problem(self, data): 

        host_attacked = data["attacked"]
        damage_used = data["damage"]
        responses_used = data["response"]
        responseList = []
        conflicts = {}

        adminList = []

        damage = {}
        for elem in damage_used:
            damage[elem.name] = elem.value

        for entry in responses_used:
            for hostElem in entry.dest:
                host = hostElem
                response = entry.name
                costGes = 0
                cost = {}
                for elem in entry.metrics:
                    costGes = costGes + elem.value
                    cost[elem.name] = elem.value

                listEntry = [host, response, costGes, cost]

                adminList.append(listEntry)

            responseList.append(response)
            conflicts[response] = []
            for confl in entry.conflicting_responses:
                conflicts[response].append(confl.name)

        return [adminList, damage, conflicts, responseList]

    def checkSingle(self, costs, damage, liste):
        for name, value in damage.iteritems():
            if liste[name] + costs[name] > value:
                return False
        return True

    def updateDamageList (self, costs, liste):
        for key, value in liste.iteritems():
            liste[key] = value + costs[key]
        return liste

    def solve_problem(self, problem, config):
        
        adminList = problem[0]
        conflicts = problem[2]
        responseList = problem[3]
        damage = 0
        damageList = {}

        solution = []
        for name, value in problem[1].items():
            damage = damage + float(value) 
            damageList[name] = 0   

        hosts = {}
        responses = {}
        costs = 0

        adminList.sort(key=lambda x: (x[0], x[2]))

        for entry in adminList:

            host = entry[0]
            response = entry[1]
            cost = entry[2]
            
            if damage > 0:
                if (host not in hosts.keys()) and (response not in responses.keys()) and (costs + cost < damage and self.checkSingle(entry[3], problem[1], damageList)):
                    hosts[host] = True
                    responses[response] = True
                    costs = costs + cost
                    damageList = self.updateDamageList(entry[3], damageList)
                    for confl in conflicts[response]:
                        responses[response] = False
            
            else:
                if (host not in hosts.keys()) and (response not in responses.keys()):
                    hosts[host] = True
                    responses[response] = True
                    costs = costs + cost
                    for confl in conflicts[response]:
                        responses[response] = False

            
            if (host not in hosts.keys()) and (response in responses.keys()):
                if responses[response] == True: 
                    hosts[host] = True

        
        for entry in responseList:
            if entry not in responses.keys():
                responses[entry] = False
        
        for entry in responses:
            solution.append([entry, responses[entry]])

        return(problem, solution, costs)


    def evaluate_problem(self, data, erg):
        cost = erg[2]
        counter = 0
        for elem in erg[1]:
            if elem[1] :
                counter = counter + 1

        return(counter, cost)
