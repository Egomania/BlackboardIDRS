import abc

class SolverBase(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod    
    def script(self, fileName, solverConfig, dumpSolution, solPath):
        return

    @abc.abstractmethod
    def create_problem(self, data):
        return
    
    @abc.abstractmethod
    def solve_problem(self, data, config):
        return

    @abc.abstractmethod
    def dump_problem(self, problem, path):
        return

    @abc.abstractmethod
    def dump_solution(self, problem, path):
        return

    @abc.abstractmethod
    def evaluate_problem(self, data, problem):
        return

    @abc.abstractmethod
    def delete_problem(self, problem):
        return
