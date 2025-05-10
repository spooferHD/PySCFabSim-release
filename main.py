from simulation.greedy import run_greedy
from rl_train import main as rl_train
from rl_test import main as rl_test 
import sys
import time

sys.path.insert(0, '.')


def greedy():
    profile = False
    if profile:
        from pyinstrument import Profiler

        p = Profiler()
        p.start()

    for i in range(30):    
        try:
            print("HEIK - START DURCHGANG=", i)
            run_greedy()
            print("HEIK - ENDE DURCHGANG=" , i)
            print()
        except:
            print("HEIK - Fehler bei DURCHGANG=" , i)
            print()
        


    if profile:
        p.stop()
        p.open_in_browser()


if __name__ == '__main__':
    greedy()