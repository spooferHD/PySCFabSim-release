import os
import sys
#sys.path.append(os.path.join(os.path.sep, 'data','horse','ws','wiro085f-WsRodmann','Final_Version','PySCFabSim', 'simulation'))
#sys.path.append(os.path.join('C:/','Users','willi','OneDrive','Documents','Studium','Diplomarbeit','Programm + Datengrundlage','PySCFabSim-release-William-Rodmann','simulation'))
sys.path.append(os.path.join('C:/','Users','David Heik','Desktop','Arbeit2024','PySCFabSim','Projekt-Reproduktion','Mai-Session', 'PySCFabSim-release','simulation'))
#sys.path.append(os.path.join('C:/','Users','David Heik','Desktop','Arbeit2024','PySCFabSim','Projekt-Reproduktion','Mai-Session', 'PySCFabSim-release','simulation', 'gym'))
#sys.path.append(os.path.join(os.path.sep, 'projects','p078','p_htw_promentat','Heik_Reproduktion_', 'simulation'))
from collections import defaultdict
from datetime import datetime
from typing import List

from classes import Lot, Machine
from dispatching.dispatcher import dispatcher_map
from file_instance import FileInstance
from plugins.cost_plugin import CostPlugin
from randomizer import Randomizer
from read import read_all
from stats import print_statistics
from events import ResetEvent
import random

import argparse
import pandas as pd
import matplotlib.pyplot as plt

last_sort_time = -1
round_robin = False


def dispatching_combined_permachine(ptuple_fcn, machine, time, setups):
    for lot in machine.waiting_lots:
        # if (machine.min_runs_left is not None and machine.current_setup != lot.actual_step.setup_needed) or lot.cqt_waiting != '':
        #     print("Einstieg")
        lot.ptuple = ptuple_fcn(lot, time, machine, setups)

        
def find_alternative_machine(instance, lots, machine):
    m: Machine
    for m in instance.family_machines[machine.family]: #hier wird eine Maschine gesucht, wo das Setup dem Los-Setup entspricht
        if m in instance.usable_machines and m.current_setup == lots[0].actual_step.setup_needed:  
            machine = m
            break
    return machine

def max_batch(lots):     
    if len(lots) >= lots[0].actual_step.batch_max:
        lots = lots[:lots[0].actual_step.batch_max]
    if len(lots) < lots[0].actual_step.batch_max:
        lots = None
    return lots

def min_batch(lots):   
    if len(lots) < lots[0].actual_step.batch_min:
        lots = None
    elif len(lots) >= lots[0].actual_step.batch_min:
        if len(lots) >= lots[0].actual_step.batch_max:
            lots = lots[:lots[0].actual_step.batch_max]
        else:
            lots = lots
    return lots

def demand_batch(lots):
    if len(lots) >= lots[0].actual_step.batch_max:
        lots = lots[:lots[0].actual_step.batch_max]
    elif len(lots) < lots[0].actual_step.batch_max and len(lots) >= lots[0].actual_step.batch_min:
        lots = lots
    else:
        lots = None
    return lots

def get_lots_to_dispatch_by_machine(instance, ptuple_fcn, machine=None):
    global round_robin	
    time = instance.current_time
    if machine is None:
        for machine in instance.usable_machines:
            break
    dispatching_combined_permachine(ptuple_fcn, machine, time, instance.setups)
    wl = sorted(machine.waiting_lots, key=lambda k: k.ptuple)
    # select lots to dispatch
    lot = wl[0]
    if lot.actual_step.batch_max > 1:
        # construct batch
        lot_m = defaultdict(lambda: [])
        for w in wl:
            lot_m[w.actual_step.step_name + '_' + w.part_name].append(w) 
        lot_l = sorted(list(lot_m.values()),
                       key=lambda l: (
                           l[0].ptuple[0],  # min run setup 
                           #l[0].ptuple[1],  # cqt
                           -min(1, len(l) / l[0].actual_step.batch_max),  # then maximize the batch size
                           0 if len(l) >= l[0].actual_step.batch_min else 1,  # then take min batch size into account
                           *(l[0].ptuple[2:]),  # finally, order based on prescribed priority rule
                       ))
        lots: List[Lot] = lot_l[0]
        if instance.rpt_route is not None:
            if len(lots) >= lots[0].actual_step.batch_min:
                lots = lots[:lots[0].actual_step.batch_min]
            if len(lots) > lots[0].actual_step.batch_max:
                lots = lots[:lots[0].actual_step.batch_max]
        elif instance.rpt_route is None:
            if instance.batch_strat == 'Max':        
                lots = max_batch(lots)
            if instance.batch_strat == 'Min':
                lots = min_batch(lots)
            if instance.batch_strat == 'RoundRobin':
                lots = max_batch(lots) if not round_robin else min_batch(lots)
                round_robin = not round_robin
            if instance.batch_strat == 'Demand':
                lots = demand_batch(lots)
                
    else:
        # dispatch single lot
        lots = [lot]
   
    if lots is not None:
        if len(lot.dedications) > 1:
            for d in lot.dedications:
                if lot.actual_step.idx + 1 == d:
                    machine_dict = {m.idx: m for m in instance.usable_machines}
                    machine_idx = lot.dedications[d]
                    machine = machine_dict.get(machine_idx)
                    if machine:
                        lot.dedications.pop(d)
                        break
        # Das hier ist exterm wichtig, dass die LSSU-Regel eingehalten wird. 
        elif lot.actual_step.setup_needed != '' and machine.current_setup != lot.actual_step.setup_needed:
            machine = find_alternative_machine(instance, lots, machine)
        
            
    if machine.min_runs_left is not None and machine.min_runs_setup != lots[0].actual_step.setup_needed:
   
        lots = None
    
    return machine, lots


def build_batch(lot, nexts):
    batch = [lot]
    if lot.actual_step.batch_max > 1:
        for bo_lot in nexts:
            if lot.actual_step.step_name == bo_lot.actual_step.step_name:
                batch.append(bo_lot)
            if len(batch) == lot.actual_step.batch_max:
                break
    return batch


def get_lots_to_dispatch_by_lot(instance, current_time, dispatcher):
    global last_sort_time
    if last_sort_time != current_time:
        for lot in instance.usable_lots:
            lot.ptuple = dispatcher(lot, current_time, None)
        last_sort_time = current_time
        instance.usable_lots.sort(key=lambda k: k.ptuple)
    lots = instance.usable_lots
    setup_machine, setup_batch = None, None
    min_run_break_machine, min_run_break_batch = None, None
    family_lock = None
    for i in range(len(lots)):
        lot: Lot = lots[i]
        if family_lock is None or family_lock == lot.actual_step.family:
            family_lock = lot.actual_step.family
            assert len(lot.waiting_machines) > 0
            for machine in lot.waiting_machines:
                if lot.actual_step.setup_needed == '' or lot.actual_step.setup_needed == machine.current_setup:
                    return machine, build_batch(lot, lots[i + 1:])
                else:
                    if setup_machine is None and machine.min_runs_left is None:
                        setup_machine = machine
                        setup_batch = i
                    if min_run_break_machine is None:
                        min_run_break_machine = machine
                        min_run_break_batch = i
    if setup_machine is not None:
        return setup_machine, build_batch(lots[setup_batch], lots[setup_batch + 1:])
    return min_run_break_machine, build_batch(lots[min_run_break_batch], lots[min_run_break_batch + 1:])


def run_greedy():
    p = argparse.ArgumentParser()
    p.add_argument('--dataset', type=str)
    p.add_argument('--days', type=int)
    p.add_argument('--dispatcher', type=str)
    p.add_argument('--seed', type=int)
    p.add_argument('--wandb', action='store_true', default=False)
    p.add_argument('--chart', action='store_true', default=False)
    p.add_argument('--alg', type=str, default='l4m', choices=['l4m', 'm4l'])
    p.add_argument('--WIP', type=bool, default=True)
    p.add_argument('--rpt_mode', type=bool, default=None)
    p.add_argument('--rpt_route', type=str, default=None)
    p.add_argument('--batch_strat', type=str, default="Max", choices=['Max', 'Min', 'RoundRobin', 'Demand']) #Max,Min, RoundRobin, Demand
    a = p.parse_args()
    seed = random.randint(1, 10000)
    seed = 9949  #### ACHTUNG - Fixed Seed
    a.dataset = 'SMT2020_HVLM'
    a.days = 730
    a.dispatcher = 'fifo'
    a.seed = seed
    a.WIP = True 
    a.rpt_mode = False
    a.rpt_route = None
    a.batch_strat = 'Demand'
    if a.rpt_mode and not None:
        if a.rpt_route:
            print('RPT mode is on')
            a.rpt_route = "part_" + str(a.rpt_route)
            a.WIP = False 
            a.days = 720
        else:
            print('RPT mode is on, but no route is given')
            exit()
    elif a.rpt_route:
        print('RPT route is given, but RPT mode is off')
        exit()
    else:
        print('RPT mode is off')


    
    sys.stderr.write('Loading ' + a.dataset + ' for ' + str(a.days) + ' days, using ' + a.dispatcher + '\n')
    sys.stderr.flush()

    start_time = datetime.now()

    files = read_all('datasets/' + a.dataset)
    if a.WIP == False:
        files['WIP.txt'] = files['WIPempty.txt']
    run_to = 3600 * 24 * a.days
    seedValue=Randomizer().random.seed(a.seed)
    l4m = a.alg == 'l4m'
    plugins = []
    if a.wandb:
        from plugins.wandb_plugin import WandBPlugin
        plugins.append(WandBPlugin())
    if a.chart:
        from plugins.chart_plugin import ChartPlugin
        plugins.append(ChartPlugin())
    plugins.append(CostPlugin())
    instance = FileInstance(files, run_to, l4m, plugins, a.rpt_route, a.batch_strat)
    if (a.WIP == False or a.days > 365) and a.rpt_mode == False:
        instance.add_event(ResetEvent(31536000))

    dispatcher = dispatcher_map[a.dispatcher]

    sys.stderr.write('Seed: ' + str(a.seed) + '\n')
    sys.stderr.write('Starting simulation with dispatching rule'+ a.dispatcher +'\n\n')
    sys.stderr.flush()

    while not instance.done:
        done = instance.next_decision_point()
        instance.print_progress_in_days()
        if done or instance.current_time > run_to:
            break

        if l4m:
            machine, lots = get_lots_to_dispatch_by_machine(instance, dispatcher)
            if lots is None:
                instance.usable_machines.remove(machine)
            else:
                instance.dispatch(machine, lots)
        else:
            machine, lots = get_lots_to_dispatch_by_lot(instance, instance.current_time, dispatcher)
            if lots is None:
                instance.usable_lots.clear()
                instance.lot_in_usable.clear()
                instance.next_step()
            else:
                instance.dispatch(machine, lots)
        
    #instance.save_setup_when_needed()
    instance.finalize()
    interval = datetime.now() - start_time
    print(instance.current_time_days, ' days simulated in ', interval)
    print_statistics(instance, a.days, a.dataset, a.dispatcher, method='greedy_seed' + str(a.seed), wip=a.WIP, seed=a.seed)
    

def run_greedy_RL(dataset, RL_days, greedy_days, dispatcher, seed, wandb, chart, alg='l4m'):
     
    sys.stderr.write('Loading ' + dataset + ' for ' + str(greedy_days) + ' days, using ' + dispatcher + '\n')
    sys.stderr.flush()

    start_time = datetime.now()

    files = read_all('datasets/' + dataset)

    run_to = 3600 * 24 * RL_days
    greedy_run_to = 3600 * 24 * greedy_days
    Randomizer().random.seed(seed)
    l4m = alg == 'l4m'
    files['WIP.txt'] = files['WIPempty.txt']
    plugins = []
    if wandb:
        from plugins.wandb_plugin import WandBPlugin
        plugins.append(WandBPlugin())
    if chart:
        from plugins.chart_plugin import ChartPlugin
        plugins.append(ChartPlugin())
    plugins.append(CostPlugin())
    instance = FileInstance(files, run_to, l4m, plugins)

    dispatcher = dispatcher_map[dispatcher]

    sys.stderr.write('Starting simulation with dispatching rule\n\n')
    sys.stderr.flush()

    while instance.current_time < greedy_run_to:
        done = instance.next_decision_point()
        instance.print_progress_in_days()
        if done or instance.current_time > run_to:
            break

        if l4m:
            machine, lots = get_lots_to_dispatch_by_machine(instance, dispatcher)
            if lots is None:
                instance.usable_machines.remove(machine)
            else:
                #action = Rl.choose()
                instance.dispatch(machine, lots)
        else:
            machine, lots = get_lots_to_dispatch_by_lot(instance, instance.current_time, dispatcher)
            if lots is None:
                instance.usable_lots.clear()
                instance.lot_in_usable.clear()
                instance.next_step()
            else:
                instance.dispatch(machine, lots)

    return instance

    