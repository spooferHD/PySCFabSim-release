import os
import sys
sys.path.append(os.path.join('C:/','Users','willi','OneDrive','Documents','Studium','Diplomarbeit','Programm + Datengrundlage','PySCFabSim-release','simulation'))
sys.path.append(os.path.join('data','horse','ws','wiro085f-WsRodmann','Final_Version','PySCFabSim', 'simulation'))

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
import copy

import argparse
import pandas as pd
import matplotlib.pyplot as plt

last_sort_time = -1


def dispatching_combined_permachine(ptuple_fcn, machine, time, setups):
    for lot in machine.waiting_lots:
        lot.ptuple = ptuple_fcn(lot, time, machine, setups)

        
def find_alternative_machine(instance, lots, machine):
    m: Machine
    for m in instance.family_machines[machine.family]: #hier wird eine Maschine gesucht, wo das Setup dem Los-Setup entspricht
        if m in instance.usable_machines and m.current_setup == lots[0].actual_step.setup_needed:  
            machine = m
            break

def get_lots_to_dispatch_by_machine(instance, ptuple_fcn, machine=None):
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
            lot_m[w.actual_step.step_name].append(w)  # + '_' + w.part_name
        lot_l = sorted(list(lot_m.values()),
                       key=lambda l: (
                           l[0].ptuple[0],  # cqt
                           l[0].ptuple[1],  # min run setup is the most important
                           -min(1, len(l) / l[0].actual_step.batch_max),  # then maximize the batch size
                           0 if len(l) >= l[0].actual_step.batch_min else 1,  # then take min batch size into account
                           *(l[0].ptuple[2:]),  # finally, order based on prescribed priority rule
                       ))
        lots: List[Lot] = lot_l[0]
        if len(lots) > lots[0].actual_step.batch_max:
            lots = lots[:lots[0].actual_step.batch_max]
        if len(lots) < lots[0].actual_step.batch_max:
            lots = None
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
                    #machine = None
            else:
                find_alternative_machine(instance, lots, machine)
        else:
            find_alternative_machine(instance, lots, machine)
            
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


def run_greedy(dataset, RL_days, greedy_days, dispatcher, seed, wandb, chart, alg='l4m'):
     
    sys.stderr.write('Loading ' + dataset + ' for ' + str(greedy_days) + ' days, using ' + dispatcher + '\n')
    sys.stderr.flush()

    start_time = datetime.now()

    files = read_all('datasets/' + dataset)

    run_to = 3600 * 24 * RL_days
    greedy_run_to = 3600 * 24 * greedy_days
    Randomizer().random.seed(seed)
    l4m = alg == 'l4m'
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

   # instance.finalize()
   # interval = datetime.now() - start_time
   # print(instance.current_time_days, ' days simulated in ', interval)
    return instance
    #print_statistics(instance, days, dataset, dispatcher, method='greedy_seed' + str(seed))
    