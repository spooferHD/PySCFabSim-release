from collections import defaultdict
from typing import Dict, List, Set, Tuple
import os
import sys

import pandas as pd
sys.path.append(os.path.join('C:/','Users','willi','OneDrive','Documents','Studium','Diplomarbeit','Programm + Datengrundlage','PySCFabSim-release-William-Rodmann','simulation'))
sys.path.append(os.path.join('C:/','Users','willi','OneDrive','Documents','Studium','Diplomarbeit','Programm + Datengrundlage','PySCFabSim-release-William-Rodmann','simulation', 'gym'))
from classes import Machine, Route, Lot
from dispatching.dm_lot_for_machine import LotForMachineDispatchManager
from dispatching.dm_machine_for_lot import MachineForLotDispatchManager
from event_queue import EventQueue
from events import MachineDoneEvent, LotDoneEvent, BreakdownEvent, ReleaseEvent
from plugins.interface import IPlugin
import pickle


class Instance:

    def __init__(self, machines: List[Machine], routes: Dict[str, Route], lots: List[Lot],
                 setups: Dict[Tuple, int], setup_min_run: Dict[str, int], breakdowns: List[BreakdownEvent],
                 lot_for_machine, plugins):
        self.plugins: List[IPlugin] = plugins
        self.lot_waiting_at_machine = defaultdict(lambda: (0, 0))

        self.free_machines: List[bool] = []
        self.usable_machines: Set[Machine] = set()
        self.usable_lots: List[Lot] = list()

        self.machines: List[Machine] = [m for m in machines]
        self.family_machines = defaultdict(lambda: [])
        for m in self.machines:
            self.family_machines[m.family].append(m)
        self.routes: Dict[str, Route] = routes
        self.setups: Dict[Tuple, int] = setups
        self.setup_min_run: Dict[str, int] = setup_min_run

        self.dm = LotForMachineDispatchManager() if lot_for_machine else MachineForLotDispatchManager()
        self.dm.init(self)

        self.dispatchable_lots: List[Lot] = lots
        self.dispatchable_lots.sort(key=lambda k: k.release_at)
        self.active_lots: List[Lot] = []
        self.done_lots: List[Lot] = []

        self.events = EventQueue()    

        #self.setup_per_timestep_when_needed = {}
        self.counter_cqt_violated = 0

        self.current_time = 0 

        for plugin in self.plugins:
            plugin.on_sim_init(self)

        self.next_step()

        self.free_up_machines(self.machines)

        for br in breakdowns:
            self.add_event(br)

        self.printed_days = -1

    @property
    def current_time_days(self):
        return self.current_time / 3600 / 24
    
    def process_until_calc(self):
        process_until = []
        if len(self.events.arr) > 0:
            process_until.append(max(0, self.events.first.timestamp))
        if len(self.dispatchable_lots) > 0:
            process_until.append(max(0, self.dispatchable_lots[0].release_at))
        else: 
            process_until.append(0)
        return min(process_until)
    
    def move_event(self, ev):
        temp_time = 0
        for ev_m in ev.machine.events:
            if ev_m.timestamp > temp_time:
                temp_time = ev_m.timestamp
                
        delay = temp_time  - ev.timestamp     
        ev.timestamp += delay
        self.add_event(ev)


    def next_step(self):
        process_until = self.process_until_calc()
        while len(self.events.arr) > 0 and self.events.first.timestamp <= process_until:
            ev = self.events.pop_first()
            self.current_time = max(0, ev.timestamp, self.current_time)
            ev.handle(self)
        ReleaseEvent.handle(self, process_until)

    def free_up_machines(self, machines):
        # add machine to list of available machines
        for machine in machines:
            machine.events.clear()
            self.dm.free_up_machine(self, machine)

            for plugin in self.plugins:
                plugin.on_machine_free(self, machine)

    def free_up_lots(self, lots: List[Lot]):
        # add lot to lists, make it available
        for lot in lots:
            lot.free_since = self.current_time
            step_found = False
            while len(lot.remaining_steps) > 0:
                old_step = None
                if lot.actual_step is not None:
                    lot.processed_steps.append(lot.actual_step)
                    old_step = lot.actual_step
                if lot.actual_step is not None and lot.actual_step.has_to_rework(lot.idx):
                    rw_step = lot.actual_step.rework_step
                    removed = lot.processed_steps[rw_step - 1:]
                    lot.processed_steps = lot.processed_steps[:rw_step - 1]
                    lot.remaining_steps = removed + lot.remaining_steps
                lot.actual_step, lot.remaining_steps = lot.remaining_steps[0], lot.remaining_steps[1:]
                if lot.actual_step.has_to_perform():
                    self.dm.free_up_lots(self, lot)
                    step_found = True
                    for plugin in self.plugins:
                        plugin.on_step_done(self, lot, old_step)
                    break
            if not step_found:
                assert len(lot.remaining_steps) == 0
                lot.actual_step = None
                lot.done_at = self.current_time
                self.active_lots.remove(lot)
                self.done_lots.append(lot)
                for plugin in self.plugins:
                    plugin.on_lot_done(self, lot)

            for plugin in self.plugins:
                plugin.on_lot_free(self, lot)

    def dispatch(self, machine: Machine, lots: List[Lot]):
        # remove machine and lot from active sets
        self.reserve_machine_lot(lots, machine)
        lwam = self.lot_waiting_at_machine[machine.family]
        self.lot_waiting_at_machine[machine.family] = (lwam[0] + len(lots),
                                                       lwam[1] + sum([self.current_time - l.free_since for l in lots]))
        for lot in lots:
            lot.waiting_time += self.current_time - lot.free_since
            if lot.actual_step.batch_max > 1:
                lot.waiting_time_batching += self.current_time - lot.free_since
            # if lot.actual_step.cqt_for_step is not None: TODO: CQT handling, Deactivated for now
            #     lot.cqt_waiting = lot.actual_step.cqt_for_step
            #     #lot.cqt_deadline = self.current_time + lot.actual_step.cqt_time
            #     lot.cqt_deadline = lot.actual_step.cqt_time
            # if lot.actual_step.order == lot.cqt_waiting:
            #     if lot.cqt_deadline < self.current_time:
            #         for plugin in self.plugins:
            #             plugin.on_cqt_violated(self, machine, lot)
            #     lot.cqt_waiting = None
            #     lot.cqt_deadline = None
        # compute times for lot and machine
        lot_time, machine_time, setup_time = self.get_times(self.setups, lots, machine)
        # compute per-piece preventive maintenance requirement
        for i in range(len(machine.pieces_until_maintenance)):
            machine.pieces_until_maintenance[i] -= sum([l.pieces for l in lots])
            if machine.pieces_until_maintenance[i] <= 0:
                s = machine.maintenance_time[i].sample()
                machine_time += s
                machine.pieces_until_maintenance[i] = machine.piece_per_maintenance[i]
                machine.pmed_time += s
                machine.current_setup = ''
        # compute timebased preventive maintenance requirement
        look_ahead_time = self.current_time + machine_time + setup_time
        for event in self.events.arr:
            if "BreakdownEvent" in str(event) and event.is_breakdown == False and event.machine.idx == machine.idx and event.timestamp <= look_ahead_time:
                self.events.remove(event)
                s = event.handle_timebased_pm(self)
                machine_time += s
            if event.timestamp > look_ahead_time:
                    break

        # if there is ltl dedication, dedicate lot for selected step
        for lot in lots:
            if lot.actual_step.lot_to_lens_dedication is not None:
                lot.dedications[lot.actual_step.lot_to_lens_dedication] = machine.idx
        # decrease / eliminate min runs required before next setup
        if machine.min_runs_left is not None:
            machine.min_runs_left -= len(lots)
            if machine.min_runs_left <= 0:
                machine.min_runs_left = None
                machine.min_runs_setup = None
        # add events
        machine_done = self.current_time + machine_time + setup_time
        lot_done = self.current_time + lot_time + setup_time
        ev1 = MachineDoneEvent(machine_done, [machine])
        ev2 = LotDoneEvent(lot_done, [machine], lots)
        self.add_event(ev1)
        self.add_event(ev2)
        machine.events += [ev1, ev2]

        for plugin in self.plugins:
            plugin.on_dispatch(self, machine, lots, machine_done, lot_done)
        return machine_done, lot_done

    def get_times(self, setups, lots, machine):
        proc_t_samp = lots[0].actual_step.processing_time.sample()
        lot_time = proc_t_samp + machine.load_time + machine.unload_time
        for lot in lots:
            lot.processing_time += lot_time
        if len(lots[0].remaining_steps) > 0:
            tt = lots[0].remaining_steps[0].transport_time.sample()
            lot_time += tt
            for lot in lots:
                lot.transport_time += tt
        if lots[0].actual_step.processing_time == lots[0].actual_step.cascading_time:
            cascade_t_samp = proc_t_samp
        else:
            cascade_t_samp = lots[0].actual_step.cascading_time.sample()
        machine_time = cascade_t_samp + (machine.load_time + machine.unload_time if not machine.cascading else 0)
        new_setup = lots[0].actual_step.setup_needed
        if new_setup != '':
            self.setup_count_when_needed(machine, new_setup)
        if new_setup != '' and machine.current_setup != new_setup:
            if lots[0].actual_step.setup_time is not None:
                setup_time = lots[0].actual_step.setup_time             # SetupTime für in der Route geplante Setups
            elif (machine.current_setup, new_setup) in setups:
                setup_time = setups[(machine.current_setup, new_setup)] # SetupTime für in setup.txt für DE_BE_ Maschinen
            elif ('', new_setup) in setups:
                setup_time = setups[('', new_setup)]                    # SetupTime für in setup.txt für Implant_91/128/131
            else:
                setup_time = 0                                          # SetupTime, wenn in DE_BE kein Setup vorhanden ist
        else:
            setup_time = 0
        if new_setup in self.setup_min_run and machine.min_runs_left is None and setup_time > 0:
            machine.min_runs_left = self.setup_min_run[new_setup]
            machine.min_runs_setup = new_setup
            machine.has_min_runs = True
        if setup_time > 0:
            machine.last_setup_time = setup_time
        machine.utilized_time += machine_time
        machine.setuped_time += setup_time
        machine.last_setup = machine.current_setup
        machine.current_setup = new_setup
        return lot_time, machine_time, setup_time

    def reserve_machine_lot(self, lots, machine):
        self.dm.reserve(self, lots, machine)

    def add_event(self, to_insert):
        self.events.ordered_insert(to_insert)

    def next_decision_point(self):
        return self.dm.next_decision_point(self)

    def handle_breakdown(self, machine, delay):
        ta = []
        for ev in machine.events: # nur Events mit LoteDone oder MachineDone werden verschoben
            if ev in self.events.arr:
                ta.append(ev)
                self.events.remove(ev)
        for ev in ta:
            ev.timestamp += delay
            self.add_event(ev)

    @property
    def done(self):
        return len(self.dispatchable_lots) == 0 and len(self.active_lots) == 0

    def finalize(self):
        for plugin in self.plugins:
            plugin.on_sim_done(self)

    def print_progress_in_days(self):
        import sys
        if int(self.current_time_days) > self.printed_days:
            self.printed_days = int(self.current_time_days)
            if self.printed_days > 0:
                sys.stderr.write(
                    f'\rDay {self.printed_days}===Throughput: {round(len(self.done_lots) / self.printed_days)}/day=')
                sys.stderr.flush()

    # def setup_count_when_needed(self, machine, setup_needed):
    #     if self.current_time > 0:
    #         #if self.current_time not in self.setup_per_timestep_when_needed[machine.family] or self.setup_count_when_needed == {}:
    #             self.setup_per_timestep_when_needed[int(self.current_time)] = [setup_needed, 'Verfügbar',{machine.family: {}}, 'Nicht frei', {machine.family: {}}]
    #             self.setup_per_timestep_when_needed[int(self.current_time)][2][machine.family]= {machine.current_setup: 1}
    #             for machines in self.machines:
    #                 if machines.idx != machine.idx:
    #                     if self.free_machines[machines.idx] == True:
    #                         if machines.family == machine.family:
    #                             if machines.current_setup in self.setup_per_timestep_when_needed[int(self.current_time)][2][machine.family]:
    #                                 self.setup_per_timestep_when_needed[int(self.current_time)][2][machine.family][machines.current_setup] += 1
    #                             else:
    #                                 self.setup_per_timestep_when_needed[int(self.current_time)][2][machine.family][machines.current_setup] =  1
    #                     else:
    #                         if machines.family == machine.family:
    #                             if machines.current_setup in self.setup_per_timestep_when_needed[int(self.current_time)][4][machine.family]:
    #                                 self.setup_per_timestep_when_needed[int(self.current_time)][4][machine.family][machines.current_setup] += 1
    #                             else:
    #                                 self.setup_per_timestep_when_needed[int(self.current_time)][4][machine.family][machines.current_setup] =  1



    # def save_setup_when_needed(self):
    #     with open('setup_when_needed.pkl', 'wb') as f:
    #         pickle.dump(self.setup_per_timestep_when_needed, f)
    
                
