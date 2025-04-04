import datetime
import io
import json
import statistics
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

from classes import Lot

lot_list = []
percentile_list = []

def save_percentile(instance):
    global percentile_list
    output_file = 'percentil_' + instance.rpt_route + ".csv"
    if not os.path.exists(output_file):
        df = pd.DataFrame(percentile_list, columns=['Percentil', 'Value'])
        df.to_csv(output_file, index=False)
    else:
        df_acc = pd.DataFrame(percentile_list, columns=['Percentil', 'Value'])
        value_min= pd.read_csv(output_file)['Value'].min()
        value_max= pd.read_csv(output_file)['Value'].max()
        if df_acc['Value'].min() < value_min and df_acc['Value'].max() > value_max:
            df_acc.to_csv(output_file, index=False)
    percentile_list = []

def print_statistics(instance, days, dataset, disp, method='greedy', dir='greedy', wip = False):
    global percentile_list
    from instance import Instance
    instance: Instance
    lot: Lot
    lots = defaultdict(lambda: {'ACT': [], 'throughput_one_year': 0 , 'throughput': 0, 'on_time': 0, 'tardiness': 0,'early_tardiness':0, 'waiting_time': 0,
                                'processing_time': 0, 'transport_time': 0, 'waiting_time_batching': 0})
    apt = {}
    dl = {}
    print("SUM_CQT_VIOLATIONS:", instance.counter_cqt_violated)
    for lot in instance.done_lots:
        #lots[lot.name]['throughput'] += 1
        if wip == False and lot.release_at < 31536000 and instance.rpt_route is None:
            continue
        if wip == False and lot.release_at >= 31536000 and instance.rpt_route is None:
            lots[lot.name]['throughput_one_year'] += 1
            lots[lot.name]['ACT'].append(lot.done_at - lot.release_at)
            lots[lot.name]['tardiness'] += max(0, lot.done_at - lot.deadline_at)
            lots[lot.name]['early_tardiness'] += max(0, lot.deadline_at - lot.done_at)
            lots[lot.name]['waiting_time'] += lot.waiting_time
            lots[lot.name]['waiting_time_batching'] += lot.waiting_time_batching
            lots[lot.name]['processing_time'] += lot.processing_time
            lots[lot.name]['transport_time'] += lot.transport_time
            if lot.done_at <= lot.deadline_at:
                lots[lot.name]['on_time'] += 1
            if lot.name not in apt:
                apt[lot.name] = sum([s.processing_time.avg() for s in lot.processed_steps])
                dl[lot.name] = lot.deadline_at - lot.release_at
        else:
            if lot.release_at >= 0.0:
                lots[lot.name]['throughput_one_year'] += 1
                lots[lot.name]['ACT'].append(lot.done_at - lot.release_at)
                lots[lot.name]['tardiness'] += max(0, lot.done_at - lot.deadline_at)
                lots[lot.name]['early_tardiness'] += max(0, lot.deadline_at - lot.done_at)
                lots[lot.name]['waiting_time'] += lot.waiting_time
                lots[lot.name]['waiting_time_batching'] += lot.waiting_time_batching
                lots[lot.name]['processing_time'] += lot.processing_time
                lots[lot.name]['transport_time'] += lot.transport_time
                if lot.done_at <= lot.deadline_at:
                    lots[lot.name]['on_time'] += 1
                if lot.name not in apt:
                    apt[lot.name] = sum([s.processing_time.avg() for s in lot.processed_steps])
                    dl[lot.name] = lot.deadline_at - lot.release_at
                lot_list.append((lot.done_at-lot.release_at)/60/60/24)
    if instance.rpt_route is not None:
        percentile_list.append(["Percentil "+ "0%", np.percentile(lot_list, 0)])
        percentile_list.append(["Percentil "+ "5%", np.percentile(lot_list, 5)])
        percentile_list.append(["Percentil "+ "25%", np.percentile(lot_list, 25)])
        percentile_list.append(["Percentil "+ "50%", np.percentile(lot_list, 50)])
        percentile_list.append(["Percentil "+ "75%", np.percentile(lot_list, 75)])
        percentile_list.append(["Percentil "+ "95%", np.percentile(lot_list, 95)])
        percentile_list.append(["Percentil "+ "100%", np.percentile(lot_list, 100)])
        print("RPT - 0%-Percentile:", np.percentile(lot_list, 0))
        print("RPT - 100%-Percentile:", np.percentile(lot_list, 100))
        save_percentile(instance)
    print('Lot', 'APT', 'DL', 'ACT', 'TH', 'ONTIME', 'tardiness', 'early_tardiness', 'wa', 'pr', 'tr')
    acts = []
    ths = []
    ontimes = []
    for lot_name in sorted(list(lots.keys())):
        l = lots[lot_name]
        avg = statistics.mean(l['ACT']) / 3600 / 24
        lots[lot_name]['ACT'] = avg
        acts += [avg]
        th = lots[lot_name]['throughput_one_year']
        ths += [th]
        ontime = round(l['on_time'] / l['throughput_one_year'] * 100)
        ontimes += [ontime]
        wa = lots[lot_name]['waiting_time'] / l['throughput_one_year'] / 3600 / 24
        wab = lots[lot_name]['waiting_time_batching'] / l['throughput_one_year'] / 3600 / 24
        pr = lots[lot_name]['processing_time'] / l['throughput_one_year'] / 3600 / 24
        tr = lots[lot_name]['transport_time'] / l['throughput_one_year'] / 3600 / 24
        print(lot_name, round(apt[lot_name] / 3600 / 24, 1), round(dl[lot_name] / 3600 / 24, 1), round(avg, 1), th,
              ontime, l['tardiness'], l['early_tardiness'], wa, wab, pr, tr)
    print('---------------')
    print(round(statistics.mean(acts), 2), statistics.mean(ths), statistics.mean(ontimes))
    print(round(sum(acts), 2), sum(ths), sum(ontimes))

    if instance.rpt_route is None:
        utilized_times = defaultdict(lambda: [])
        setup_times = defaultdict(lambda: [])
        pm_times = defaultdict(lambda: [])
        br_times = defaultdict(lambda: [])
        for machine in instance.machines:
            utilized_times[machine.family].append(machine.utilized_time)
            setup_times[machine.family].append(machine.setuped_time)
            pm_times[machine.family].append(machine.pmed_time)
            br_times[machine.family].append(machine.bred_time)

        print('Machine', 'Cnt', 'avail','util', 'br', 'pm', 'setup')
        machines = defaultdict(lambda: {})
        for machine_name in sorted(list(utilized_times.keys())):
            if wip == False and instance.rpt_route is None:
                time = instance.current_time - 31536000 #if not wip else instance.current_time
            else:
                time = instance.current_time
            av = (time - statistics.mean(pm_times[machine_name]) - statistics.mean(br_times[machine_name]))
            machines[machine_name]['avail'] = av / time
            machines[machine_name]['util'] = statistics.mean(utilized_times[machine_name]) / av
            machines[machine_name]['pm'] = statistics.mean(pm_times[machine_name]) / time
            machines[machine_name]['br'] = statistics.mean(br_times[machine_name]) / time
            machines[machine_name]['setup'] = statistics.mean(setup_times[machine_name]) / time
            r = instance.lot_waiting_at_machine[machine_name]
            if r[0] > 0 and r[1] > 0:
                machines[machine_name]['waiting_time'] = r[1] / r[0] / 3600 / 24
            print(machine_name, len(utilized_times[machine_name]),
                round(machines[machine_name]['avail'] * 100, 2),
                round(machines[machine_name]['util'] * 100, 2),
                round(machines[machine_name]['br'] * 100, 2),
                round(machines[machine_name]['pm'] * 100, 2),
                round(machines[machine_name]['setup'] * 100, 2))

        plugins = {}

        for plugin in instance.plugins:
            if plugin.get_output_name() is not None:
                plugins[plugin.get_output_name()] = plugin.get_output_value()

        with io.open(f'{dir}/{method}_{days}days_{dataset}_{disp}.json', 'w') as f:
            json.dump({
                'lots': lots,
                'machines': machines,
                'plugins': plugins,
            }, f)

