#Imports von stats.py

import datetime
import io
import json
import statistics
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import pickle
import os
import sys

sys.path.append(os.path.join('C:/','Users','David Heik','Desktop','Arbeit2024','PySCFabSim','Projekt-Reproduktion','Mai-Session', 'PySCFabSim-release','simulation'))

from classes import Lot, Step


import pickle
import statistics
import argparse
import os


def load_debug_data(path):
    print(path)
    with open(path, 'rb') as f:
        return pickle.load(f)


def analyze(debug_data):

    lots = defaultdict(lambda: {'ACT': [], 'throughput_one_year': 0 , 'throughput': 0, 'on_time': 0, 'tardiness': 0,'early_tardiness':0, 'waiting_time': 0,
                                'processing_time': 0, 'transport_time': 0, 'waiting_time_batching': 0})


    done_lots = debug_data['instance_done_lots']
    cqt_violated = debug_data['instance_counter_cqt_violated']
    apt = debug_data['apt']
    dl = debug_data['dl']

    for lot in done_lots:
        if lot.release_at >= 0:
            lots[lot.name]['throughput'] += 1
        
        if lot.release_at >= 31536000:   
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


    print('Lot', 'APT', 'DL', 'ACT', 'TH', 'ONTIME', 'tardiness', 'early_tardiness', 'wa', 'wab', 'pr', 'tr')
    acts = []
    ths = []
    ontimes = []
    for lot_name in sorted(list(lots.keys())):
        l = lots[lot_name]
        avg = statistics.mean(l['ACT']) / 3600 / 24
        lots[lot_name]['ACT'] = avg
        acts += [avg]
        th = lots[lot_name]['throughput']
        ths += [th]
        ontime = round(l['on_time'] / l['throughput_one_year'] * 100,2)
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
    print('---------------')





if __name__ == "__main__":

    file = "greedy/debug_data_greedy_seed9949_730days_SMT2020_HVLM_fifo_seed-9949.pkl"

    if not os.path.exists(file):
        print("Datei nicht gefunden:", file)
    else:
        data = load_debug_data(file)
        analyze(data)
