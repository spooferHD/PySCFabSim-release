import datetime
import json
import os
import sys
import time

import wandb

from stable_baselines3 import PPO

from wandb.integration.sb3 import WandbCallback

from simulation.gym.environment import DynamicSCFabSimulationEnvironment
from stable_baselines3.common.callbacks import CheckpointCallback, EvalCallback
from simulation.greedy_RL import run_greedy
from sys import argv

from simulation.gym.sample_envs import DEMO_ENV_1


def main():
    to_train = 608000  #10000000 # 608000 für 730 Tage --> 32 Jahre Trainingszeit (mit Initialisierungsphase)
    greedy_days = 100
    t = time.time()
    save_freq = 500 
    class MyCallBack(CheckpointCallback):

        def on_step(self) -> bool:
            if self.num_timesteps % 100 == 0:
                ratio = self.num_timesteps / to_train
                perc = round(ratio * 100)
                remaining = (time.time() - t) / ratio * (1 - ratio) if ratio > 0 else 9999999999999
                remaining /= 3600

                sys.stderr.write(f'\r{self.num_timesteps} / {to_train} {perc}% {round(remaining, 2)} hours left    {env.instance.current_time_days}      ')
            return super().on_step()

    fn = argv[1]
    #fn = "experiments/0_ds_HVLM_a9_tp730_reward2_di_fifo_TF\config.json"
    with open(fn, 'r') as config:
        p = json.load(config)['params']
    args = dict(num_actions=p['action_count'], active_station_group=p['station_group'],
                days=p['training_period'], dataset='SMT2020_' + p['dataset'],
                dispatcher=p['dispatcher'])
    args_eval= dict(num_actions=p['action_count'], active_station_group=p['station_group'], dataset='SMT2020_' + p['dataset'],
                dispatcher=p['dispatcher'])
    print(f'Greedy ENV bis {greedy_days} Tage erstellt')
    greedy_instance =run_greedy('SMT2020_' + p['dataset'],p['training_period'] , greedy_days, p['dispatcher'], 0, False, False, alg='l4m')
    print("Greedy Instance abgeschlossen")
    print("Args angenommen")
    env = DynamicSCFabSimulationEnvironment(**DEMO_ENV_1, **args, seed=p['seed'], max_steps=10000000, reward_type=p['reward'],greedy_instance=greedy_instance, plugins=[] )
    print("Env erstellt")
    eval_env = DynamicSCFabSimulationEnvironment(**DEMO_ENV_1, **args_eval, days= 265, seed=777, max_steps=0, reward_type=p['reward'], greedy_instance=greedy_instance,plugins=[])
    print("Alles erstellt - ich lerne jz")
    model = PPO("MlpPolicy", env, verbose=1)
    #Callbacks
    p = os.path.dirname(os.path.realpath(fn))
    checkpoint_callback_MyCallBack = MyCallBack(save_freq=100000, save_path=p, name_prefix='checkpoint_')
    checkpoint_callback_eval = EvalCallback(eval_env, best_model_save_path=p+'/eval/best_model/',log_path=p+'/eval/', eval_freq=2000000, deterministic=True, render=False )
    callback= [checkpoint_callback_MyCallBack,checkpoint_callback_eval] 
    # model.learn(
    #    total_timesteps=to_train, eval_freq=4000000, eval_env=eval_env, n_eval_episodes=1,
    #    callback=checkpoint_callback
    # )
    model.learn(
        total_timesteps=to_train,
        callback=callback,
    )
    print("Ich sichere")
    model.save(os.path.join(p, 'trained.weights'))


if __name__ == '__main__':
    main()
