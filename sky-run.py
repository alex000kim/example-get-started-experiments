import os

import sky

DEFAULT_CLUSTER_NAME = 'mycluster'
IDLE_MINUTES_TO_AUTOSTOP = 10


def main(rerun_setup, cluster_name):
    task = sky.Task.from_yaml('sky-config.yaml')
    # This is optional, but if you want touse https://studio.iterative.ai to, for example, monitor your experiments in real-time. 
    # See: https://dvc.org/doc/studio/user-guide/projects-and-experiments/live-metrics-and-plots
    task.update_envs({'DVC_STUDIO_TOKEN': os.getenv('DVC_STUDIO_TOKEN')})

    s = sky.status(cluster_names=[cluster_name])
    print(f'Found {len(s)} cluster(s)')
    print(f'Status:\n{s}\n')
        
    if len(s) == 0:
        print('Cluster not found, launching cluster')
        sky.launch(task, 
                cluster_name=cluster_name,
                idle_minutes_to_autostop=10)
    elif len(s) == 1 and s[0]['name'] == cluster_name:
        cluster_status = s[0]['status']
        if cluster_status.value == 'UP':
            print(f'Cluster is UP, running task (rerun_setup: {rerun_setup})')
            if not rerun_setup:
                sky.exec(task, cluster_name=cluster_name)
            else:
                # this won't launch a new cluster, 
                # but will rerun the setup and then the run step
                sky.launch(task, 
                        cluster_name=cluster_name,
                        idle_minutes_to_autostop=10)
        elif cluster_status.value == 'STOPPED':
            print('Cluster is STOPPED, starting cluster')
            sky.start(cluster_name=cluster_name,
                    idle_minutes_to_autostop=10)
        elif cluster_status.value == 'INIT':
            print('Cluster is INIT, waiting for cluster to be ready')
    else:
        print('Multiple clusters found. Status:')
        print(s)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--rerun-setup', '-r', help='Rerun the setup step', action='store_true', default=False)
    parser.add_argument('--cluster', '-c', help='Name of the cluster', default=DEFAULT_CLUSTER_NAME)
    args = parser.parse_args()
    main(rerun_setup=args.rerun_setup, cluster_name=args.cluster)