from django.shortcuts import render
from django.http import JsonResponse
import subprocess
import os
import time

# Your view that handles the MapReduce operation
def run_map_reduce(request):
    try:

        server_path = "distributed_map_reduce/server.py"
        # Start the server process
        server_process = subprocess.Popen(['python', server_path ])
        
        # Now, start the master process that spawns mappers and reducers
        master_process = subprocess.run(['python', 'distributed_map_reduce/master.py'], check=True)
        
        # Placeholder for actual result retrieval logic
        output = "Results of the job"
        
        # Optionally, terminate the server process if it's meant to run only for the duration of the job
        server_process.terminate()
        master_process.terminate()

        
        return JsonResponse({'status': 'success', 'output': output})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})
