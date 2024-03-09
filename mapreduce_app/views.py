from django.shortcuts import render, redirect
from django.http import JsonResponse
from .forms import ConfigForm
import subprocess
import yaml

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




def config_view(request):
    if request.method == 'POST':
        form = ConfigForm(request.POST)
        if form.is_valid():
            # Process form data to update config and restart map-reduce
            with open('distributed_map_reduce/config.yml', 'r') as file:
                config = yaml.safe_load(file)
            
            # Update the configuration with form data
            config['master']['mapperCount'] = form.cleaned_data['mapper_count']
            config['master']['reducerCount'] = form.cleaned_data['reducer_count']
            config['application'] = form.cleaned_data['application']
            
            with open('distributed_map_reduce/config.yml', 'w') as file:
                yaml.safe_dump(config, file)
            
            return redirect('success_url')  # Redirect to a new URL
    else:
        form = ConfigForm()
    return render(request, 'MAP_REDUCE/config_form.html', {'form': form})
