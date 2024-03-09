from django.shortcuts import render, redirect
from django.http import JsonResponse
from .forms import ConfigForm, TextInputForm
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
 
        return render(request, 'processing.html')
    except Exception as e:
        return render(request, 'error.html', {'message': str(e)})


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
            
            return redirect('text_input')  # Redirect to a new URL
    else:
        form = ConfigForm()
    return render(request, 'MAP_REDUCE/config_form.html', {'form': form})


def text_input_view(request):
    if request.method == 'POST':
        form = TextInputForm(request.POST, request.FILES)
        if form.is_valid():
            text = form.cleaned_data.get('text')
            file = request.FILES.get('file')
            if file:
                # Read text from the uploaded file
                text = file.read().decode('utf-8')
            
            # Write the text or file content to input.txt for map-reduce processing
            with open('distributed_map_reduce/input.txt', 'w') as input_file:
                input_file.write(text)
            
            # Redirect or call your map-reduce processing function here
            return redirect('process_data')  # Assume you have a URL named 'process_data' for processing

    else:
        form = TextInputForm()
    return render(request, 'text_input_form.html', {'form': form})