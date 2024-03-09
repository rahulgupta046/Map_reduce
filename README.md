# Distributed Map-Reduce Django Application for Inverted Index and Word Count

![Distributed Map-Reduce System](/distributed_map_reduce/image.webp)

## Overview
This Django project showcases a scalable map-reduce system for processing large datasets, focusing on generating inverted indexes and counting word occurrences. It leverages Django for web interaction, allowing users to easily configure and run distributed data processing tasks.

## Features
- Distributed Processing: Leverages parallel computing across nodes.
- Inverted Index and Word Count: Supports key data processing tasks.
- Scalable & Fault Tolerant: Easily expands with more nodes and handles node failures gracefully.

## Getting Started

### Prerequisites
- Python 3.8+
- Django 5.0.3

### Installation
Clone the repository and set up the environment:
```bash
git clone https://github.com/rahulgupta046/Map_reduce.git
cd Map_reduce
pip install -r distributed_map_reduce/requirements.txt
python manage.py migrate
python manage.py runserver
```


## Usage
After starting the server, visit http://127.0.0.1:8000/config/ to configure and initiate map-reduce tasks like word_count or inverted_index.
Enter the text in the input box, Output shows the output of map reduce process


## Features
Configurable map-reduce settings via a web interface.
Supports multiple map-reduce algorithms.

